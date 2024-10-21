module Connector.Postgres
  ( ConnectorConfig(..)
  , connect
  , partitioner
  , encoder
  ) where

import Control.Exception (Exception, bracket, throwIO, ErrorCall(..))
import qualified Data.Aeson as Aeson
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as LB
import Data.ByteString.Base64 (encodeBase64)
import Data.Base64.Types (extractBase64)
import Data.Hashable (Hashable)
import Data.Int (Int64)
import Data.List ((\\), intercalate)
import Data.Map.Strict (Map)
import Data.Maybe (fromMaybe)
import qualified Data.Map.Strict as Map
import Data.String (fromString)
import Data.Text (Text)
import qualified Data.Text as Text
import Data.Void (Void)
import Data.Word (Word64)
import qualified Database.PostgreSQL.Simple as P
import qualified Database.PostgreSQL.Simple.FromField as P
import qualified Database.PostgreSQL.Simple.FromRow as P
import GHC.Generics (Generic)

import Utils.Delay (Duration, millis, seconds)
import qualified Connector.Poll as Poll
import Connector.Poll (BoundaryTracker(..), Boundaries(..))
import Queue.Topic (Producer, Partitioner, Encoder, hashPartitioner)

_POLLING_INTERVAL :: Duration
_POLLING_INTERVAL = millis 50

_MAX_TRANSACTION_TIME :: Duration
_MAX_TRANSACTION_TIME = seconds 120

data ConnectorConfig = ConnectorConfig
  { c_host :: Text
  , c_port :: Int
  , c_username :: Text
  , c_password :: Text
  , c_database :: Text
  , c_table :: Text
  , c_columns :: [Text]
  , c_partitioningColumn :: Text
  , c_serialColumn :: Text
  }

newtype TableSchema = TableSchema { unTableSchema :: Map Text PgType }
  deriving Show

-- | Supported PostgreSQL types
data PgType
  = PgInt8
  | PgInt2
  | PgInt4
  | PgFloat4
  | PgFloat8
  | PgBool
  | PgJson
  | PgBytea
  | PgTimestamp
  | PgTimestamptz
  | PgText
  deriving (Show, Eq)

data UnsupportedType = UnsupportedType String
  deriving (Show, Exception)

instance P.FromField PgType where
  fromField t d = do
    str <- P.fromField t d
    case str of
      "int8" -> return PgInt8
      "int2" -> return PgInt2
      "int4" -> return PgInt4
      "float4" -> return PgFloat4
      "float8" -> return PgFloat8
      "bool" -> return PgBool
      "json" -> return PgJson
      "bytea" -> return PgBytea
      "timestamp" -> return PgTimestamp
      "timestamptz" -> return PgTimestamptz
      "text" -> return PgText
      _ -> P.conversionError $ UnsupportedType str

-- | One row retrieved from a PostgreSQL database.
type Row = [Value]

data Value
  = Boolean Bool
  | UInt Word64
  | Int Int64
  | Real Double
  | String Text
  | Bytes ByteString
  | DateTime Text
  | Json Aeson.Value
  | Null
  deriving (Generic, Show, Eq, Hashable, Ord)

instance Aeson.ToJSON Value where
  toJSON = \case
    Boolean b -> Aeson.toJSON b
    UInt b -> Aeson.toJSON b
    Int b -> Aeson.toJSON b
    Real b -> Aeson.toJSON b
    String b -> Aeson.toJSON b
    Bytes b -> Aeson.String $ extractBase64 $ encodeBase64 b
    DateTime b -> Aeson.toJSON b
    Json b -> b
    Null -> Aeson.Null

connect :: Producer Row -> ConnectorConfig -> IO Void
connect producer config@ConnectorConfig{..} =
   bracket open P.close $ \conn -> do
   schema <- fetchSchema c_table conn
   print schema
   validate config schema
   let pcol = unTableSchema schema Map.! c_partitioningColumn
   withTracker pcol $ consume conn schema
   where
   open = P.connect P.ConnectInfo
      { P.connectHost = Text.unpack c_host
      , P.connectPort = fromIntegral c_port
      , P.connectUser = Text.unpack c_username
      , P.connectPassword = Text.unpack c_password
      , P.connectDatabase = Text.unpack c_database
      }

   consume
      :: forall id. Show id
      => P.Connection
      -> TableSchema
      -> BoundaryTracker id
      -> (Row -> id)
      -> IO Void
   consume conn schema tracker getSerial = Poll.connect tracker pc
     where
     pc = Poll.PollingConnector
        { Poll.c_getId = getSerial
        , Poll.c_poll = run
        , Poll.c_pollingInterval = _POLLING_INTERVAL
        , Poll.c_maxTransactionTime = _MAX_TRANSACTION_TIME
        , Poll.c_producer = producer
        }

     parser = mkParser (columns config) schema

     run (Boundaries bs) = P.queryWith parser conn query ()
       where
       query = fromString $ Text.unpack $ Text.unwords
          [ "SELECT" , Text.intercalate ", " (columns config)
          , "FROM" , c_table
          , "WHERE" , constraints
          , "ORDER BY" , c_serialColumn
          ]
       constraints = Text.pack $ intercalate "AND" $ flip fmap bs
         $ \(low, high) -> unwords
             [ "("
             , Text.unpack c_serialColumn, "<", show low
             , "OR"
             , Text.unpack c_serialColumn, ">", show high
             , ")"]

partitioner :: Partitioner Row
partitioner = hashPartitioner partitioningValue

encoder :: ConnectorConfig -> Encoder Row
encoder config row =
  LB.toStrict $ Aeson.encode $ Map.fromList $ zip (columns config) row

-- | Columns in the order they will be queried.
columns :: ConnectorConfig -> [Text]
columns ConnectorConfig{..} =
  [c_serialColumn, c_partitioningColumn] <>
    ((c_columns \\ [c_serialColumn]) \\ [c_partitioningColumn])

serialValue :: Row -> Value
serialValue = \case
  [] -> error "serialValue: empty row"
  x:_ -> x

partitioningValue :: Row -> Value
partitioningValue = (!! 1)

mkParser :: [Text] -> TableSchema -> P.RowParser Row
mkParser cols (TableSchema schema) = traverse (parser . getType) cols
  where
  getType col = schema Map.! col

  parser :: PgType -> P.RowParser Value
  parser ty = fmap (fromMaybe Null) $ case ty of
    PgInt8 -> fmap Int <$> P.field
    PgInt2 -> fmap Int <$> P.field
    PgInt4 -> fmap Int <$> P.field
    PgFloat4 -> fmap Real <$> P.field
    PgFloat8 -> fmap Real <$> P.field
    PgBool -> fmap Boolean <$> P.field
    PgJson -> fmap Json <$> P.field
    PgBytea -> fmap Bytes <$> P.field
    PgTimestamp -> fmap DateTime <$> P.field
    PgTimestamptz -> fmap DateTime <$> P.field
    PgText -> fmap String <$> P.field

withTracker
   :: PgType
   -> (forall id. Show id => BoundaryTracker id -> (Row -> id) -> IO b)
   -> IO b
withTracker ty f = case ty of
   PgInt8 -> intTracker
   PgInt2 -> intTracker
   PgInt4 -> intTracker
   PgFloat4 -> realTracker
   PgFloat8 -> realTracker
   PgBool -> unsupported
   PgJson -> unsupported
   PgBytea -> unsupported
   PgTimestamp -> unsupported
   PgTimestamptz -> unsupported
   PgText -> unsupported
   where
   unsupported = error $ "Unsupported serial column type: " <> show ty

   intTracker = f Poll.rangeTracker get
      where
      get row = case serialValue row of
         Int n -> n
         val -> error "Invalid serial column value:" (show val)

   realTracker = f Poll.rangeTracker get
      where
      get row = case serialValue row of
         Real n -> n
         val -> error "Invalid serial column value:" (show val)

validate :: ConnectorConfig -> TableSchema -> IO ()
validate config (TableSchema schema) = do
  missingCol
  invalidSerial
  where
  missing = filter (not . (`Map.member` schema)) (columns config)
  missingCol =
    case missing of
      [] -> return ()
      xs -> throwIO $ ErrorCall $ "Missing columns in target table: " <> Text.unpack (Text.unwords xs)

  invalidSerial =
    if serialTy `elem` allowedSerialTypes
    then return ()
    else throwIO $ ErrorCall $ "Invalid serial column type: " <> show serialTy
  serialTy = schema Map.! c_serialColumn config
  allowedSerialTypes = [PgInt8, PgInt2, PgInt4, PgFloat4, PgFloat8]

fetchSchema :: Text -> P.Connection -> IO TableSchema
fetchSchema table conn = do
   cols <- P.query conn query [table]
   return $ TableSchema $ Map.fromList cols
   where
   -- In PostgreSQL internals 'columns' are called 'attributes' for hysterical raisins.
   -- oid is the table identifier in the pg_class table.
   query = fromString $ unwords
      [ "SELECT a.attname, t.typname"
      , "  FROM pg_class c"
      , "  JOIN pg_attribute a ON relname = ? AND c.oid = a.attrelid AND a.attnum > 0"
      , "  JOIN pg_type t ON t.oid = a.atttypid"
      , "  ORDER BY a.attnum ASC;"
      ]




