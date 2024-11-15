module Ambar.Emulator.Connector.Postgres
  ( ConnectorConfig(..)
  , withConnector
  , ConnectorState
  , partitioner
  , encoder
  , Value(..)
  , Row
  ) where

import Control.Concurrent.STM (STM, TVar, newTVarIO, readTVar)
import Control.Exception (Exception, bracket, throwIO, ErrorCall(..))
import Data.Aeson (FromJSON, ToJSON)
import qualified Data.Aeson as Aeson
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as LB
import Data.ByteString.Base64 (encodeBase64)
import Data.Base64.Types (extractBase64)
import Data.Default (Default(..))
import Data.Hashable (Hashable)
import Data.Int (Int64)
import Data.List ((\\))
import Data.Map.Strict (Map)
import Data.Maybe (fromMaybe)
import qualified Data.Map.Strict as Map
import Data.String (fromString)
import Data.Text (Text)
import qualified Data.Text as Text
import Data.Void (Void)
import Data.Word (Word64, Word16)
import qualified Database.PostgreSQL.Simple as P
import qualified Database.PostgreSQL.Simple.Transaction as P
import qualified Database.PostgreSQL.Simple.FromField as P
import qualified Database.PostgreSQL.Simple.FromRow as P
import GHC.Generics (Generic)
import Utils.Prettyprinter (renderPretty, sepBy, commaSeparated, prettyJSON)
import Prettyprinter (pretty, (<+>))
import qualified Prettyprinter as Pretty

import qualified Ambar.Emulator.Connector.Poll as Poll
import Ambar.Emulator.Connector.Poll (BoundaryTracker, Boundaries(..), EntryId(..), Stream)
import Ambar.Emulator.Queue.Topic (Producer, Partitioner, Encoder, hashPartitioner)
import Utils.Async (withAsyncThrow)
import Utils.Delay (Duration, millis, seconds)
import Utils.Logger (SimpleLogger, logDebug, logInfo)

_POLLING_INTERVAL :: Duration
_POLLING_INTERVAL = millis 50

_MAX_TRANSACTION_TIME :: Duration
_MAX_TRANSACTION_TIME = seconds 120

data ConnectorConfig = ConnectorConfig
  { c_host :: Text
  , c_port :: Word16
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
newtype Row = Row [Value]

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

-- | Opaque serializable connector state
newtype ConnectorState = ConnectorState BoundaryTracker
  deriving (Generic)
  deriving newtype (Default)
  deriving anyclass (FromJSON, ToJSON)

withConnector
  :: SimpleLogger
  -> ConnectorState
  -> Producer Row
  -> ConnectorConfig
  -> (STM ConnectorState -> IO a)
  -> IO a
withConnector logger (ConnectorState tracker) producer config@ConnectorConfig{..} f =
   bracket open P.close $ \conn -> do
   schema <- fetchSchema c_table conn
   validate config schema
   trackerVar <- newTVarIO tracker
   let readState = ConnectorState <$> readTVar trackerVar
   withAsyncThrow (consume conn schema trackerVar) (f readState)
   where
   open = P.connect P.ConnectInfo
      { P.connectHost = Text.unpack c_host
      , P.connectPort = c_port
      , P.connectUser = Text.unpack c_username
      , P.connectPassword = Text.unpack c_password
      , P.connectDatabase = Text.unpack c_database
      }

   consume
      :: P.Connection
      -> TableSchema
      -> TVar BoundaryTracker
      -> IO Void
   consume conn schema trackerVar = Poll.connect trackerVar pc
     where
     pc = Poll.PollingConnector
        { Poll.c_getId = entryId
        , Poll.c_poll = run
        , Poll.c_pollingInterval = _POLLING_INTERVAL
        , Poll.c_maxTransactionTime = _MAX_TRANSACTION_TIME
        , Poll.c_producer = producer
        }

     parser = mkParser (columns config) schema

     opts = P.FoldOptions
       { P.fetchQuantity = P.Automatic
       , P.transactionMode = P.TransactionMode
          { P.isolationLevel = P.ReadCommitted
          , P.readWriteMode = P.ReadOnly
          }
       }

     run :: Boundaries -> Stream Row
     run (Boundaries bs) acc0 emit = do
       logDebug logger query
       (acc, count) <- P.foldWithOptionsAndParser opts parser conn (fromString query) () (acc0, 0) $
         \(acc, !count) row -> do
           logResult row
           acc' <- emit acc row
           return (acc', succ count)
       logDebug logger $ "results: " <> show @Int count
       return acc
       where
       query = fromString $ Text.unpack $ renderPretty $ Pretty.fillSep
          [ "SELECT" , commaSeparated $ map pretty $ columns config
          , "FROM" , pretty c_table
          , if null bs then "" else "WHERE" <> constraints
          , "ORDER BY" , pretty c_serialColumn
          ]

       constraints = sepBy "AND"
          [ Pretty.fillSep
             [ "("
             , pretty c_serialColumn, "<", pretty low
             , "OR"
             ,  pretty high, "<", pretty c_serialColumn
             , ")"]
          | (EntryId low, EntryId high) <- bs
          ]

       logResult row =
        logInfo logger $ renderPretty $
          "ingested." <+> commaSeparated
            [ "serial_value:" <+> prettyJSON (serialValue row)
            , "partitioning_value:" <+> prettyJSON (partitioningValue row)
            ]

partitioner :: Partitioner Row
partitioner = hashPartitioner partitioningValue

-- | A rows gets saved in the database as a JSON object with
-- the columns specified in the config file as keys.
encoder :: ConnectorConfig -> Encoder Row
encoder config (Row row) =
  LB.toStrict $ Aeson.encode $ Map.fromList $ zip (columns config) row

-- | Columns in the order they will be queried.
columns :: ConnectorConfig -> [Text]
columns ConnectorConfig{..} =
  [c_serialColumn, c_partitioningColumn] <>
    ((c_columns \\ [c_serialColumn]) \\ [c_partitioningColumn])

serialValue :: Row -> Value
serialValue (Row row) =
  case row of
    [] -> error "serialValue: empty row"
    x:_ -> x

partitioningValue :: Row -> Value
partitioningValue (Row row) = row !! 1

mkParser :: [Text] -> TableSchema -> P.RowParser Row
mkParser cols (TableSchema schema) = Row <$> traverse (parser . getType) cols
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

entryId :: Row -> EntryId
entryId row = case serialValue row of
   Int n -> EntryId $ fromIntegral n
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
  allowedSerialTypes = [PgInt8, PgInt2, PgInt4]

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




