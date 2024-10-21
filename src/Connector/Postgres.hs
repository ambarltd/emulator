module Connector.Postgres where

import Control.Exception (Exception, bracket, throwIO, ErrorCall(..))
import Control.Monad (unless)
import qualified Data.Aeson as Aeson
import Data.ByteString (ByteString)
import Data.Int (Int64)
import Data.List ((\\), intercalate)
import Data.Map.Strict (Map)
import Data.Maybe (fromMaybe)
import qualified Data.Map.Strict as Map
import Data.String (fromString)
import Data.Text (Text)
import qualified Data.Text as Text
import Data.Word (Word64)
import qualified Database.PostgreSQL.Simple as P
import qualified Database.PostgreSQL.Simple.FromField as P
import qualified Database.PostgreSQL.Simple.FromRow as P

import qualified Connector.Poll as Poll
import Connector.Poll (BoundaryTracker(..), Boundaries(..))

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
      "int8"        -> return PgInt8
      "int2"        -> return PgInt2
      "int4"        -> return PgInt4
      "float4"      -> return PgFloat4
      "float8"      -> return PgFloat8
      "bool"        -> return PgBool
      "json"       -> return PgJson
      "bytea"       -> return PgBytea
      "timestamp"   -> return PgTimestamp
      "timestamptz" -> return PgTimestamptz
      "text"        -> return PgText
      _             -> P.conversionError $ UnsupportedType str

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
   deriving Show

-- | A PostgreSQL connection string.
-- https://www.postgresql.org/docs/9.5/libpq-connect.html#LIBPQ-CONNSTRING
newtype ConnectionString = ConnectionString Text

connect :: ConnectorConfig -> IO ()
connect config@ConnectorConfig{..} =
   bracket open P.close $ \conn -> do
   schema <- fetchSchema "test_user" conn
   print schema
   validate config schema
   let pcol = unTableSchema schema Map.! c_partitioningColumn
   withTracker pcol $ \tracker getSerial -> do
      let pc = Poll.PollingConnector
             { Poll.c_getId = getSerial
             , Poll.c_poll = run
             , Poll.c_pollingInterval = undefined -- Duration
             , Poll.c_maxTransactionTime = undefined -- Duration
             , Poll.c_producer = undefined -- Topic.Producer item
             }

          cols = [c_serialColumn, c_partitioningColumn] <>
             ((c_columns \\ [c_serialColumn]) \\ [c_partitioningColumn])

          run (Boundaries bs) = P.queryWith parser conn query ()
            where
            query = fromString $ Text.unpack $ Text.unwords
               [ "SELECT" , Text.intercalate ", " cols
               , "FROM" , c_table
               , "WHERE" , constraints
               , "ORDER BY" , c_serialColumn
               ]
            constraints
              = Text.pack
              $ intercalate "AND"
              $ flip fmap bs
              $ \(low, high) -> unwords
                  [ "("
                  , Text.unpack c_serialColumn, "<", show low
                  , "OR"
                  , Text.unpack c_serialColumn, ">", show high
                  , ")"]

            parser :: P.RowParser Row
            parser = mkParser cols schema

      _ <- Poll.connect tracker pc
      return ()

   where
   open = P.connect P.ConnectInfo
      { P.connectHost = Text.unpack c_host
      , P.connectPort = fromIntegral c_port
      , P.connectUser = Text.unpack c_username
      , P.connectPassword = Text.unpack c_password
      , P.connectDatabase = Text.unpack c_database
      }

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
   -> (forall a. Show a => BoundaryTracker a -> (Row -> a) -> IO b)
   -> IO b
withTracker ty f =
   case ty of
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
      get row = case row of
         [] -> error "empty row"
         Int n : _ -> n
         val : _ -> error "Invalid serial column value:" (show val)

   realTracker = f Poll.rangeTracker get
      where
      get row = case row of
         [] -> error "empty row"
         Real n : _ -> n
         val : _ -> error "Invalid serial column value:" (show val)

validate :: ConnectorConfig -> TableSchema -> IO ()
validate ConnectorConfig{..} (TableSchema schema) = do
   case missing of
     [] -> return ()
     xs -> throwIO $ ErrorCall $ "Missing columns in target table: " <> Text.unpack (Text.unwords xs)

   let serialTy = schema Map.! c_serialColumn
   unless (serialTy `elem` allowedSerialTypes) $
      throwIO $ ErrorCall $ "Invalid serial column type: " <> show serialTy
   where
   allowedSerialTypes = [PgInt8, PgInt2, PgInt4, PgFloat4, PgFloat8]
   missing = filter (not . (`Map.member` schema)) cols
   cols =  c_partitioningColumn : c_serialColumn : c_columns

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




