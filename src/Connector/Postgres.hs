module Connector.Postgres where

import Control.Exception (Exception, bracket, throwIO, ErrorCall(..))
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.String (fromString)
import Data.Text (Text)
import qualified Data.Text as Text
import qualified Database.PostgreSQL.Simple as P
import qualified Database.PostgreSQL.Simple.FromField as P

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

newtype TableSchema = TableSchema (Map Text PgType)
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
   deriving Show

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
      "json"        -> return PgJson
      "bytea"       -> return PgBytea
      "timestamp"   -> return PgTimestamp
      "timestamptz" -> return PgTimestamptz
      "text"        -> return PgText
      _             -> P.conversionError $ UnsupportedType str

-- | A PostgreSQL connection string.
-- https://www.postgresql.org/docs/9.5/libpq-connect.html#LIBPQ-CONNSTRING
newtype ConnectionString = ConnectionString Text

connect :: ConnectorConfig -> IO ()
connect config =
   bracket open P.close $ \conn -> do
   {-
      let pc = Poll.PollingConnector
             { Poll.c_getId = undefined -- item -> id
             , Poll.c_poll = undefined -- Boundaries id -> IO [item]  -- ^ run query
             , Poll.c_pollingInterval = undefined -- Duration
             , Poll.c_maxTransactionTime = undefined -- Duration
             , Poll.c_producer = undefined -- Topic.Producer item
             }
      Poll.connect @Int Poll.rangeTracker pc
   -}
   schema <- fetchSchema "test_user" conn
   print schema
   validate config schema
   where
   open = P.connect P.ConnectInfo
      { P.connectHost = Text.unpack $ c_host config
      , P.connectPort = fromIntegral $ c_port config
      , P.connectUser = Text.unpack $ c_username config
      , P.connectPassword = Text.unpack $ c_password config
      , P.connectDatabase = Text.unpack $ c_database config
      }

validate :: ConnectorConfig -> TableSchema -> IO ()
validate ConnectorConfig{..} (TableSchema schema) =
   case missing of
     [] -> return ()
     xs -> throwIO $ ErrorCall $ "Missing columns in target table: " <> Text.unpack (Text.unwords xs)
   where
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




