module Connector.Postgres where

import Control.Exception (Exception, bracket)
import Data.String (fromString)
import Data.Text (Text)
import qualified Data.Text.Encoding as Text
import qualified Database.PostgreSQL.Simple as P
import qualified Database.PostgreSQL.Simple.FromField as P
import GHC.Generics (Generic)

-- | A PostgreSQL connection string.
-- https://www.postgresql.org/docs/9.5/libpq-connect.html#LIBPQ-CONNSTRING
newtype ConnectionString = ConnectionString Text

connect :: ConnectionString -> IO ()
connect (ConnectionString txt) =
   bracket (P.connectPostgreSQL $ Text.encodeUtf8 txt) P.close $ \conn -> do
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

newtype TableSchema = TableSchema [RowSchema]
   deriving Show

data RowSchema = RowSchema
   { row_name :: Text
   , row_type :: PgType
   }
   deriving (Show, Generic, P.FromRow)

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


fetchSchema :: Text -> P.Connection -> IO TableSchema
fetchSchema table conn = TableSchema <$> P.query conn query [table]
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



