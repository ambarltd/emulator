module Ambar.Emulator.Connector.Postgres
  ( PostgreSQL(..)
  , PostgreSQLState
  , UnsupportedType(..)
  ) where

import Control.Concurrent.STM (STM, TVar, newTVarIO, readTVar)
import Control.Exception (Exception, bracket, throwIO, ErrorCall(..))
import Data.Aeson (FromJSON, ToJSON)
import qualified Data.Aeson as Aeson
import qualified Data.ByteString.Lazy as LB
import Data.Default (Default(..))
import Data.List ((\\))
import Data.Map.Strict (Map)
import Data.Maybe (fromMaybe)
import qualified Data.Map.Strict as Map
import Data.String (fromString)
import Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.Text.Lazy as Text (toStrict)
import qualified Data.Text.Lazy.Encoding as Text (decodeUtf8)
import Data.Void (Void)
import Data.Word (Word16)
import qualified Database.PostgreSQL.Simple as P
import qualified Database.PostgreSQL.Simple.Transaction as P
import qualified Database.PostgreSQL.Simple.FromField as P
import qualified Database.PostgreSQL.Simple.FromRow as P
import GHC.Generics (Generic)
import Utils.Prettyprinter (renderPretty, sepBy, commaSeparated, prettyJSON)
import Prettyprinter (pretty, (<+>))
import qualified Prettyprinter as Pretty

import qualified Ambar.Emulator.Connector.Poll as Poll
import qualified Ambar.Emulator.Connector as C
import Ambar.Emulator.Connector.Poll (BoundaryTracker, Boundaries(..), EntryId(..), Stream)
import Ambar.Emulator.Queue.Topic (Producer, hashPartitioner)
import Ambar.Record (Record(..), Value(..), Bytes(..))
import qualified Ambar.Record.Encoding as Encoding
import Utils.Async (withAsyncThrow)
import Utils.Delay (Duration, millis, seconds)
import Utils.Logger (SimpleLogger, logDebug, logInfo)

_POLLING_INTERVAL :: Duration
_POLLING_INTERVAL = millis 50

_MAX_TRANSACTION_TIME :: Duration
_MAX_TRANSACTION_TIME = seconds 120

data PostgreSQL = PostgreSQL
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

instance C.Connector PostgreSQL where
  type ConnectorState PostgreSQL = PostgreSQLState
  type ConnectorRecord PostgreSQL = Record

  partitioner = hashPartitioner partitioningValue

  -- | A rows gets saved in the database as a JSON object with
  -- the columns specified in the config file as keys.
  encoder = LB.toStrict . Aeson.encode . Encoding.encode @Aeson.Value

  connect = connect

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

-- | Opaque serializable connector state
newtype PostgreSQLState = PostgreSQLState BoundaryTracker
  deriving (Generic)
  deriving newtype (Default)
  deriving anyclass (FromJSON, ToJSON)

connect
  :: PostgreSQL
  -> SimpleLogger
  -> PostgreSQLState
  -> Producer Record
  -> (STM PostgreSQLState -> IO a)
  -> IO a
connect config@PostgreSQL{..} logger (PostgreSQLState tracker) producer f =
   bracket open P.close $ \conn -> do
   schema <- fetchSchema c_table conn
   validate config schema
   trackerVar <- newTVarIO tracker
   let readState = PostgreSQLState <$> readTVar trackerVar
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

     run :: Boundaries -> Stream Record
     run (Boundaries bs) acc0 emit = do
       logDebug logger query
       (acc, count) <- P.foldWithOptionsAndParser opts parser conn (fromString query) () (acc0, 0) $
         \(acc, !count) record -> do
           logResult record
           acc' <- emit acc record
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

-- | Columns in the order they will be queried.
columns :: PostgreSQL -> [Text]
columns PostgreSQL{..} =
  [c_serialColumn, c_partitioningColumn] <>
    ((c_columns \\ [c_serialColumn]) \\ [c_partitioningColumn])

serialValue :: Record -> Value
serialValue (Record row) =
  case row of
    [] -> error "serialValue: empty row"
    (_,x):_ -> x

partitioningValue :: Record -> Value
partitioningValue (Record row) = snd $ row !! 1

mkParser :: [Text] -> TableSchema -> P.RowParser Record
mkParser cols (TableSchema schema) = Record . zip cols <$> traverse (parser . getType) cols
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
    PgJson -> do
      mvalue <- P.field
      return $ flip fmap mvalue $ \value ->
        let txt = Text.toStrict $ Text.decodeUtf8 $ (Aeson.encode value) in
        Json txt value
    PgBytea -> fmap (Binary . Bytes . P.fromBinary) <$> P.field
    PgTimestamp -> fmap DateTime <$> P.field
    PgTimestamptz -> fmap DateTime <$> P.field
    PgText -> fmap String <$> P.field

entryId :: Record -> EntryId
entryId record = case serialValue record of
   Int n -> EntryId $ fromIntegral n
   val -> error "Invalid serial column value:" (show val)

validate :: PostgreSQL -> TableSchema -> IO ()
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




