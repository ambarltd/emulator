module Ambar.Emulator.Connector.MicrosoftSQLServer
  ( SQLServer(..)
  , SQLServerState
  , SQLServerRow
  ) where

import Control.Concurrent.STM (STM, TVar, newTVarIO, readTVar)
import Data.Aeson (FromJSON, ToJSON)
import qualified Data.Aeson as Aeson
import qualified Data.ByteString.Lazy as LB
import Data.Default (Default(..))
import Data.List ((\\))
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.String (fromString)
import Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import Data.Void (Void)
import Data.Word (Word16)
import GHC.Generics (Generic)
import qualified Prettyprinter as Pretty
import Prettyprinter (pretty, (<+>))

import Database.MicrosoftSQLServer
  ( Connection
  , ConnectionInfo(..)
  , withConnection
  , RowParser
  , FieldParser
  , fieldParser
  )
import qualified Database.MicrosoftSQLServer as M

import qualified Ambar.Emulator.Connector as C
import Ambar.Emulator.Connector.Poll (BoundaryTracker, Boundaries(..), EntryId(..))
import qualified Ambar.Emulator.Connector.Poll as Poll
import Ambar.Emulator.Queue.Topic (Producer, hashPartitioner)
import Ambar.Record (Record(..), Value(..), Type(..), Bytes(..), TimeStamp(..))
import qualified Ambar.Record.Encoding as Encoding

import Utils.Delay (Duration, millis, seconds)
import Utils.Async (withAsyncThrow)
import Utils.Logger (SimpleLogger, logDebug, logInfo)
import Utils.Prettyprinter (renderPretty, sepBy, commaSeparated, prettyJSON)

_POLLING_INTERVAL :: Duration
_POLLING_INTERVAL = millis 50

_MAX_TRANSACTION_TIME :: Duration
_MAX_TRANSACTION_TIME = seconds 120

data SQLServer = SQLServer
  { c_host :: Text
  , c_port :: Word16
  , c_username :: Text
  , c_password :: Text
  , c_database :: Text
  , c_table :: Text
  , c_columns :: [Text]
  , c_partitioningColumn :: Text
  , c_incrementingColumn :: Text
  }

newtype SQLServerState = SQLServerState BoundaryTracker
  deriving (Generic)
  deriving newtype (Default)
  deriving anyclass (FromJSON, ToJSON)

data SQLServerRow = SQLServerRow { unSQLServerRow :: Record }

instance C.Connector SQLServer where
  type ConnectorState SQLServer = SQLServerState
  type ConnectorRecord SQLServer = SQLServerRow
  -- | A rows gets saved in the database as a JSON object with
  -- the columns specified in the config file as keys.
  encoder = LB.toStrict . Aeson.encode . Encoding.encode @Aeson.Value . unSQLServerRow
  partitioner = hashPartitioner partitioningValue
  connect = connect

connect
  :: SQLServer
  -> SimpleLogger
  -> SQLServerState
  -> Producer SQLServerRow
  -> (STM SQLServerState -> IO a)
  -> IO a
connect config@SQLServer{..} logger (SQLServerState tracker) producer f =
  withConnection cinfo $ \conn -> do
  schema <- fetchSchema c_table conn
  validate config schema
  trackerVar <- newTVarIO tracker
  let readState = SQLServerState <$> readTVar trackerVar
  withAsyncThrow (consume conn schema trackerVar) (f readState)
  where
  cinfo = ConnectionInfo
    { conn_host = c_host
    , conn_port = c_port
    , conn_username = c_username
    , conn_password = c_password
    , conn_database = c_database
    }

  consume
     :: Connection
     -> Schema
     -> TVar BoundaryTracker
     -> IO Void
  consume conn schema trackerVar = Poll.connect trackerVar pc
    where
    pc = Poll.PollingConnector
       { Poll.c_getId = entryId
       , Poll.c_poll = \bs -> Poll.streamed (run bs)
       , Poll.c_pollingInterval = _POLLING_INTERVAL
       , Poll.c_maxTransactionTime = _MAX_TRANSACTION_TIME
       , Poll.c_producer = producer
       }

    parser :: RowParser SQLServerRow
    parser = mkParser (columns config) schema

    run :: Boundaries -> IO [SQLServerRow]
    run (Boundaries bs) = do
      results <- M.queryWith parser conn query
      mapM_ logResult results
      logDebug logger $ "results: " <> show (length results)
      return results
      where
      query = fromString $ Text.unpack $ renderPretty $ Pretty.fillSep
         [ "SELECT" , commaSeparated $ map pretty $ columns config
         , "FROM" , pretty c_table
         , if null bs then "" else "WHERE" <> constraints
         , "ORDER BY" , pretty c_incrementingColumn
         ]

      constraints = sepBy "AND"
         [ Pretty.fillSep
            [ "("
            , pretty c_incrementingColumn, "<", pretty low
            , "OR"
            ,  pretty high, "<", pretty c_incrementingColumn
            , ")"]
         | (EntryId low, EntryId high) <- bs
         ]

      logResult row =
       logInfo logger $ renderPretty $
         "ingested." <+> commaSeparated
           [ "serial_value:" <+> prettyJSON (serialValue row)
           , "partitioning_value:" <+> prettyJSON (partitioningValue row)
           ]

newtype Schema = Schema (Map Text Type)
  deriving Show

newtype MSSQLType = MSSQLType Text

fetchSchema :: Text -> Connection -> IO Schema
fetchSchema table conn = do
  cols <- M.queryWith parser conn query
  return $ Schema $ Map.fromList cols
  where
  parser :: RowParser (Text, Type)
  parser = do
    colName <- M.parseField
    colTy <- M.parseField
    case toExpectedType (MSSQLType colTy) of
      Just ty -> return (colName, ty)
      Nothing -> fail $
        "unsupported Microsoft SQL Server type: '" <> Text.unpack colTy <> "'"

  query = fromString $ unwords
    [ "select COLUMN_NAME, DATA_TYPE"
    , "from INFORMATION_SCHEMA.COLUMNS"
    , "where TABLE_NAME='" <> Text.unpack table <> "'"
    ]

toExpectedType :: MSSQLType -> Maybe Type
toExpectedType (MSSQLType ty) = case Text.toUpper ty of
  "TEXT" -> Just TString
  "INT" -> Just TInteger
  _ -> Nothing

validate :: SQLServer -> Schema -> IO ()
validate _ _ = do
  -- TODO: Validate
  return ()

entryId :: SQLServerRow -> EntryId
entryId row =
  case serialValue row of
    UInt n -> EntryId $ fromIntegral n
    Int n -> EntryId $ fromIntegral n
    val -> error "Invalid serial column value:" (show val)

partitioningValue :: SQLServerRow -> Value
partitioningValue (SQLServerRow (Record row)) = snd $ row !! 1

serialValue :: SQLServerRow -> Value
serialValue (SQLServerRow (Record row)) =
  case row of
    [] -> error "serialValue: empty row"
    (_,x):_ -> x

-- | Columns in the order they will be queried.
columns :: SQLServer -> [Text]
columns SQLServer{..} =
  [c_incrementingColumn, c_partitioningColumn] <>
    ((c_columns \\ [c_incrementingColumn]) \\ [c_partitioningColumn])

mkParser :: [Text] -> Schema -> RowParser SQLServerRow
mkParser cols (Schema schema) = do
  values <- mapM (M.parseFieldWith . parse) cols
  return $ SQLServerRow $ Record $ zip cols values
  where
  parse :: Text -> FieldParser Value
  parse cname = do
    ty <- case Map.lookup cname schema of
      Nothing -> fail $ "unknown column: " <> Text.unpack cname
      Just v -> return v
    M.Field _ mbytes <- M.fieldInfo
    case mbytes of
      Nothing -> return Null
      Just _ -> parseFieldWithType ty

  parseFieldWithType :: Type -> FieldParser Value
  parseFieldWithType = \case
    TBoolean -> Boolean <$> fieldParser
    TUInteger -> UInt . fromInteger <$> fieldParser
    TInteger -> Int . fromInteger <$> fieldParser
    TReal -> Real <$> fieldParser
    TString -> String <$> fieldParser
    TBytes -> Binary . Bytes <$> fieldParser
    TJSON -> do
      txt <- fieldParser
      value <- case Aeson.eitherDecode' $ LB.fromStrict $ Text.encodeUtf8 txt of
        Right r -> return r
        Left err -> fail $ "Invalid JSON: " <> err
      return $ Json txt value
    TDateTime -> do
      txt <- fieldParser
      utc <- fieldParser
      return $ DateTime $ TimeStamp txt utc
