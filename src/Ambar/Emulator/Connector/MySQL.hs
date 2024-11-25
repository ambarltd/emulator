module Ambar.Emulator.Connector.MySQL
  ( MySQL(..)
  , MySQLState
  , MySQLRow
  ) where

import Control.Concurrent.STM (STM, TVar, newTVarIO, readTVar)
import Control.Exception (Exception, throw)
import Control.Monad (forM_)
import Data.Aeson (FromJSON, ToJSON)
import qualified Data.Aeson as Aeson
import qualified Data.ByteString.Lazy as LB
import Data.Default (Default(..))
import Data.List ((\\))
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.String (fromString)
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import Data.Text (Text)
import Data.Word (Word16)
import Data.Void (Void)
import qualified Database.MySQL.Base.Types as M (Field(..), Type(..))
import qualified Database.MySQL.Simple as M
import qualified Database.MySQL.Simple.Result as M
import qualified Database.MySQL.Simple.QueryResults as M
import GHC.Generics (Generic)
import qualified Prettyprinter as Pretty
import Prettyprinter (pretty, (<+>))

import qualified Ambar.Emulator.Connector as C
import Ambar.Emulator.Connector.Poll (BoundaryTracker, Boundaries(..), EntryId(..))
import qualified Ambar.Emulator.Connector.Poll as Poll
import Ambar.Emulator.Queue.Topic (Producer, hashPartitioner)
import Ambar.Record (Record(..), Value(..), Bytes(..), TimeStamp(..))
import qualified Ambar.Record.Encoding as Encoding

import Database.MySQL (Connection, ConnectionInfo(..), query_, withConnection)
import Utils.Delay (Duration, millis, seconds)
import Utils.Async (withAsyncThrow)
import Utils.Logger (SimpleLogger, logDebug, logInfo)
import Utils.Prettyprinter (renderPretty, sepBy, commaSeparated, prettyJSON)

_POLLING_INTERVAL :: Duration
_POLLING_INTERVAL = millis 50

_MAX_TRANSACTION_TIME :: Duration
_MAX_TRANSACTION_TIME = seconds 120

data MySQL = MySQL
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

newtype MySQLState = MySQLState BoundaryTracker
  deriving (Generic)
  deriving newtype (Default)
  deriving anyclass (FromJSON, ToJSON)

newtype MySQLRow = MySQLRow { unMySQLRow :: Record }

newtype RawRow = RawRow [RawValue]

instance M.QueryResults RawRow where
  convertResults fs mbs = RawRow $ zipWith M.convert fs mbs

newtype RawValue = RawValue { unRawValue :: Value }

instance M.Result RawValue where
  convert _ Nothing = RawValue Null
  convert field mbs@(Just bs) = RawValue $
    case M.fieldType field of
      M.Decimal -> Real $ M.convert field mbs
      M.Long -> Real $ M.convert field mbs
      M.Float -> Real $ M.convert field mbs
      M.Double -> Real $ M.convert field mbs
      M.NewDecimal -> Real $ M.convert field mbs
      M.Null -> Null
      M.Timestamp -> DateTime $ TimeStamp (Text.decodeUtf8 bs) (M.convert field mbs)
      M.Tiny -> Int $ M.convert field mbs
      M.Short -> Int $ M.convert field mbs
      M.LongLong -> Int $ M.convert field mbs
      M.Int24 -> Int $ M.convert field mbs
      M.Date -> unsupported
      M.Time -> unsupported
      M.DateTime -> unsupported
      M.Year -> unsupported
      M.NewDate -> unsupported
      M.Bit -> unsupported
      M.Enum -> unsupported
      M.Set -> unsupported
      M.TinyBlob -> Binary $ Bytes $ M.convert field mbs
      M.MediumBlob -> Binary $ Bytes $ M.convert field mbs
      M.LongBlob -> Binary $ Bytes $ M.convert field mbs
      M.Blob -> Binary $ Bytes $ M.convert field mbs
      M.VarChar -> String $ M.convert field mbs
      M.VarString -> String $ M.convert field mbs
      M.String -> String $ M.convert field mbs
      M.Geometry -> unsupported
      M.Json ->
        case Aeson.eitherDecode' $ LB.fromStrict bs of
          Left err -> throw $ M.ConversionFailed
            { M.errSQLType = show $ M.fieldType field
            , M.errHaskellType = "Aeson.Value"
            , M.errFieldName = Text.unpack $ Text.decodeUtf8 $ M.fieldName field
            , M.errMessage = "Unable to decode JSON input: " <> err
            }
          Right v -> Json (M.convert field mbs) v
    where
    unsupported = throw $ M.Incompatible
      { M.errSQLType = show $ M.fieldType field
      , M.errHaskellType = "UNSUPPORTED"
      , M.errFieldName = Text.unpack $ Text.decodeUtf8 $ M.fieldName field
      , M.errMessage = "Type not supported by the MySQL Connector"
      }

instance C.Connector MySQL where
  type ConnectorState MySQL = MySQLState
  type ConnectorRecord MySQL = MySQLRow
  -- | A rows gets saved in the database as a JSON object with
  -- the columns specified in the config file as keys.
  encoder = LB.toStrict . Aeson.encode . Encoding.encode @Aeson.Value . unMySQLRow
  partitioner = hashPartitioner partitioningValue
  connect = connect

partitioningValue :: MySQLRow -> Value
partitioningValue (MySQLRow (Record row)) = snd $ row !! 1

newtype MySQLSchema = MySQLSchema { unTableSchema :: Map Text MySQLType }
  deriving Show

-- | All MySQL types
newtype MySQLType = MySQLType Text
  deriving (Show, Eq)

instance M.Result MySQLType where
  convert field mb = MySQLType (M.convert field mb)

data UnsupportedMySQLType = UnsupportedMySQLType String
  deriving (Show, Exception)

connect
  :: MySQL
  -> SimpleLogger
  -> MySQLState
  -> Producer MySQLRow
  -> (STM MySQLState -> IO a)
  -> IO a
connect config@MySQL{..} logger (MySQLState tracker) producer f =
  withConnection cinfo $ \conn -> do
  schema <- fetchSchema c_table conn
  validate config schema
  trackerVar <- newTVarIO tracker
  let readState = MySQLState <$> readTVar trackerVar
  withAsyncThrow (consume conn trackerVar) (f readState)
  where
  cinfo = ConnectionInfo
    { conn_host = c_host
    , conn_port = c_port
    , conn_username = c_username
    , conn_password = c_password
    , conn_database = c_database
    }

  fetchSchema :: Text -> Connection -> IO MySQLSchema
  fetchSchema table conn = do
    cols <- query_ conn $ fromString $ "DESCRIBE " <> Text.unpack c_database <> "." <> Text.unpack table
    return $ MySQLSchema $ Map.fromList $ fromCol <$> cols
    where
    fromCol :: (Text, MySQLType, Maybe Bool, Maybe Text, Maybe Text, Maybe Text) -> (Text, MySQLType)
    fromCol (field, ty, _, _, _, _) = (field, ty)

  consume
     :: Connection
     -> TVar BoundaryTracker
     -> IO Void
  consume conn trackerVar = Poll.connect trackerVar pc
    where
    pc = Poll.PollingConnector
       { Poll.c_getId = entryId
       , Poll.c_poll = \bs -> Poll.streamed $ run bs
       , Poll.c_pollingInterval = _POLLING_INTERVAL
       , Poll.c_maxTransactionTime = _MAX_TRANSACTION_TIME
       , Poll.c_producer = producer
       }

    toRow :: RawRow -> MySQLRow
    toRow (RawRow rawValues) = MySQLRow $ Record $ zip cols vals
      where
      vals = fmap unRawValue rawValues
      cols = columns config

    run :: Boundaries -> IO [MySQLRow]
    run (Boundaries bs) = do
      logDebug logger query
      raw <- query_ conn (fromString query)
      let results = fmap toRow raw
      forM_ results logResult
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


-- | Columns in the order they will be queried.
columns :: MySQL -> [Text]
columns MySQL{..} =
  [c_incrementingColumn, c_partitioningColumn] <>
    ((c_columns \\ [c_incrementingColumn]) \\ [c_partitioningColumn])

entryId :: MySQLRow -> EntryId
entryId row =
  case serialValue row of
    Int n -> EntryId $ fromIntegral n
    val -> error "Invalid serial column value:" (show val)

serialValue :: MySQLRow -> Value
serialValue (MySQLRow (Record row)) =
  case row of
    [] -> error "serialValue: empty row"
    (_,x):_ -> x

validate :: MySQL -> MySQLSchema -> IO ()
validate _ _ = do
  -- TODO: Validate
  return ()
