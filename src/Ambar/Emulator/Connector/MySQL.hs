module Ambar.Emulator.Connector.MySQL
  ( MySQL(..)
  , MySQLState
  , MySQLRow
  ) where

import Control.Concurrent.STM (STM, TVar, newTVarIO, readTVar)
import Control.Exception (Exception)
import Control.Applicative (many)
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
import GHC.Generics (Generic)
import qualified Prettyprinter as Pretty
import Prettyprinter (pretty, (<+>))

import qualified Ambar.Emulator.Connector as C
import Ambar.Emulator.Connector.Poll (BoundaryTracker, Boundaries(..), EntryId(..), Stream)
import qualified Ambar.Emulator.Connector.Poll as Poll
import Ambar.Emulator.Queue.Topic (Producer, hashPartitioner)
import Ambar.Record (Record(..), Value(..), Bytes(..), TimeStamp(..))
import qualified Ambar.Record.Encoding as Encoding

import Database.MySQL
  ( FromRow(..)
  , FromField(..)
  , Connection
  , ConnectionInfo(..)

  , fieldInfo
  , fieldParseError
  , withConnection
  )
import qualified Database.MySQL as M
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

instance FromRow RawRow where
  rowParser = RawRow <$> many M.parseField

newtype RawValue = RawValue { unRawValue :: Value }

instance FromField RawValue where
  fieldParser = do
    (field, mbs) <- fieldInfo
    fmap RawValue $ case mbs of
      Nothing -> pure Null
      Just bs -> case M.fieldType field of
        M.Decimal -> Real <$> fieldParser
        M.Long -> Real <$> fieldParser
        M.Float -> Real <$> fieldParser
        M.Double -> Real <$> fieldParser
        M.NewDecimal -> Real <$> fieldParser
        M.Null -> pure Null
        M.Timestamp -> DateTime . TimeStamp (Text.decodeUtf8 bs) <$> fieldParser
        M.Tiny -> Int <$> fieldParser
        M.Short -> Int <$> fieldParser
        M.LongLong -> Int <$> fieldParser
        M.Int24 -> Int <$> fieldParser
        M.Date -> unsupported
        M.Time -> unsupported
        M.DateTime -> unsupported
        M.Year -> unsupported
        M.NewDate -> unsupported
        M.Bit -> unsupported
        M.Enum -> unsupported
        M.Set -> unsupported
        M.TinyBlob -> Binary . Bytes <$> fieldParser
        M.MediumBlob -> Binary . Bytes <$> fieldParser
        M.LongBlob -> Binary . Bytes <$> fieldParser
        M.Blob -> Binary . Bytes <$> fieldParser
        M.VarChar -> String <$> fieldParser
        M.VarString -> String <$> fieldParser
        M.String -> String <$> fieldParser
        M.Geometry -> unsupported
        M.Json ->
          case Aeson.eitherDecode' $ LB.fromStrict bs of
            Left err -> fieldParseError $ M.ConversionFailed
              { M.errSQLType = show $ M.fieldType field
              , M.errHaskellType = "Aeson.Value"
              , M.errFieldName = Text.unpack $ Text.decodeUtf8 $ M.fieldName field
              , M.errMessage = "Unable to decode JSON input: " <> err
              }
            Right v -> (\val -> Json val v) <$> fieldParser
    where
    unsupported = fail "Type not supported by the MySQL Connector"

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
  deriving newtype (FromField)

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
    cols <- M.query_ conn $ fromString $ "DESCRIBE " <> Text.unpack c_database <> "." <> Text.unpack table
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
       , Poll.c_poll = run
       , Poll.c_pollingInterval = _POLLING_INTERVAL
       , Poll.c_maxTransactionTime = _MAX_TRANSACTION_TIME
       , Poll.c_producer = producer
       }

    toRow :: RawRow -> MySQLRow
    toRow (RawRow rawValues) = MySQLRow $ Record $ zip cols vals
      where
      vals = fmap unRawValue rawValues
      cols = columns config

    run :: Boundaries -> Stream MySQLRow
    run (Boundaries bs) acc0 emit = do
      logDebug logger query
      (acc, count) <- M.fold_ conn (fromString query) (acc0, 0 :: Int) $
        \(acc, !count) r -> do
          let row = toRow r
          logResult row
          acc' <- emit acc row
          return (acc', succ count)
      logDebug logger $ "results: " <> show count
      return acc
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
