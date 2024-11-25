module Ambar.Emulator.Connector.MySQL
  ( MySQL(..)
  , MySQLState
  , MySQLRow
  ) where

import Control.Concurrent.STM (STM, TVar, newTVarIO, readTVar)
import Control.Exception (Exception, bracket)
import Data.Aeson (FromJSON, ToJSON)
import qualified Data.Aeson as Aeson
import qualified Data.ByteString.Lazy as LB
import Data.Default (Default(..))
import Data.List ((\\))
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Text as Text
import Data.Text (Text)
import Data.Typeable (Typeable)
import Data.Word (Word16)
import Data.Void (Void)
import qualified Database.MySQL.Simple as M
import GHC.Generics (Generic)

import qualified Ambar.Emulator.Connector as C
import Ambar.Emulator.Connector.Poll (BoundaryTracker, Boundaries(..), EntryId(..), Stream)
import qualified Ambar.Emulator.Connector.Poll as Poll
import Ambar.Emulator.Queue.Topic (Producer, hashPartitioner)
import Ambar.Record (Record(..), Value(..))
import qualified Ambar.Record.Encoding as Encoding

import Utils.Delay (Duration, millis, seconds)

import Utils.Async (withAsyncThrow)
import Utils.Logger (SimpleLogger)

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
  , c_serialColumn :: Text
  }

newtype MySQLState = MySQLState BoundaryTracker
  deriving (Generic)
  deriving newtype (Default)
  deriving anyclass (FromJSON, ToJSON)

newtype MySQLRow = MySQLRow { unMySQLRow :: Record }

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

-- | Supported MySQL types
data MySQLType
  = MySQLInt8
  | MySQLInt2
  | MySQLInt4
  | MySQLFloat4
  | MySQLFloat8
  | MySQLBool
  | MySQLJson
  | MySQLBytea
  | MySQLTimestamp
  | MySQLTimestamptz
  | MySQLText
  deriving (Show, Eq, Typeable)
  deriving anyclass (M.Result)

data UnsupportedMySQLType = UnsupportedMySQLType String
  deriving (Show, Exception)

instance M.FromField MySQLType where
  fromField = undefined

connect
  :: MySQL
  -> SimpleLogger
  -> MySQLState
  -> Producer MySQLRow
  -> (STM MySQLState -> IO a)
  -> IO a
connect config@MySQL{..} _ (MySQLState tracker) producer f =
  withConnection $ \conn -> do
  schema <- fetchSchema c_table conn
  validate config schema
  trackerVar <- newTVarIO tracker
  let readState = MySQLState <$> readTVar trackerVar
  withAsyncThrow (consume conn schema trackerVar) (f readState)
  where
  withConnection = bracket open M.close
    where
    open = M.connect M.defaultConnectInfo
       { M.connectHost = Text.unpack c_host
       , M.connectPort = c_port
       , M.connectUser = Text.unpack c_username
       , M.connectPassword = Text.unpack c_password
       , M.connectDatabase = Text.unpack c_database
       }

  fetchSchema :: Text -> M.Connection -> IO MySQLSchema
  fetchSchema table conn = do
    cols <- M.query conn "DESCRIBE" [c_database <> "." <> table]
    return $ MySQLSchema $ Map.fromList $ fromCol <$> cols
    where
    fromCol :: (Text, MySQLType, Bool, Text, Text, Text) -> (Text, MySQLType)
    fromCol (field, ty, _, _, _, _) = (field, ty)

  consume
     :: M.Connection
     -> MySQLSchema
     -> TVar BoundaryTracker
     -> IO Void
  consume _ _ trackerVar = Poll.connect trackerVar pc
    where
    pc = Poll.PollingConnector
       { Poll.c_getId = entryId
       , Poll.c_poll = run
       , Poll.c_pollingInterval = _POLLING_INTERVAL
       , Poll.c_maxTransactionTime = _MAX_TRANSACTION_TIME
       , Poll.c_producer = producer
       }

    run :: Boundaries -> Stream MySQLRow
    run _ = error "TODO: run" (columns config)

-- | Columns in the order they will be queried.
columns :: MySQL -> [Text]
columns MySQL{..} =
  [c_serialColumn, c_partitioningColumn] <>
    ((c_columns \\ [c_serialColumn]) \\ [c_partitioningColumn])

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
validate = error "TODO: validate"
