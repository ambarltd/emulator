module Ambar.Emulator where

import Control.Concurrent.Async (concurrently_, forConcurrently_)
import Control.Concurrent.STM (STM)
import Control.Exception (finally, uninterruptibleMask_, throwIO, ErrorCall(..))
import Control.Monad (forM)
import Data.Aeson (FromJSON, ToJSON)
import qualified Data.Aeson as Aeson
import Data.Default (def)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe (fromMaybe)
import Foreign.Marshal.Utils (withMany)
import GHC.Generics (Generic)
import System.Directory (doesFileExist)
import System.FilePath ((</>))

import Ambar.Emulator.Connector.Postgres (PostgreSQLState)
import Ambar.Emulator.Connector (Connector(..), connect, partitioner, encoder)

import qualified Ambar.Emulator.Projector as Projector
import Ambar.Emulator.Projector (Projection(..))
import qualified Ambar.Transport.File as FileTransport
import qualified Ambar.Transport.Http as HttpTransport
import qualified Ambar.Emulator.Queue.Topic as Topic
import Ambar.Emulator.Queue (Queue, TopicName(..))
import qualified Ambar.Emulator.Queue as Queue
import Ambar.Emulator.Config
  ( EmulatorConfig(..)
  , EnvironmentConfig(..)
  , Id(..)
  , DataSource(..)
  , Source(..)
  , DataDestination(..)
  , Destination(..)
  )
import Utils.Delay (every, seconds)
import Utils.Directory (writeAtomically)
import Utils.Logger (SimpleLogger, annotate, logInfo, logDebugAction)
import Utils.Some (Some(..))
import Utils.STM (atomicallyNamed)

emulate :: SimpleLogger -> EmulatorConfig -> EnvironmentConfig -> IO ()
emulate logger_ config env = do
  Queue.withQueue queuePath pcount $ \queue ->
    concurrently_ (connectAll queue) (projectAll queue)
  where
  queuePath = c_dataPath config </> "queues"
  statePath = c_dataPath config </> "state.json"
  pcount = Topic.PartitionCount $ c_partitionsPerTopic config

  connectAll queue = do
    EmulatorState connectorStates <- load
    let getState source =
          fromMaybe (initialStateFor source) $
          Map.lookup (s_id source) connectorStates

    configs <- sequence
      [ toConnectorConfig source (getState source)
      | source <- Map.elems $ c_sources env
      ]

    withMany (connect_ queue) configs $ \svars ->
      every (seconds 30) (save svars) `finally` save svars

  load = do
    exists <- doesFileExist statePath
    if not exists
    then return (EmulatorState def)
    else do
      r <- Aeson.eitherDecodeFileStrict statePath
      case r of
        Right v -> return v
        Left err ->
          throwIO $ ErrorCall $ "Unable to decode emulator state: " <> show err

  save svars =
    logDebugAction logger_ "saving state" $
    uninterruptibleMask_ $ do
      -- reading is non-blocking so should be fine to run under uninterruptibleMask
      states <- forM svars $ \(sid, svar) -> (sid,) <$> atomicallyNamed "emulator.save" svar
      writeAtomically statePath $ \path ->
        Aeson.encodeFile path $ EmulatorState (Map.fromList states)

  connect_ :: Queue -> ConnectorConfig -> ((Id DataSource, STM SavedState) -> IO a) -> IO a
  connect_ queue (ConnectorConfig source connector state asSaved) f = do
    let logger = annotate ("src: " <> unId (s_id source)) logger_
    topic <- Queue.openTopic queue $ topicName $ s_id source
    Topic.withProducer topic partitioner encoder $ \producer ->
      connect connector logger state producer $ \stateVar -> do
      logInfo @String logger "connected"
      f (s_id source, asSaved <$> stateVar)

  initialStateFor source =
    case s_source source of
      SourcePostgreSQL _ -> StatePostgres def
      SourceFile _ -> StateFile ()

  projectAll queue = forConcurrently_ (c_destinations env) (project queue)

  project queue dest =
    withDestination dest $ \transport -> do
    sourceTopics <- forM (d_sources dest) $ \source -> do
      topic <- Queue.openTopic queue (topicName $ s_id source)
      return (source, topic)

    Projector.project logger_ Projection
        { p_id = projectionId (d_id dest)
        , p_destination = d_id dest
        , p_destinationDescription = d_description dest
        , p_sources = sourceTopics
        , p_transport = transport
        }

  withDestination dest act =
    case d_destination dest of
      DestinationFile path ->
        FileTransport.withFileTransport path (act . Some)
      DestinationHttp{..} -> do
        transport <-  HttpTransport.new d_endpoint d_username d_password
        act (Some transport)
      DestinationFun f -> do
        act (Some f)

-- | This is what gets persisted across sessions.
newtype EmulatorState = EmulatorState
  { connectors :: Map (Id DataSource) SavedState
  }
  deriving (Generic)
  deriving anyclass (ToJSON, FromJSON)

data SavedState
  = StatePostgres PostgreSQLState
  | StateFile ()
  deriving (Generic)
  deriving anyclass (ToJSON, FromJSON)

data ConnectorConfig =
  forall c. (Connector c) =>
  ConnectorConfig
    { cc_source :: DataSource
    , cc_connector :: c
    , cc_connstate :: ConnectorState c
    , cc_asSavedState :: ConnectorState c -> SavedState
    }

toConnectorConfig :: DataSource -> SavedState -> IO ConnectorConfig
toConnectorConfig source sstate =
  case s_source source of
    SourcePostgreSQL psql ->
      case sstate of
        StatePostgres state ->
          return $ ConnectorConfig source psql state StatePostgres
        _ -> incompatible
    SourceFile path ->
      case sstate of
        StateFile () ->
          return $ ConnectorConfig source path () StateFile
        _ -> incompatible
    where
    incompatible = throwIO $ ErrorCall $
      "Incompatible state for source: " <> show (s_id source)

topicName :: Id DataSource -> TopicName
topicName sid = TopicName $ "t-" <> unId sid

projectionId :: Id DataDestination -> Id Projection
projectionId (Id dst) = Id ("p-" <> dst)
