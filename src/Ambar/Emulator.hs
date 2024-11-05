module Ambar.Emulator where

import Control.Concurrent.STM (atomically)
import Control.Concurrent.Async (concurrently_, forConcurrently_, withAsync)
import Control.Exception (finally, uninterruptibleMask_)
import Control.Monad (forM_, forM)
import Data.Aeson (FromJSON, ToJSON)
import qualified Data.Aeson as Aeson
import Data.Map (Map)
import qualified Data.Map as Map
import qualified Data.HashMap.Strict as HashMap
import Foreign.Marshal.Utils (withMany)
import GHC.Generics (Generic)
import System.FilePath ((</>))

import qualified Ambar.Emulator.Connector.Postgres as Postgres
import qualified Ambar.Emulator.Connector.File as FileConnector

import qualified Ambar.Emulator.Projector as Projector
import Ambar.Emulator.Projector (Projection(..))
import qualified Ambar.Transport.File as FileTransport
import qualified Ambar.Transport.Http as HttpTransport
import qualified Ambar.Emulator.Queue.Topic as Topic
import Ambar.Emulator.Queue (TopicName(..))
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
import Utils.Logger (SimpleLogger, logInfo)
import Utils.Some (Some(..))
import Utils.Delay (every, seconds)

data ConnectorState
  = StatePostgres Postgres.ConnectorState
  | StateFile ()
  deriving (Generic)
  deriving anyclass (ToJSON, FromJSON)

newtype EmulatorState = EmulatorState
  { connectors :: Map (Id DataSource) ConnectorState
  }
  deriving (Generic)
  deriving anyclass (ToJSON, FromJSON)

emulate :: SimpleLogger -> EmulatorConfig -> EnvironmentConfig -> IO ()
emulate logger config env = do
  Queue.withQueue queuePath pcount $ \queue -> do
    createTopics queue
    concurrently_ (connectAll queue) (projectAll queue)
  where
  queuePath = c_dataPath config </> "queues"
  statePath = c_dataPath config </> "state.json"
  sources = Map.elems $ c_sources env
  pcount = Topic.PartitionCount $ c_partitionsPerTopic config

  createTopics queue = do
    info <- Queue.getInfo queue
    forM_ sources $ \source -> do
      let tname = topicName $ s_id source
      if tname `HashMap.member` info
        then logInfo logger $ "loaded topic '" <> unTopicName tname <> "'"
        else do
          _ <- Queue.openTopic queue tname
          logInfo logger $ "created topic for '" <> unTopicName tname <> "'"

  connectAll queue =
    withMany (connect queue) sources $ \svars ->
      every (seconds 30) (save svars) `finally` save svars

  save svars =
    uninterruptibleMask_ $ do
      -- reading is non-blocking so should be fine to run under uninterruptibleMask
      states <- forM svars $ \(sid, svar) -> (sid,) <$> atomically svar
      Aeson.encodeFile statePath $ EmulatorState (Map.fromList states)

  connect queue source f = do
    topic <- Queue.openTopic queue $ topicName $ s_id source
    case s_source source of
      SourcePostgreSQL pconfig ->
        Topic.withProducer topic Postgres.partitioner (Postgres.encoder pconfig) $ \producer ->
        Postgres.withConnector producer pconfig $ \stateVar ->
        f (s_id source, StatePostgres <$> stateVar)

      SourceFile path ->
        Topic.withProducer topic FileConnector.partitioner FileConnector.encoder $ \producer ->
        withAsync (FileConnector.connect logger producer path) $ \_ ->
        f (s_id source, return (StateFile ()))

  projectAll queue = forConcurrently_ (c_destinations env) (project queue)

  project queue dest =
    withDestination dest $ \transport -> do
    sourceTopics <- forM (d_sources dest) $ \sid -> do
      topic <- Queue.openTopic queue (topicName sid)
      return (sid, topic)
    Projector.project logger Projection
        { p_id = projectionId (d_id dest)
        , p_destination = d_id dest
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

topicName :: Id DataSource -> TopicName
topicName sid = TopicName $ "t-" <> unId sid

projectionId :: Id DataDestination -> Id Projection
projectionId (Id dst) = Id ("p-" <> dst)
