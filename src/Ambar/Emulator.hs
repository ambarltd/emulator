module Ambar.Emulator where

import Control.Concurrent.Async (concurrently_, forConcurrently_)
import Control.Monad (forM_, void, forM)
import qualified Data.Map as Map
import qualified Data.HashMap.Strict as HashMap

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

emulate :: SimpleLogger -> EmulatorConfig -> EnvironmentConfig -> IO ()
emulate logger config env = do
  Queue.withQueue qpath pcount $ \queue -> do
    createTopics queue
    concurrently_ (connectAll queue) (projectAll queue)
  where
  qpath = c_queuePath config
  pcount = Topic.PartitionCount $ c_partitionsPerTopic config

  createTopics queue = do
    info <- Queue.getInfo queue
    forM_ (Map.elems $ c_sources env) $ \source -> do
      let tname = topicName $ s_id source
      if tname `HashMap.member` info
        then logInfo logger $ "loaded topic '" <> unTopicName tname <> "'"
        else do
          _ <- Queue.openTopic queue tname
          logInfo logger $ "created topic for '" <> unTopicName tname <> "'"

  connectAll queue = forConcurrently_ (c_sources env) (connect queue)

  connect queue source = do
    topic <- Queue.openTopic queue $ topicName $ s_id source
    case s_source source of
      SourcePostgreSQL pconfig ->
        Topic.withProducer topic Postgres.partitioner (Postgres.encoder pconfig) $ \producer ->
        void $ Postgres.connect producer pconfig

      SourceFile path ->
        Topic.withProducer topic FileConnector.partitioner FileConnector.encoder $ \producer ->
        FileConnector.connect producer path

  projectAll queue = forConcurrently_ (c_destinations env) (project queue)

  project queue dest =
    withDestination dest $ \transport -> do
    sources <- forM (d_sources dest) $ \sid -> do
      topic <- Queue.openTopic queue (topicName sid)
      return (sid, topic)
    Projector.project logger Projection
        { p_id = projectionId (d_id dest)
        , p_destination = d_id dest
        , p_sources = sources
        , p_transport = transport
        }

  withDestination dest act =
    case d_destination dest of
      DestinationFile path ->
        FileTransport.withFileTransport path (act . Some)
      DestinationHttp{..} -> do
        transport <-  HttpTransport.new d_endpoint d_username d_password
        act (Some transport)

topicName :: Id DataSource -> TopicName
topicName sid = TopicName $ "t-" <> unId sid

projectionId :: Id DataDestination -> Id Projection
projectionId (Id dst) = Id ("p-" <> dst)
