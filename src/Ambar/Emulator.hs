module Ambar.Emulator where

import Control.Concurrent.Async (concurrently_, forConcurrently_)
import Control.Monad (forM_, void)
import qualified Data.Map as Map
import qualified Data.HashMap.Strict as HashMap

import qualified Ambar.Emulator.Connector.Postgres as Postgres
import qualified Ambar.Emulator.Connector.File as FileConnector

import qualified Ambar.Emulator.Queue.Topic as Topic
import Ambar.Emulator.Queue (TopicName(..), PartitionCount(..))
import qualified Ambar.Emulator.Queue as Queue
import Ambar.Emulator.Config
  ( EmulatorConfig(..)
  , EnvironmentConfig(..)
  , Id(..)
  , DataSource(..)
  , Source(..)
  )
import Utils.Logger (SimpleLogger, logInfo)

emulate :: SimpleLogger -> EmulatorConfig -> EnvironmentConfig -> IO ()
emulate logger config env = do
  Queue.withQueue qpath pcount $ \queue -> do
    createTopics queue
    concurrently_ (connectAll queue) (projectAll queue)
  where
  qpath = c_queuePath config
  pcount = PartitionCount $ c_partitionsPerTopic config

  createTopics queue = do
    info <- Queue.getInfo queue
    forM_ (Map.elems $ c_sources env) $ \source -> do
      let sid = unId (s_id source)
          name = topicName source
      if name `HashMap.member` info
        then logInfo logger $ "loaded topic for '" <> sid <> "'"
        else do
          _ <- Queue.openTopic queue (topicName source)
          logInfo logger $ "created topic for '" <> sid <> "'"

  connectAll queue = forConcurrently_ (c_sources env) (connect queue)

  connect queue source = do
    topic <- Queue.openTopic queue (topicName source)
    case s_source source of
      SourcePostgreSQL pconfig ->
        Topic.withProducer topic Postgres.partitioner (Postgres.encoder pconfig) $ \producer ->
        void $ Postgres.connect producer pconfig

      SourceFile path ->
        Topic.withProducer topic FileConnector.partitioner FileConnector.encoder $ \producer ->
        FileConnector.connect producer path

  projectAll _ = return ()


topicName :: DataSource -> TopicName
topicName source = TopicName $ "t-" <> unId (s_id source)

