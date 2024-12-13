{-# OPTIONS_GHC -Wno-orphans #-}
module Test.Connector.File
  ( testFileConnector
  ) where

import Control.Concurrent (MVar, newMVar, modifyMVar)
import Data.ByteString.Lazy.Char8 as Char8
import qualified Data.Aeson as Json
import Data.Default (def)
import Control.Monad (forM_, forM)
import System.IO (withFile, IOMode(..), hClose)
import System.IO.Temp (withSystemTempFile)
import Test.Hspec (Spec, describe)

import qualified Ambar.Emulator.Queue.Topic as Topic
import Ambar.Emulator.Queue.Topic (Topic, PartitionCount(..))
import Ambar.Emulator.Connector (partitioner, encoder, connect)
import Ambar.Emulator.Connector.File (FileConnector(..))


import Test.Queue (withFileTopic)
import Utils.Logger (plainLogger, Severity(..))
import Test.Utils.SQL

testFileConnector :: Spec
testFileConnector =
  describe "FileConnector" $ do
    testGenericSQL with_
  where
  with_
    :: PartitionCount
    -> (  FileConnection
       -> EventsTable FileConnector
       -> Topic
       -> (IO b -> IO b)
       -> IO a
       )
    -> IO a
  with_ partitions f =
    withSystemTempFile "file-db" $ \path h -> do
    hClose h
    var <- newMVar 0
    let connector = FileConnector path "aggregate_id" "sequence_number"
        conn = FileConnection connector var
    withFileTopic partitions $ \topic ->                                    -- create topic
      Topic.withProducer topic partitioner encoder $ \producer ->           -- create topic producer
      withTable () conn $ \table -> do
      let logger = plainLogger Warn
          connected :: forall a. IO a -> IO a
          connected act = connect connector logger def producer (const act) -- setup connector
      f conn table topic connected

data FileConnection = FileConnection
  { _connector :: FileConnector
  , _maxId :: MVar Int
  }

instance Table (EventsTable FileConnector) where
  type Entry (EventsTable FileConnector) = Event
  type Config (EventsTable FileConnector) = ()
  type Connection (EventsTable FileConnector) = FileConnection
  tableName (EventsTable name) = name
  tableCols _ = ["id", "aggregate_id", "sequence_number"]
  withTable () (FileConnection connector _) f = do
    name <- mkTableName
    let table = EventsTable name
    withFile (c_path connector) ReadMode $ \_ -> f table
  mocks _ =
    [ [ Event err agg_id seq_id | seq_id <- [0..] ]
      | agg_id <- [0..]
    ]
    where err = error "aggregate id is determined by mysql"
  insert (FileConnection connector varMaxId) _ events =
    forM_ events $ \(Event _ agg_id seq_id) -> do
      modifyMVar varMaxId $ \nxt -> do
        let encoded = Json.encode $ Event nxt agg_id seq_id
        Char8.appendFile (c_path connector) (encoded <> "\n")
        return (nxt + 1, nxt)

  selectAll (FileConnection connector _) _ = do
    bs <- Char8.readFile (c_path connector)
    forM (Char8.lines bs) $ \line ->
      case Json.eitherDecode' line of
        Left err -> error $ "unable to decode file entry: " <> err
        Right v -> return v




