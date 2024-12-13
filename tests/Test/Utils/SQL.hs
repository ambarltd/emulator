{-# OPTIONS_GHC -Wno-x-partial #-}
-- | Utilities for testing SQL connectors
module Test.Utils.SQL
  ( Table(..)
  , testGenericSQL
  , mkTableName
  , withConnector
  , group
  , readEntry

  , Event(..)
  , EventsTable(..)

  , TEntry(..)
  , TTable(..)
  ) where

import Control.Concurrent (MVar, newMVar, modifyMVar)
import Control.Concurrent.Async (mapConcurrently_)
import Control.Exception (throwIO, ErrorCall(..))
import Control.Monad (replicateM, forM_)
import qualified Data.Aeson as Aeson
import Data.Aeson (FromJSON, ToJSON)
import qualified Data.ByteString.Lazy as LB
import Data.Default (Default, def)
import Data.List (sort, stripPrefix)
import qualified Data.Map.Strict as Map
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import GHC.Generics (Generic)
import System.IO.Unsafe (unsafePerformIO)
import Test.Hspec (Spec, it, shouldBe, sequential)
import Test.Hspec.Expectations.Contrib (annotate)

import Ambar.Emulator.Connector (partitioner, encoder, Connector(..))
import Ambar.Emulator.Queue.Topic (Topic, PartitionCount(..))
import qualified Ambar.Emulator.Queue.Topic as Topic
import Test.Queue (withFileTopic)
import Test.Utils.OnDemand (OnDemand)
import qualified Test.Utils.OnDemand as OnDemand

import Utils.Async (withAsyncThrow)
import Utils.Delay (deadline, seconds)
import Utils.Logger (plainLogger, Severity(..))

-- An SQL table
class Table a where
  type Config a
  type Entry a
  type Connection a
  withTable :: Config a -> Connection a -> (a -> IO b) -> IO b
  tableCols :: a -> [Text]
  tableName :: a -> String
  -- Mock events to be added to the database.
  -- Each sublist is an infinite list of events for the same aggregate.
  mocks :: a -> [[Entry a]]
  insert :: Connection a -> a -> [Entry a] -> IO ()
  selectAll :: Connection a -> a -> IO [Entry a] -- retrieve all events in the table.

data Event = Event
  { e_id :: Int
  , e_aggregate_id :: Int
  , e_sequence_number :: Int
  }
  deriving (Eq, Show, Generic)

instance ToJSON Event where
  toJSON = Aeson.genericToJSON eventJSONOptions

instance FromJSON Event where
  parseJSON = Aeson.genericParseJSON eventJSONOptions

eventJSONOptions :: Aeson.Options
eventJSONOptions = Aeson.defaultOptions
  { Aeson.fieldLabelModifier = \label ->
    fromMaybe label (stripPrefix "e_" label)
  }

newtype EventsTable a = EventsTable String

testGenericSQL
  :: (Table table, Connector connector, Default (ConnectorState connector))
  => OnDemand db
  -> (forall x. db -> (Connection table -> IO x) -> IO x)
  -> (db -> table -> connector)
  -> Config table
  -> Spec
testGenericSQL od withConnection mkConfig conf = sequential $ do
  -- checks that our tests can connect to postgres
  it "connects" $
    with (PartitionCount 1) $ \conn table _ _ -> do
      insert conn table (take 10 $ head $ mocks table)
      rs <- selectAll conn table
      length rs `shouldBe` 10

  it "retrieves all events already in the db" $
    with (PartitionCount 1) $ \conn table topic connected -> do
      let count = 10
      insert conn table (take count $ head $ mocks table)
      connected $
        deadline (seconds 2) $
        Topic.withConsumer topic group $ \consumer -> do
        es <- replicateM count $ readEntry @Event consumer
        length es `shouldBe` count

  it "can retrieve a large number of events" $
    with (PartitionCount 1) $ \conn table topic connected -> do
      let count = 10_000
      insert conn table (take count $ head $ mocks table)
      connected $ deadline (seconds 2) $
        Topic.withConsumer topic group $ \consumer -> do
        es <- replicateM count $ readEntry @Event consumer
        length es `shouldBe` count

  it "retrieves events added after initial snapshot" $
    with (PartitionCount 1) $ \conn table topic connected -> do
      let count = 10
          write = insert conn table (take count $ head $ mocks table)
      connected $ deadline (seconds 1) $
        Topic.withConsumer topic group $ \consumer ->
        withAsyncThrow write $ do
        es <- replicateM count $ readEntry @Event consumer
        length es `shouldBe` count

  it "maintains ordering through parallel writes" $ do
    let partitions = 5
    with (PartitionCount partitions) $ \conn table topic connected -> do
      let count = 1_000
          write = mapConcurrently_ id
              [ insert conn table (take count $ mocks table !! partition)
              | partition <- [1..partitions] ]
      connected $ deadline (seconds 1) $
        Topic.withConsumer topic group $ \consumer -> do
        -- write and consume concurrently
        withAsyncThrow write $ do
          es <- replicateM (count * partitions) $ readEntry consumer
          let byAggregateId = Map.toList $ Map.fromListWith (flip (++))
                  [ (e_aggregate_id, [e_sequence_number])
                  | (Event{..}, _) <- es
                  ]
          forM_ byAggregateId $ \(a_id, seqs) ->
            annotate ("ordered (" <> show a_id <> ")") $
              sort seqs `shouldBe` seqs
  where
  with = withConnector od withConnection mkConfig conf

-- | A generic consumer group
group :: Topic.ConsumerGroupName
group = Topic.ConsumerGroupName "test_group"

readEntry :: Aeson.FromJSON a => Topic.Consumer -> IO (a, Topic.Meta)
readEntry consumer = do
  result <- Topic.read consumer
  (bs, meta) <- either (throwIO . ErrorCall . show) return result
  case Aeson.eitherDecode $ LB.fromStrict bs of
    Left err -> throwIO $ ErrorCall $ "decoding error: " <> show err
    Right val -> return (val, meta)

-- | Set up a full pipeline.
-- * Create table
-- * Create Topic
-- * Connect connector to db reading from table
withConnector
  :: (Table table, Connector connector, Default (ConnectorState connector))
  => OnDemand db
  -> (forall x. db -> (Connection table -> IO x) -> IO x)
  -> (db -> table -> connector)
  -> Config table
  -> PartitionCount
  -> (Connection table -> table -> Topic -> (IO b -> IO b) -> IO a)
  -> IO a
withConnector od withConnection mkConfig conf partitions f =
  OnDemand.with od $ \db ->                                          -- load db
  withConnection db $ \conn ->
  withTable conf conn $ \table ->                                    -- create events table
  withFileTopic partitions $ \topic ->                               -- create topic
  Topic.withProducer topic partitioner encoder $ \producer -> do     -- create topic producer
  let logger = plainLogger Warn
      config = mkConfig db table
      connected act = connect config logger def producer (const act) -- setup connector
  f conn table topic connected


{-# NOINLINE tableNumber #-}
tableNumber :: MVar Int
tableNumber = unsafePerformIO (newMVar 0)

mkTableName :: IO String
mkTableName = do
  number <- modifyMVar tableNumber $ \n -> return (n + 1, n)
  return $ "table_" <> show number

--------------------

-- | A table for testing type support
data TTable db a = TTable
  { _tt_name :: String
  , _tt_tyName :: String
  }

-- | Event for testing type support
data TEntry a = TEntry
  { te_id :: Int
  , te_aggregate_id :: Int
  , te_sequence_number :: Int
  , te_value :: a
  }
  deriving (Eq, Show, Generic, Functor)

instance FromJSON a => FromJSON (TEntry a) where
  parseJSON = Aeson.genericParseJSON opt
    where
    opt = Aeson.defaultOptions
      { Aeson.fieldLabelModifier = \label ->
        fromMaybe label (stripPrefix "te_" label)
      }
