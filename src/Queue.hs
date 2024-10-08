module Queue where

import Control.Monad (forM)
import Control.Concurrent (MVar, modifyMVar)
import Data.Bifunctor (second)
import Data.ByteString (ByteString)
import Data.Hashable (Hashable(..))
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.Text (Text)

import Queue.Partition (Partition, Offset, Record(..))
import qualified Queue.Partition as P

newtype Topic = Topic { unTopic :: Text }
  deriving newtype (Eq, Ord, Hashable)

newtype PartitionNumber = PartitionNumber { unPartitionNumber :: Int }
  deriving newtype (Eq, Ord, Hashable)

data Meta = Meta
  { meta_topic :: Topic
  , meta_partition :: PartitionNumber
  , meta_offset :: Offset
  }

data PartitionInstance =
  forall a. Partition a => PartitionInstance a

newtype Inventory = Inventory
  (HashMap Topic (HashMap PartitionNumber (Maybe PartitionInstance)))

data Consumer = Consumer

data Producer a = Producer
  { p_topic :: Topic
  , p_partitions :: HashMap PartitionNumber PartitionInstance
  , p_select :: a -> PartitionNumber
  , p_encode :: a -> Record
  }

data Queue = Queue
  { q_openPartition :: Topic -> PartitionNumber -> IO PartitionInstance
  , q_partitionsPerTopic :: Int
  , q_inventory :: MVar Inventory
  }

withProducer
  :: Hashable b
  => Queue
  -> Topic
  -> (a -> b)              -- ^ partitioner
  -> (a -> ByteString)     -- ^ encoder
  -> (Producer a -> IO b)
  -> IO b
withProducer Queue{..} topic partitioner encode act = do
  partitions <- modifyMVar q_inventory $ \(Inventory inventory) -> do
    partitions <- case HashMap.lookup topic inventory of
      Nothing ->
        forM [0 .. q_partitionsPerTopic - 1] $ \n ->
          (PartitionNumber n,) <$> q_openPartition topic (PartitionNumber n)
      Just ps ->
        forM (HashMap.toList ps) $ \(pid, mp) ->
          (pid,) <$> maybe (q_openPartition topic pid) return mp
    return
      ( Inventory
        $ HashMap.insert topic
        (HashMap.fromList $ fmap (second Just) partitions)
        inventory
      , partitions
      )
  act $ Producer
    { p_topic = topic
    , p_partitions = HashMap.fromList partitions
    , p_select = PartitionNumber . (`mod` q_partitionsPerTopic) . hash . partitioner
    , p_encode = Record . encode
    }

write :: Producer a -> a -> IO ()
write Producer{..} msg = do
  PartitionInstance partition <- maybe err return (HashMap.lookup pid p_partitions)
  P.write partition (p_encode msg)
  where
  pid = p_select msg
  err = error $ unwords
    [ "Queue: unknown partition"
    <> show (unPartitionNumber pid)
    <> "on topic"
    <> show (unTopic p_topic)
    ]

withConsumer :: Queue -> (Consumer -> IO b) -> IO b
withConsumer = undefined

-- | blocks until there is a message.
read :: Consumer -> IO (ByteString, Meta)
read  = undefined

commit :: Consumer -> Meta -> IO ()
commit  = undefined
