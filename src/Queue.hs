module Queue where

import Control.Monad (forM)
import Control.Concurrent (MVar, modifyMVar)
import Data.Bifunctor (second)
import Data.ByteString (ByteString)
import Data.Hashable (Hashable)
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.Text (Text)

import Queue.Partition (Offset)
import qualified Queue.Partition as P

newtype Topic = Topic Text
  deriving newtype (Eq, Ord, Hashable)

newtype Partition = Partition Int
  deriving newtype (Eq, Ord, Hashable)

data Meta = Meta
  { meta_topic :: Topic
  , meta_partition :: Partition
  , meta_offset :: Offset
  }

data PartitionInstance =
  forall a. P.Partition a => PartitionInstance a

newtype Inventory = Inventory
  (HashMap Topic (HashMap Partition (Maybe PartitionInstance)))

data Consumer = Consumer

newtype Producer = Producer (HashMap Partition PartitionInstance)

data Queue = Queue
  { q_openPartition :: Topic -> Partition -> IO PartitionInstance
  , q_partitionsPerTopic :: Int
  , q_inventory :: MVar Inventory
  }

withProducer :: Queue -> Topic -> (Producer -> IO b) -> IO b
withProducer Queue{..} topic act = do
  partitions <- modifyMVar q_inventory $ \(Inventory inventory) -> do
    partitions <- case HashMap.lookup topic inventory of
      Nothing ->
        forM [0 .. q_partitionsPerTopic - 1] $ \n ->
          (Partition n,) <$> q_openPartition topic (Partition n)
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
  act (Producer $ HashMap.fromList partitions)

write :: Producer -> ByteString -> IO ()
write = undefined

withConsumer :: Queue -> (Consumer -> IO b) -> IO b
withConsumer  = undefined

-- | blocks until there is a message.
read :: Consumer -> IO (ByteString, Meta)
read  = undefined

commit :: Consumer -> Meta -> IO ()
commit  = undefined
