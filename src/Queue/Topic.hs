 module Queue.Topic
    ( Topic
    , ConsumerGroupName
    , Consumer
    , Producer
    , Meta
    , withProducer
    , write
    , withConsumer
    , read
    , commit
    ) where

import Prelude hiding (read)

import Control.Concurrent.Async (mapConcurrently)
import Control.Concurrent.STM
  ( STM
  , TVar
  , atomically
  , readTVar
  , writeTVar
  , newTVarIO
  , newTVar
  )
import Control.Exception (bracket)
import Control.Monad (when, forM_)
import Data.ByteString (ByteString)
import Data.List.Extra (chunksOf)
import Data.Hashable (Hashable(..))
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as Text
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Time.Clock (nominalDiffTimeToSeconds)
import System.Random (randomIO)

import Queue.Partition
  ( Partition
  , Offset
  , Record(..)
  , Reader
  , openReader
  , closeReader
  )
import qualified Queue.Partition as P

data Topic = Topic
  { t_name :: Text
  , t_partitions :: HashMap PartitionNumber PartitionInstance
  , t_cgroups :: TVar (HashMap ConsumerGroupName ConsumerGroup)
  }

newtype PartitionNumber = PartitionNumber { unPartitionNumber :: Int }
  deriving newtype (Eq, Ord, Hashable)

data PartitionInstance =
  forall a. Partition a => PartitionInstance a

data ReaderInstance =
  forall a. Partition a => ReaderInstance
    { r_reader :: Reader a
    , r_partition :: PartitionNumber
    , r_comitted :: TVar Offset
    }

newtype ConsumerGroupName = ConsumerGroupName Text
  deriving newtype (Eq, Ord, Hashable)

data ConsumerGroup = ConsumerGroup
  { g_readers :: Maybe [ReaderInstance]
  , g_offsets :: HashMap PartitionNumber (TVar Offset)
  , g_consumers :: HashMap ConsumerId Consumer
  }

newtype ConsumerId = ConsumerId UUID
  deriving newtype (Eq, Ord, Hashable)

newtype UUID = UUID Text
  deriving newtype (Eq, Ord, Hashable)

newUUID :: IO UUID
newUUID = do
  now <- nominalDiffTimeToSeconds <$> getPOSIXTime
  fixed <- randomIO @Int
  return $ UUID $ Text.pack $ show now <> show fixed

-- | A consumer holds any number of reader instances.
-- The particular instances and their number is adjusted through
-- rebalances at consumer creation and descruction.
-- Contains an infinite cycling list of reader instances for round-robin picking
data Consumer = Consumer Topic (TVar [ReaderInstance])

data Producer a = Producer
  { p_topic :: Topic
  , p_select :: a -> PartitionNumber
  , p_encode :: a -> Record
  }

withProducer
  :: Hashable b
  => Topic
  -> (a -> b)              -- ^ partitioner
  -> (a -> ByteString)     -- ^ encoder
  -> (Producer a -> IO b)
  -> IO b
withProducer topic partitioner encode act =
  act $ Producer
    { p_topic = topic
    , p_select = PartitionNumber . (`mod` partitionCount) . hash . partitioner
    , p_encode = Record . encode
    }
  where
  partitionCount = HashMap.size (t_partitions topic)

write :: Producer a -> a -> IO ()
write Producer{..} msg = do
  PartitionInstance partition <- maybe err return (HashMap.lookup pid partitions)
  P.write partition (p_encode msg)
  where
  partitions = t_partitions p_topic
  pid = p_select msg
  err = error $ unwords
    [ "Queue: unknown partition"
    <> show (unPartitionNumber pid)
    <> "on topic"
    <> show (t_name p_topic)
    ]

withConsumer :: Topic -> ConsumerGroupName -> (Consumer -> IO b) -> IO b
withConsumer topic@Topic{..} gname act = do
  group <- atomically $ do
    groups <- readTVar t_cgroups
    group <- case HashMap.lookup gname groups of
      Just g -> return g
      Nothing -> return ConsumerGroup
        { g_readers = Just []
        , g_offsets = mempty
        , g_consumers = mempty
        }
    writeTVar t_cgroups $ HashMap.insert gname group groups
    return group
  let isNewGroup = HashMap.null (g_offsets group)
  when isNewGroup initialise
  cid <- ConsumerId <$> newUUID
  bracket (rebalanceAdd cid) rebalanceRemove act
  where
    initialise = do
      readers <- mapConcurrently mkReader $ HashMap.toList t_partitions
      let group = ConsumerGroup
            { g_readers = Just readers
            , g_offsets = HashMap.fromList
                  [ (r_partition, r_comitted)
                  | ReaderInstance{..} <- readers
                  ]
            , g_consumers = mempty
            }
      atomically $ do
        groups <- readTVar t_cgroups
        writeTVar t_cgroups $ HashMap.insert gname group groups

    mkReader :: (PartitionNumber, PartitionInstance) -> IO ReaderInstance
    mkReader (number, PartitionInstance partition) = do
      reader <- openReader partition
      var <- newTVarIO 0
      return ReaderInstance
        { r_reader = reader
        , r_partition = number
        , r_comitted = var
        }

    rebalance :: ConsumerGroupName -> STM ()
    rebalance name = do
      groups <- readTVar t_cgroups
      let group = groups HashMap.! name
          cids = HashMap.keys (g_consumers group)
          readerCount = maybe 0 length (g_readers group)
          consumerCount = length cids
          chunkSize = ceiling @Double $ fromIntegral readerCount / fromIntegral consumerCount
          readerLists = chunksOf chunkSize (fromMaybe [] $ g_readers group) ++ repeat []

      -- update consumers reader lists
      -- TODO: we should reset the ones that moved owners to their comitted offset.
      forM_ (zip cids readerLists) $ \(c, readers) ->
        case HashMap.lookup c (g_consumers group) of
          Nothing -> error "rebalancing: missing consumer id"
          Just (Consumer _ rvar) -> writeTVar rvar readers

    rebalanceAdd :: ConsumerId -> IO Consumer
    rebalanceAdd cid = atomically $ do
      newConsumer <- addConsumer
      rebalance gname
      return newConsumer
      where
      addConsumer = do
        groups <- readTVar t_cgroups
        newConsumer <- do
          rvar <- newTVar []
          return $ Consumer topic rvar

        let group = groups HashMap.! gname
            groupUpdated = ConsumerGroup
              { g_readers = g_readers group
              , g_offsets = g_offsets group
              , g_consumers = HashMap.insert cid newConsumer (g_consumers group)
              }

        -- add new consumer to group
        writeTVar t_cgroups $ HashMap.insert gname groupUpdated groups
        return newConsumer


    rebalanceRemove :: Consumer -> IO ()
    rebalanceRemove (Consumer _ _) = undefined

data Meta = Meta Offset PartitionNumber

-- | blocks until there is a message.
read :: Consumer -> IO (ByteString, Meta)
read  = undefined

commit :: Consumer -> Meta -> IO ()
commit  = undefined
