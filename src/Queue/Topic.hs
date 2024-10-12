 module Queue.Topic
    ( Topic
    , TopicName(..)
    , PartitionInstance(..)
    , withTopic
    , getState

    , Consumer
    , ConsumerGroupName(..)
    , Meta(..)
    , withConsumer
    , read
    , commit

    , Producer
    , withProducer
    , write
    ) where

import Prelude hiding (read)

import Control.Concurrent.Async (forConcurrently_ , forConcurrently)
import qualified Control.Concurrent.STM as STM
import Control.Concurrent.STM (STM , TVar , atomically)
import Control.Exception (bracket)
import Control.Monad (forM_, forM, when)
import Data.ByteString (ByteString)
import Data.Foldable (sequenceA_)
import Data.Hashable (Hashable(..))
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.List (find)
import Data.List.Extra (chunksOf)
import Data.Maybe (fromMaybe, isNothing)
import Data.Text (Text)
import qualified Data.Text as Text
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Time.Clock (nominalDiffTimeToSeconds)
import Data.Traversable (mapAccumR)
import System.Random (randomIO)

import Queue.Partition
  ( Partition
  , Offset
  , Record(..)
  )
import qualified Queue.Partition as Partition
import qualified Queue.Partition.STMReader as R

-- | Abstraction for a group of independent streams (partitions)
data Topic = Topic
  { t_name :: TopicName
  , t_partitions :: HashMap PartitionNumber PartitionInstance
  , t_cgroups :: TVar (HashMap ConsumerGroupName ConsumerGroup)
  }

newtype TopicName = TopicName { unTopicName :: Text }
  deriving newtype (Eq, Ord)

newtype PartitionNumber = PartitionNumber { unPartitionNumber :: Int }
  deriving Show
  deriving newtype (Eq, Ord, Enum, Integral, Real, Num, Hashable)

data PartitionInstance =
  forall a. Partition a => PartitionInstance a

newtype ConsumerGroupName = ConsumerGroupName Text
  deriving newtype (Eq, Ord, Hashable)

data PartitionReader = PartitionReader
  { r_reader :: R.STMReader
  , r_partition :: PartitionNumber
  , r_committed :: TVar Offset
  }

-- | A group of ordered readers of independent streams (partitions).
-- Consumers always stream data from a disjoint sets of partitions.
data ConsumerGroup = ConsumerGroup
  { g_readers :: Maybe [PartitionReader]
  , g_comitted :: HashMap PartitionNumber (TVar Offset)
  -- ^ comitted offsets. The TVar is shared with the STMReader
  , g_consumers :: HashMap ConsumerId Consumer
  }

newtype ConsumerId = ConsumerId UUID
  deriving Show
  deriving newtype (Eq, Ord, Hashable)

-- | A consumer holds any number of reader instances.
-- The particular instances and their number is adjusted through
-- rebalances at consumer creation and descruction.
-- Contains an infinite cycling list of reader instances for round-robin picking
data Consumer = Consumer Topic (TVar [PartitionReader])

newtype UUID = UUID Text
  deriving Show
  deriving newtype (Eq, Ord, Hashable)

data Producer a = Producer
  { p_topic :: Topic
  , p_select :: a -> PartitionNumber
  , p_encode :: a -> Record
  }

type TopicState = HashMap ConsumerGroupName (HashMap PartitionNumber Offset)

withTopic
  :: TopicName
  -> HashMap PartitionNumber PartitionInstance
  -> TopicState
  -> (Topic -> IO a)
  -> IO a
withTopic name partitions groupOffsets act =
  bracket create delete act
  where
  create = do
    cgroupsVar <- atomically $ do
      cgroups <- forM groupOffsets $ \partitionOffsets -> do
        offsets <- forM partitionOffsets $ \offset ->
          STM.newTVar offset
        return ConsumerGroup
          { g_readers = Nothing
          , g_comitted = offsets
          , g_consumers = mempty
          }
      STM.newTVar cgroups
    return Topic
      { t_name = name
      , t_partitions = partitions
      , t_cgroups = cgroupsVar
      }

  delete topic = do
    readers <- atomically $ do
      groups <- STM.readTVar (t_cgroups topic)

      -- empty all groups
      STM.writeTVar (t_cgroups topic) $ flip fmap groups $ \group ->
        group { g_consumers = mempty }

      -- close all consumers
      sequenceA_
        [ closeConsumer consumer
        | group <- HashMap.elems groups
        , consumer <- HashMap.elems (g_consumers group)
        ]

      -- collect all readers to close
      return
        [ reader
        | group <- HashMap.elems groups
        , reader <- fromMaybe [] (g_readers group)
        ]

    forConcurrently_ readers $ R.destroy . r_reader

-- | Get the state of the latest committed offsets of the topic.
getState :: Topic -> STM TopicState
getState Topic{..} = do
  cgroups <- STM.readTVar t_cgroups
  forM cgroups $ \group -> forM (g_comitted group) STM.readTVar

withProducer
  :: Hashable b
  => Topic
  -> (a -> b)              -- ^ partitioner
  -> (a -> ByteString)     -- ^ encoder
  -> (Producer a -> IO c)
  -> IO c
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
  Partition.write partition (p_encode msg)
  where
  partitions = t_partitions p_topic
  pid = p_select msg
  err = error $ unwords
    [ "Queue: unknown partition"
    <> show (unPartitionNumber pid)
    <> "on topic"
    <> Text.unpack (unTopicName $ t_name p_topic)
    ]

withConsumer :: Topic -> ConsumerGroupName -> (Consumer -> IO b) -> IO b
withConsumer topic@Topic{..} gname act = do
  group <- atomically (retrieve gname)
  when (isNothing $ g_readers group) $ openReaders group
  cid <- ConsumerId <$> newUUID
  bracket (add cid) (remove cid) act
  where
    -- retrieve an existing group or create a new one
    retrieve :: ConsumerGroupName -> STM ConsumerGroup
    retrieve name = do
      groups <- STM.readTVar t_cgroups
      group <- case HashMap.lookup name groups of
        Just g -> return g
        Nothing -> do
          offsets <- traverse (const $ STM.newTVar 0) t_partitions
          return ConsumerGroup
            { g_readers = Nothing
            , g_comitted = offsets
            , g_consumers = mempty
            }
      STM.writeTVar t_cgroups $ HashMap.insert name group groups
      return group

    -- open an existing group's readers
    openReaders :: ConsumerGroup -> IO ()
    openReaders group = do
      readers <- forConcurrently (HashMap.toList $ g_comitted group) $ \(pnumber, offsetVar) -> do
        let p = t_partitions HashMap.! pnumber
        case p of
          PartitionInstance partition -> do
            start <- STM.readTVarIO offsetVar
            r <- R.new partition start
            return $ PartitionReader r pnumber offsetVar

      atomically $ do
        groups <- STM.readTVar t_cgroups
        let g = groups HashMap.! gname
            g' = g { g_readers = Just readers }
        STM.writeTVar t_cgroups $ HashMap.insert gname g' groups

    add :: ConsumerId -> IO Consumer
    add cid = atomically $ do
      new <- addConsumer
      _ <- rebalance gname
      return new
      where
      addConsumer = do
        groups <- STM.readTVar t_cgroups
        newConsumer <- do
          rvar <- STM.newTVar []
          return $ Consumer topic rvar

        let group = groups HashMap.! gname
            group' = group { g_consumers = HashMap.insert cid newConsumer (g_consumers group) }

        -- add new consumer to group
        STM.writeTVar t_cgroups $ HashMap.insert gname group' groups
        return newConsumer

    remove :: ConsumerId -> Consumer -> IO ()
    remove cid consumer = do
      toClose <- atomically $ do
        removeConsumer
        rebalance gname

      forConcurrently_ toClose $ R.destroy . r_reader
      where
      removeConsumer = do
        closeConsumer consumer
        STM.modifyTVar t_cgroups $ \groups ->
          let group = groups HashMap.! gname
              remaining = HashMap.delete cid (g_consumers group)
              group' = group { g_consumers = remaining }
          in
          HashMap.insert gname group' groups

    -- re-distribute readers across all existing consumers
    -- resets moved readers to the latest checkpoint
    rebalance :: ConsumerGroupName -> STM [PartitionReader]
    rebalance name = do
      groups <- STM.readTVar t_cgroups
      let group = groups HashMap.! name
          cids = HashMap.keys (g_consumers group)
          readerCount = maybe 0 length (g_readers group)
          consumerCount = length cids
          chunkSize = ceiling @Double $ fromIntegral readerCount / fromIntegral consumerCount
          readerLists = chunksOf chunkSize (fromMaybe [] $ g_readers group) ++ repeat []

      before <- assignments group

      -- update consumers' readers lists
      forM_ (zip cids readerLists) $ \(c, readers) ->
        case HashMap.lookup c (g_consumers group) of
          Nothing -> error "rebalancing: missing consumer id"
          Just (Consumer _ rvar) -> STM.writeTVar rvar (cycle readers)

      after <- assignments group

      -- readers assigned to different customers should be reset to
      -- their last comitted offset.
      let changed = HashMap.elems $ after `HashMap.difference` before
      toReset <- forM changed $ \(PartitionReader reader _ committedVar) -> do
        offset <- STM.readTVar committedVar
        return (offset, reader)
      forM_ toReset $ \(offset, rinstance) ->
        R.seek rinstance offset

      let (toClose, groups') = extractClosableReaders groups
      -- remove readers from groups without a consumer
      STM.writeTVar t_cgroups groups'
      -- return readers from groups without a consumer
      return toClose
      where
        extractClosableReaders groups = mapAccumR f [] groups
          where
          f acc g =
            let readers = fromMaybe [] (g_readers g) in
            if HashMap.null (g_consumers g)
               then (readers ++ acc, g { g_readers = Nothing })
               else (acc, g)

        -- get readers assigned to each consumer id.
        assignments
          :: ConsumerGroup
          -> STM (HashMap (ConsumerId, PartitionNumber) PartitionReader)
        assignments g = do
          xss <- forM (HashMap.toList (g_consumers g)) $ \(cid, Consumer _ rvar) -> do
            readers <- STM.readTVar rvar
            let deduped = case readers of
                  [] -> []
                  x:xs -> x : takeWhile (\y -> r_partition y /= r_partition x) xs
            return [ ((cid, r_partition r), r) | r <- deduped ]
          return $ HashMap.fromList (concat xss)

closeConsumer :: Consumer -> STM ()
closeConsumer (Consumer _ var) =  STM.writeTVar var []

newUUID :: IO UUID
newUUID = do
  now <- nominalDiffTimeToSeconds <$> getPOSIXTime
  fixed <- randomIO @Int
  return $ UUID $ Text.pack $ show now <> show fixed

data Meta = Meta TopicName PartitionNumber Offset

-- | Try to read all readers assigned to the consumer in round-robin fashion.
-- Blocks until there is a message.
read :: Consumer -> IO (ByteString, Meta)
read (Consumer topic var) = atomically $ do
  readers <- STM.readTVar var
  go [] readers
  where
  go _ [] = STM.retry
  go seen (PartitionReader reader pnumber _: rs) = do
    when (pnumber `elem` seen) STM.retry
    mval <- R.tryRead reader
    case mval of
      Nothing -> go (pnumber : seen) rs
      Just (offset, Record bs) -> do
        STM.writeTVar var rs
        return (bs, Meta (t_name topic) pnumber offset)

-- | If the partition was moved to a different consumer
-- the commit will fail silently.
commit :: Consumer -> Meta -> IO ()
commit (Consumer _ var) (Meta _ pnumber offset) = atomically $ do
  readers <- STM.readTVar var
  let mreader = find (\r -> r_partition r == pnumber) readers
  forM_ mreader $ \r -> STM.writeTVar (r_committed r) offset
