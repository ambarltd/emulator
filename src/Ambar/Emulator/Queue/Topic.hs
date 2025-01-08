module Ambar.Emulator.Queue.Topic
  ( Topic
  , TopicState(..)
  , PartitionNumber(..)
  , PartitionCount(..)
  , withTopic
  , openTopic
  , closeTopic
  , getState
  , partitionCount

  , Consumer
  , ConsumerGroupName(..)
  , Meta(..)
  , ReadError(..)
  , withConsumer
  , withConsumers
  , read
  , commit

  , Producer
  , Encoder
  , Partitioner
  , withProducer
  , hashPartitioner
  , modPartitioner
  , write
  ) where

import Prelude hiding (read)

import Control.Concurrent.Async (forConcurrently_ , forConcurrently)
import qualified Control.Concurrent.STM as STM
import Control.Concurrent.STM (STM, TVar)
import Control.Exception (bracket, handle, BlockedIndefinitelyOnSTM)
import Control.Monad (forM_, forM, unless, replicateM)
import Data.Aeson (FromJSON, ToJSON, FromJSONKey, ToJSONKey)
import Data.ByteString (ByteString)
import Data.Foldable (sequenceA_)
import Data.Functor.Contravariant (Contravariant(..))
import Data.Hashable (Hashable(..))
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.List (find)
import Data.List.Extra (chunksOf)
import Data.Maybe (fromMaybe)
import qualified Data.Set as Set
import Data.Set (Set)
import Data.Text (Text)
import qualified Data.Text as Text
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Time.Clock (nominalDiffTimeToSeconds)
import GHC.Generics (Generic)
import GHC.Stack (HasCallStack)
import System.Random (randomIO)

import Util.Some (Some(..))
import Util.STM (atomicallyNamed)
import Util.Warden (Warden)

import Ambar.Emulator.Queue.Partition
  ( Partition
  , Offset
  , Record(..)
  )
import qualified Ambar.Emulator.Queue.Partition as Partition
import qualified Ambar.Emulator.Queue.Partition.STMReader as R

-- | Abstraction for a group of independent streams (partitions)
--
-- * One Topic is composed of a number of Partitions.
-- * One Topic can have any number of Producers.
-- * One Producer can produce to any Partition, as determined by the Producer's
--    partitioning function
-- * One Topic can have any number of Consumer Groups.
-- * One Consumer Group can have any number of Consumers.
-- * One Consumer reads messages from one or more Partitions in order.
-- * A Consumer will return the first available Message from its
--    Partitions in round-robin manner.
-- * A Message is first read and later its offset can be committed.
-- * Adding and removing Consumers causes a rebalance.
-- * Partitions that change consumers during a rebalance are rewinded to the
--    latest committed Offset.
data Topic = Topic
  { t_warden :: Warden
  , t_partitions :: HashMap PartitionNumber (Some Partition)
  , t_cgroups :: TVar (HashMap ConsumerGroupName ConsumerGroup)
  }

newtype PartitionNumber = PartitionNumber { unPartitionNumber :: Int }
  deriving Show
  deriving newtype (Eq, Ord, Enum, Integral, Real, Num, Hashable, FromJSONKey, ToJSONKey, FromJSON, ToJSON)

newtype ConsumerGroupName = ConsumerGroupName Text
  deriving Show
  deriving newtype (Eq, Ord, Hashable, FromJSONKey, ToJSONKey)

data PartitionReader = PartitionReader
  { r_reader :: R.STMReader
  , r_partition :: PartitionNumber
  , r_committed :: TVar Offset
  }

data GroupState
  = Initialising
  | Ready [PartitionReader]
  | Closed

-- | A group of ordered readers of independent streams (partitions).
-- Consumers always stream data from a disjoint sets of partitions.
data ConsumerGroup = ConsumerGroup
  { g_state :: GroupState
  , g_comitted :: HashMap PartitionNumber (TVar Offset)
  -- ^ comitted offsets. The TVar is shared with the STMReader
  , g_consumers :: HashMap ConsumerId Consumer
  }

newtype ConsumerId = ConsumerId UUID
  deriving Show
  deriving newtype (Eq, Ord, Hashable)

-- | A consumer holds any number of reader instances.
-- The particular instances and their number is adjusted through
-- rebalances at consumer creation and destruction.
-- A value of Nothing means that the consumer is closed.
data Consumer = Consumer ConsumerId Topic (TVar (Maybe [PartitionReader]))

newtype UUID = UUID Text
  deriving Show
  deriving newtype (Eq, Ord, Hashable)

data Producer a = Producer
  { p_topic :: Topic
  , p_select :: a -> PartitionNumber
  , p_encode :: a -> Record
  }

instance Contravariant Producer where
  contramap f (Producer t s e) = Producer t (s . f) (e . f)

data TopicState = TopicState
  { s_partitions :: Set PartitionNumber
  , s_consumers :: HashMap ConsumerGroupName (HashMap PartitionNumber Offset)
  }
  deriving (Generic, Eq, Show)
  deriving anyclass (FromJSON, ToJSON)

withTopic
  :: HasCallStack
  => Warden
  -> HashMap PartitionNumber (Some Partition)
  -> HashMap ConsumerGroupName (HashMap PartitionNumber Offset)
  -> (Topic -> IO a)
  -> IO a
withTopic warden partitions groupOffsets =
  bracket (openTopic warden partitions groupOffsets) closeTopic

openTopic
  :: HasCallStack
  => Warden
  -> HashMap PartitionNumber (Some Partition)
  -> HashMap ConsumerGroupName (HashMap PartitionNumber Offset)
  -> IO Topic
openTopic warden partitions groupOffsets =
  atomicallyNamed "topic.openTopic" $ do
  groups <- forM groupOffsets $ \partitionOffsets -> do
    offsets <- forM partitionOffsets STM.newTVar
    return ConsumerGroup
      { g_state = Closed
      , g_comitted = offsets
      , g_consumers = mempty
      }
  var <- STM.newTVar groups
  return Topic
    { t_warden = warden
    , t_partitions = partitions
    , t_cgroups = var
    }

closeTopic :: Topic -> IO ()
closeTopic topic = do
  readers <- atomicallyNamed "topic.closeTopic" $ do
    groups <- STM.readTVar (t_cgroups topic)

    -- close all consumers
    sequenceA_
      [ closeConsumer consumer
      | group <- HashMap.elems groups
      , consumer <- HashMap.elems (g_consumers group)
      ]

    -- close all groups
    let close g = g { g_consumers = mempty, g_state = Closed }
    STM.writeTVar (t_cgroups topic) $ fmap close groups

    -- collect all readers to destroy
    return
      [ reader
      | group <- HashMap.elems groups
      , Ready readers <- [g_state group]
      , reader <- readers
      ]

  forConcurrently_ readers $ R.destroy . r_reader

-- | Get the state of the latest committed offsets of the topic.
getState :: Topic -> IO TopicState
getState Topic{..} = atomicallyNamed "topic.getState" $ do
  cgroups <- STM.readTVar t_cgroups
  consumers <- forM cgroups $ \group -> forM (g_comitted group) STM.readTVar
  return TopicState
    { s_partitions = Set.fromList $ HashMap.keys t_partitions
    , s_consumers = consumers
    }

newtype Partitioner a = Partitioner (PartitionCount -> a -> PartitionNumber)

-- | Choose a partition based on the hash of a partitioning key.
hashPartitioner :: Hashable key => (a -> key) -> Partitioner a
hashPartitioner f = Partitioner $ \(PartitionCount pcount) x ->
  PartitionNumber $ hash (f x) `mod` pcount

-- | Control exactly which partition to use for a message.
modPartitioner :: (a -> Int) -> Partitioner a
modPartitioner f = Partitioner $ \(PartitionCount pcount) x ->
  PartitionNumber $ f x `mod` pcount

type Encoder a = a -> ByteString

withProducer
  :: HasCallStack
  => Topic
  -> Partitioner a
  -> Encoder a
  -> (Producer a -> IO c)
  -> IO c
withProducer topic (Partitioner p) encode act =
  act $ Producer
    { p_topic = topic
    , p_select = p (partitionCount topic)
    , p_encode = Record . encode
    }

write :: HasCallStack => Producer a -> a -> IO ()
write Producer{..} msg = do
  Some partition <- maybe err return (HashMap.lookup pid partitions)
  Partition.write partition (p_encode msg)
  where
  partitions = t_partitions p_topic
  pid = p_select msg
  err = error $ unwords
    [ "Queue: unknown partition"
    <> show (unPartitionNumber pid)
    ]

withConsumer
  :: HasCallStack
  => Topic
  -> ConsumerGroupName
  -> (Consumer -> IO b)
  -> IO b
withConsumer topic name act =
  withConsumers topic name 1 $ \cs ->
    case cs of
      [c] -> act c
      _ -> error $ "Topic.withConsumer: unexpected count: " <> show (length cs)

withConsumers
  :: HasCallStack
  => Topic
  -> ConsumerGroupName
  -> Int               -- ^ number of consumer
  -> ([Consumer] -> IO b)
  -> IO b
withConsumers topic@Topic{..} gname count act = do
  cids <- fmap ConsumerId <$> replicateM count newUUID
  bracket (add gname cids) (remove gname . fst) $ \(consumers, group) -> do
    initialise gname group
    act consumers
  where
  add name cids = atomicallyNamed "topic.withConsumer.add" $ do
    groups <- STM.readTVar t_cgroups
    consumers <- forM cids $ \cid -> do
      rvar <- STM.newTVar (Just [])
      return $ Consumer cid topic rvar

    group <- do
      group <- prepare name groups
      let new = HashMap.fromList (zip cids consumers)
          old = g_consumers group
      return $ group { g_consumers = HashMap.union old new }

    STM.writeTVar t_cgroups $ HashMap.insert gname group groups

    case g_state group of
      Ready _ -> rebalance group
      Closed -> error "closed group"
      Initialising -> return () -- will be rebalanced after initialisation
    return (consumers, group)

  -- retrieve an existing group or create a new one
  prepare
    :: ConsumerGroupName
    -> HashMap ConsumerGroupName ConsumerGroup
    -> STM ConsumerGroup
  prepare name groups =
    case HashMap.lookup name groups of
      Just group -> case g_state group of
        Closed -> return group { g_state = Initialising } -- we will initialise the group
        Initialising -> STM.retry -- someone else is initialising it. Let's wait
        Ready _ -> return group
      Nothing -> do
        -- create a new group
        offsets <- traverse (const $ STM.newTVar 0) t_partitions
        return ConsumerGroup
          { g_state = Initialising
          , g_comitted = offsets
          , g_consumers = mempty
          }

  initialise name group =
    case g_state group of
      Initialising -> do
        readers <- openReaders group
        atomicallyNamed "topic.withConsumer.initialise" $ do
          groups <- STM.readTVar t_cgroups
          let group' = (groups HashMap.! name) { g_state = Ready readers }
          let groups' = HashMap.insert name group' groups
          STM.writeTVar t_cgroups groups'
          rebalance group'
      Closed -> error "closed group"
      Ready _ -> return ()

  -- open an existing group's readers
  openReaders :: ConsumerGroup -> IO [PartitionReader]
  openReaders group =
    forConcurrently (HashMap.toList $ g_comitted group) $ \(pnumber, offsetVar) -> do
      let p = t_partitions HashMap.! pnumber
      case p of
        Some partition -> do
          start <- STM.readTVarIO offsetVar
          r <- R.new t_warden partition start
          return $ PartitionReader r pnumber offsetVar

  remove :: ConsumerGroupName -> [Consumer] -> IO ()
  remove name consumers = do
    toClose <- atomicallyNamed "topic.consumer.remove" $ do
      groups <- STM.readTVar t_cgroups
      assigned <- fmap concat $ forM consumers $ \consumer@(Consumer _ _ var) -> do
        readers <- fromMaybe [] <$> STM.readTVar var
        closeConsumer consumer
        return readers

      let group_before = groups HashMap.! name
          isGroupClosed = case g_state group_before of
            Ready _ -> False
            Initialising -> False
            Closed -> True
          cids = fmap (\(Consumer cid _ _) -> cid) consumers
          remaining = foldr HashMap.delete (g_consumers group_before) cids
          group = group_before
            { g_consumers = remaining
            , g_state =
                if HashMap.null remaining
                then Closed
                else g_state group_before
            }

      unless (HashMap.null remaining || null assigned || isGroupClosed) $
        rebalance group

      STM.writeTVar t_cgroups $ HashMap.insert name group groups
      return $ case g_state group of
        Initialising -> error "unexpected initialising state"
        Ready _ -> []
        Closed -> case g_state group_before of
          Ready rs -> rs
          _ -> []

    forConcurrently_ toClose $ R.destroy . r_reader

  -- re-distribute readers across all existing consumers
  -- resets moved readers to the latest checkpoint
  rebalance :: ConsumerGroup -> STM ()
  rebalance group =
    case g_state group of
      Closed -> error "rebalancing closed consumer"
      Initialising -> error "rebalancing initialising consumer"
      Ready allReaders -> do
        let cids = HashMap.keys (g_consumers group)
            readerCount = length allReaders
            consumerCount = max 1 $ length cids
            chunkSize = ceiling @Double $ fromIntegral readerCount / fromIntegral consumerCount
            chunks = if chunkSize > 0
              then chunksOf chunkSize allReaders
              else []
            readerLists = chunks ++ repeat []

        before <- assignments group

        -- update consumers' readers lists
        forM_ (zip cids readerLists) $ \(c, readers) ->
          case HashMap.lookup c (g_consumers group) of
            Nothing -> error "rebalancing: missing consumer id"
            Just (Consumer _ _ rvar) -> STM.writeTVar rvar $ Just readers

        after <- assignments group

        -- readers assigned to different customers should be reset to
        -- their last comitted offset.
        let changed = HashMap.elems $ after `HashMap.difference` before
        forM_ changed $ \(PartitionReader reader _ committedVar) -> do
          offset <- STM.readTVar committedVar
          R.seek reader offset
    where
      -- get readers assigned to each consumer id.
      assignments
        :: ConsumerGroup
        -> STM (HashMap (ConsumerId, PartitionNumber) PartitionReader)
      assignments g = do
        xss <- forM (HashMap.elems (g_consumers g)) $ \(Consumer cid _ rvar) -> do
          readers <- STM.readTVar rvar
          let deduped = case readers of
                Just [] -> []
                Just (x:xs) -> x : takeWhile (\y -> r_partition y /= r_partition x) xs
                Nothing -> []
          return [ ((cid, r_partition r), r) | r <- deduped ]
        return $ HashMap.fromList (concat xss)

closeConsumer :: HasCallStack => Consumer -> STM ()
closeConsumer (Consumer _ _ var) =  STM.writeTVar var Nothing

newUUID :: IO UUID
newUUID = do
  now <- nominalDiffTimeToSeconds <$> getPOSIXTime
  fixed <- randomIO @Int
  return $ UUID $ Text.pack $ show now <> show fixed

data Meta = Meta PartitionNumber Offset
  deriving (Show, Eq, Ord)

data ReadError
  = ClosedConsumer
  | EndOfPartition -- ^ only thrown if there is no more possibility of
                   -- other threads writing to the Topic.
  deriving (Show, Eq, Ord)

-- | Try to read all readers assigned to the consumer in round-robin fashion.
-- Blocks until there is a message.
read :: HasCallStack => Consumer -> IO (Either ReadError (ByteString, Meta))
read (Consumer _ _ var) =
  handle whenBlocked $
  STM.atomically $ do
    mreaders <- STM.readTVar var
    case mreaders of
      Nothing -> return $ Left ClosedConsumer
      Just rs -> go $ zip rs (rotations rs)
  where
    -- rotations [1,2,3] == [[2,3,1],[3,1,2],[1,2,3]]
    rotations xs =
      take (length xs)
      [ take (length xs) $ drop n (cycle xs) | n <- [1..]]

    -- if we are blocked is because no new writing threads can be
    -- created and we are at the end of the partition.
    whenBlocked :: BlockedIndefinitelyOnSTM -> IO (Either ReadError a)
    whenBlocked _ = return $ Left EndOfPartition

    go = \case
      [] -> STM.retry
      (PartitionReader reader pnumber _, rs) : xs -> do
        mval <- R.tryRead reader
        case mval of
          Nothing -> go xs
          Just (offset, Record bs) -> do
            STM.writeTVar var (Just rs)
            return $ Right (bs, Meta pnumber offset)

-- | If the partition was moved to a different consumer
-- the commit will fail silently.
commit :: HasCallStack => Consumer -> Meta -> IO ()
commit (Consumer cid _ var) (Meta pnumber offset) =
  atomicallyNamed ("topic.consumer.commit " <> show cid) $ do
  mreaders <- STM.readTVar var
  let mreader = find (\r -> r_partition r == pnumber) $ fromMaybe [] mreaders
  forM_ mreader $ \r ->
    -- we save `offset + 1` because it denotes the reset point,
    -- not the actual ofset value saved.
    STM.modifyTVar (r_committed r) $ \previous -> max previous (offset + 1)

newtype PartitionCount = PartitionCount Int

partitionCount :: Topic -> PartitionCount
partitionCount topic = PartitionCount $ HashMap.size (t_partitions topic)
