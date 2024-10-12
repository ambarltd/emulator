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
import Control.Concurrent.STM (STM , TVar)
import Control.Exception (bracket)
import Control.Monad (forM_, forM, when)
import Data.ByteString (ByteString)
import Data.Foldable (sequenceA_)
import Data.Hashable (Hashable(..))
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.List (find)
import Data.List.Extra (chunksOf)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as Text
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Time.Clock (nominalDiffTimeToSeconds)
import Data.Traversable (mapAccumR)
import GHC.Stack (HasCallStack)
import System.Random (randomIO)

import Queue.Partition
  ( Partition
  , Offset
  , Record(..)
  , atomicallyNamed
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
-- rebalances at consumer creation and descruction.
-- Contains an infinite cycling list of reader instances for round-robin picking
-- A value of Nothing means that the consumer is closed.
data Consumer = Consumer Topic (TVar (Maybe [PartitionReader]))

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
  :: HasCallStack
  => TopicName
  -> HashMap PartitionNumber PartitionInstance
  -> TopicState
  -> (Topic -> IO a)
  -> IO a
withTopic name partitions groupOffsets act =
  bracket create delete act
  where
  create = do
    cgroupsVar <- atomicallyNamed "topic 1" $ do
      cgroups <- forM groupOffsets $ \partitionOffsets -> do
        offsets <- forM partitionOffsets $ \offset ->
          STM.newTVar offset
        return ConsumerGroup
          { g_state = Closed
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
    readers <- atomicallyNamed "topic 2" $ do
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
        , Ready readers <- [g_state group]
        , reader <- readers
        ]

    forConcurrently_ readers $ R.destroy . r_reader

-- | Get the state of the latest committed offsets of the topic.
getState :: Topic -> STM TopicState
getState Topic{..} = do
  cgroups <- STM.readTVar t_cgroups
  forM cgroups $ \group -> forM (g_comitted group) STM.readTVar

withProducer
  :: (HasCallStack, Hashable b)
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

write :: HasCallStack => Producer a -> a -> IO ()
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

withConsumer :: HasCallStack => Topic -> ConsumerGroupName -> (Consumer -> IO b) -> IO b
withConsumer topic@Topic{..} gname act = do
  cid <- ConsumerId <$> newUUID
  bracket (acquire gname cid) (remove cid) act
  where
    acquire name cid = do
      (new, mg) <- STM.atomically $ do
        group <- prepare name
        let shouldOpenReaders = case g_state group of
              Initialising -> True
              Closed -> error "picked closed group"
              Ready _ -> False
        new <- addConsumer cid
        _ <- rebalance gname
        mg <- if shouldOpenReaders
           then Just . (HashMap.! name) <$> STM.readTVar t_cgroups
           else return Nothing
        return (new, mg)

      forM_ mg $ openReaders name
      return new

    -- retrieve an existing group or create a new one
    prepare :: ConsumerGroupName -> STM ConsumerGroup
    prepare name = do
      groups <- STM.readTVar t_cgroups
      group <- case HashMap.lookup name groups of
        Just g -> case g_state g of
          Closed -> return g { g_state = Initialising } -- we will initialise the group
          Initialising -> STM.retry                     -- someone else is initialising it. Let's wait
          Ready _ -> return g
        Nothing -> do
          -- create a new group
          offsets <- traverse (const $ STM.newTVar 0) t_partitions
          return ConsumerGroup
            { g_state = Initialising
            , g_comitted = offsets
            , g_consumers = mempty
            }
      STM.writeTVar t_cgroups $ HashMap.insert name group groups
      return group

    addConsumer :: ConsumerId -> STM Consumer
    addConsumer cid = do
      groups <- STM.readTVar t_cgroups
      newConsumer <- do
        rvar <- STM.newTVar (Just [])
        return $ Consumer topic rvar

      let group = groups HashMap.! gname
          group' = group { g_consumers = HashMap.insert cid newConsumer (g_consumers group) }

      -- add new consumer to group
      STM.writeTVar t_cgroups $ HashMap.insert gname group' groups
      return newConsumer

    -- open an existing group's readers
    openReaders :: ConsumerGroupName -> ConsumerGroup -> IO ()
    openReaders name group = do
      readers <- forConcurrently (HashMap.toList $ g_comitted group) $ \(pnumber, offsetVar) -> do
        let p = t_partitions HashMap.! pnumber
        case p of
          PartitionInstance partition -> do
            start <- STM.readTVarIO offsetVar
            r <- R.new partition start
            return $ PartitionReader r pnumber offsetVar

      atomicallyNamed "topic 4" $ do
        groups <- STM.readTVar t_cgroups
        let g = groups HashMap.! name
            g' = g { g_state = Ready readers }
        STM.writeTVar t_cgroups $ HashMap.insert name g' groups


    remove :: ConsumerId -> Consumer -> IO ()
    remove cid consumer = do
      toClose <- atomicallyNamed "topic 6" $ do
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
      case g_state group of
        Closed -> error "rebalancing closed consumer"
        Initialising -> return []
        Ready allReaders -> do
          let cids = HashMap.keys (g_consumers group)
              readerCount = length allReaders
              consumerCount = length cids
              chunkSize = ceiling @Double $ fromIntegral readerCount / fromIntegral consumerCount
              readerLists = chunksOf chunkSize allReaders ++ repeat []

          before <- assignments group

          -- update consumers' readers lists
          forM_ (zip cids readerLists) $ \(c, readers) ->
            case HashMap.lookup c (g_consumers group) of
              Nothing -> error "rebalancing: missing consumer id"
              Just (Consumer _ rvar) -> STM.writeTVar rvar $
                if null readers
                 then Just []
                 else Just (cycle readers)

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
        extractClosableReaders = mapAccumR f []
          where
          f acc g =
            let readers = case g_state g of
                  Ready rs -> rs
                  Closed -> []
                  Initialising -> []
            in
            if HashMap.null (g_consumers g)
               then (readers ++ acc, g { g_state = Closed })
               else (acc, g)

        -- get readers assigned to each consumer id.
        assignments
          :: ConsumerGroup
          -> STM (HashMap (ConsumerId, PartitionNumber) PartitionReader)
        assignments g = do
          xss <- forM (HashMap.toList (g_consumers g)) $ \(cid, Consumer _ rvar) -> do
            readers <- STM.readTVar rvar
            let deduped = case readers of
                  Just [] -> []
                  Just (x:xs) -> x : takeWhile (\y -> r_partition y /= r_partition x) xs
                  Nothing -> []
            return [ ((cid, r_partition r), r) | r <- deduped ]
          return $ HashMap.fromList (concat xss)

closeConsumer :: HasCallStack => Consumer -> STM ()
closeConsumer (Consumer _ var) =  STM.writeTVar var Nothing

newUUID :: IO UUID
newUUID = do
  now <- nominalDiffTimeToSeconds <$> getPOSIXTime
  fixed <- randomIO @Int
  return $ UUID $ Text.pack $ show now <> show fixed

data Meta = Meta TopicName PartitionNumber Offset

data ReadError
  = ReadingFromClosedConsumer

-- | Try to read all readers assigned to the consumer in round-robin fashion.
-- Blocks until there is a message.
read :: HasCallStack => Consumer -> IO (Either ReadError (ByteString, Meta))
read (Consumer topic var) = atomicallyNamed "topic 7" $ do
  mreaders <- STM.readTVar var
  case mreaders of
    Nothing -> return $ Left ReadingFromClosedConsumer
    Just [] -> return $ Left ReadingFromClosedConsumer
    Just rs -> go [] rs
  where
  go _ [] = STM.retry
  go seen (PartitionReader reader pnumber _: rs) = do
    when (pnumber `elem` seen) STM.retry
    mval <- R.tryRead reader
    case mval of
      Nothing -> go (pnumber : seen) rs
      Just (offset, Record bs) -> do
        STM.writeTVar var (Just rs)
        return $ Right (bs, Meta (t_name topic) pnumber offset)

-- | If the partition was moved to a different consumer
-- the commit will fail silently.
commit :: HasCallStack => Consumer -> Meta -> IO ()
commit (Consumer _ var) (Meta _ pnumber offset) = atomicallyNamed "topic 8" $ do
  mreaders <- STM.readTVar var
  let mreader = find (\r -> r_partition r == pnumber) $ fromMaybe [] mreaders
  forM_ mreader $ \r -> STM.writeTVar (r_committed r) offset

