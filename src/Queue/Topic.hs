{-# LANGUAGE RecursiveDo #-}
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

import qualified Control.Concurrent.Async as Async
import Control.Concurrent.Async
  ( Async
  , forConcurrently_
  , forConcurrently
  , mapConcurrently_
  )
import qualified Control.Concurrent.STM as STM
import Control.Concurrent.STM
  ( STM
  , TVar
  , TMVar
  , atomically
  )
import Control.Exception (bracket, finally)
import Control.Monad (forM_, forM, when)
import Data.ByteString (ByteString)
import Data.Foldable (sequenceA_)
import Data.Hashable (Hashable(..))
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.List (find)
import Data.List.Extra (chunksOf)
import Data.Maybe (fromMaybe, isNothing, isJust)
import Data.Text (Text)
import qualified Data.Text as Text
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Time.Clock (nominalDiffTimeToSeconds)
import Data.Traversable (mapAccumR)
import Data.Void (Void)
import System.Random (randomIO)

import Queue.Partition
  ( Partition
  , Offset
  , Record(..)
  , Position(..)
  )
import qualified Queue.Partition as Partition

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

-- | A group of ordered readers of independent streams (partitions).
-- Consumers always stream data from a disjoint sets of partitions.
data ConsumerGroup = ConsumerGroup
  { g_readers :: Maybe [ReaderInstance]
  , g_comitted :: HashMap PartitionNumber (TVar Offset)
  -- ^ comitted offsets. The TVar is shared with the ReaderInstance
  , g_consumers :: HashMap ConsumerId Consumer
  }

newtype ConsumerId = ConsumerId UUID
  deriving Show
  deriving newtype (Eq, Ord, Hashable)

-- | A consumer holds any number of reader instances.
-- The particular instances and their number is adjusted through
-- rebalances at consumer creation and descruction.
-- Contains an infinite cycling list of reader instances for round-robin picking
data Consumer = Consumer Topic (TVar [ReaderInstance])

-- | A single (totally ordered) stream of data from a partition.
data ReaderInstance = ReaderInstance
  { r_worker :: Async Void
  , r_next :: TMVar (Offset, Record) -- ^ take this to get the next element
  , r_expected :: TVar Offset        -- ^ next offset to be read. Change this to seek.
  , r_committed :: TVar Offset       -- ^ last committed offset
  , r_partition :: PartitionNumber
  }

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

    mapConcurrently_ readerDestroy readers

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
        let partition = t_partitions HashMap.! pnumber
        readerNew t_name partition pnumber offsetVar
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

      forConcurrently_ toClose readerDestroy
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
    rebalance :: ConsumerGroupName -> STM [ReaderInstance]
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
      toReset <- forM changed $ \i -> do
        offset <- STM.readTVar (r_committed i)
        return (offset, i)
      forM_ toReset $ \(offset, rinstance) ->
        readerSeek rinstance offset

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
          -> STM (HashMap (ConsumerId, PartitionNumber) ReaderInstance)
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
  go seen (r:rs) = do
    let pnumber = r_partition r
    when (pnumber `elem` seen) STM.retry
    mval <- readerTryRead r
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
  forM_ mreader $ \ReaderInstance{..} ->
    STM.writeTVar r_committed offset

readerSeek :: ReaderInstance -> Offset -> STM ()
readerSeek ReaderInstance{..} offset = do
  next <- STM.readTVar r_expected
  when (next /= offset) $ do
    STM.writeTVar r_expected offset

readerTryRead :: ReaderInstance -> STM (Maybe (Offset, Record))
readerTryRead ReaderInstance{..} = do
  mval <- STM.tryTakeTMVar r_next
  when (isJust mval) $ STM.modifyTVar r_expected (+1)
  return mval

readerDestroy :: ReaderInstance -> IO ()
readerDestroy ReaderInstance{..} = Async.cancel r_worker

readerNew
  :: TopicName
  -> PartitionInstance
  -> PartitionNumber
  -> TVar Offset
  -> IO ReaderInstance
readerNew (TopicName tname) (PartitionInstance partition) number offsetVar = do
  reader <- Partition.openReader partition
  start <- STM.readTVarIO offsetVar
  expectedVar <- STM.newTVarIO start
  nextVar <- STM.newEmptyTMVarIO

  -- the reader is only controlled by the worker
  let work needle = do
        -- check if seek is needed and if value in nextVar is still valid.
        mseek <- atomically $ do
          mval <- STM.tryReadTMVar nextVar
          expected <- STM.readTVar expectedVar
          case mval of
            Nothing ->
              if needle /= expected
              then return $ Just expected
              else return Nothing
            Just (offset, _) ->
              if offset == expected
              then STM.retry -- wait till value is consumed
              else do
                -- there was a seek since last read.
                -- Let's discard the value read and
                -- move the needle to the new position.
                _ <- STM.takeTMVar nextVar
                return $ Just expected

        forM_ mseek $ \pos -> Partition.seek reader (At pos)

        -- block till a value is read
        (offset, record) <- Partition.read reader

        atomically $ do
          expected <- STM.readTVar expectedVar
          when (expected == offset ) $ STM.putTMVar nextVar (offset, record)

        work (offset + 1)

      destroy worker = do
        Async.cancel worker
        Partition.closeReader reader
        atomically $ STM.writeTMVar nextVar $ error $ unwords
          [ "Topic (" <> Text.unpack tname <> "):"
          , "reading from destroyed reader"
          ]

  mdo
    worker <- Async.async (work 0 `finally` destroy worker)
    return ReaderInstance
      { r_worker = worker
      , r_next = nextVar
      , r_expected = expectedVar
      , r_partition = number
      , r_committed = offsetVar
      }
