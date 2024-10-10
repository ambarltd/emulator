 module Queue.Topic
    ( Topic
    , ConsumerGroupName
    , Consumer
    , Producer
    , Meta(..)
    , withProducer
    , write
    , withConsumer
    , read
    , commit
    ) where

import Prelude hiding (read)

import Control.Concurrent.Async
  ( Async
  , async
  , cancel
  , forConcurrently_
  , forConcurrently
  )
import Control.Concurrent.STM
  ( STM
  , TVar
  , TMVar
  , atomically
  , readTVar
  , newTVarIO
  , writeTVar
  , newTVar
  , newEmptyTMVarIO
  , readTVarIO
  , putTMVar
  , writeTMVar
  , retry
  , tryTakeTMVar
  )
import Control.Exception (bracket)
import Control.Monad (forM_, forM, when)
import Data.ByteString (ByteString)
import Data.List.Extra (chunksOf)
import Data.Hashable (Hashable(..))
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.List (find)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as Text
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Time.Clock (nominalDiffTimeToSeconds)
import Data.Void (Void)
import System.Random (randomIO)

import Queue.Partition
  ( Partition
  , Offset
  , Record(..)
  , Reader
  , Position(..)
  )
import qualified Queue.Partition as Partition

data Topic = Topic
  { t_name :: TopicName
  , t_partitions :: HashMap PartitionNumber PartitionInstance
  , t_cgroups :: TVar (HashMap ConsumerGroupName ConsumerGroup)
  }

newtype TopicName = TopicName { unTopicName :: Text }
  deriving newtype (Eq, Ord)

newtype PartitionNumber = PartitionNumber { unPartitionNumber :: Int }
  deriving newtype (Eq, Ord, Hashable)

data PartitionInstance =
  forall a. Partition a => PartitionInstance a

newtype ConsumerGroupName = ConsumerGroupName Text
  deriving newtype (Eq, Ord, Hashable)

data ConsumerGroup = ConsumerGroup
  { g_readers :: Maybe [ReaderInstance]
  , g_comitted :: HashMap PartitionNumber (TVar Offset)
  -- ^ comitted offsets. The TVar is shared with the ReaderInstance
  , g_consumers :: HashMap ConsumerId Consumer
  }

newtype ConsumerId = ConsumerId UUID
  deriving newtype (Eq, Ord, Hashable)

-- | A consumer holds any number of reader instances.
-- The particular instances and their number is adjusted through
-- rebalances at consumer creation and descruction.
-- Contains an infinite cycling list of reader instances for round-robin picking
data Consumer = Consumer Topic (TVar [ReaderInstance])

data ReaderInstance =
  forall a. Partition a => ReaderInstance
    { r_reader :: Reader a
    , r_worker :: Async Void
    , r_next :: TMVar (Offset, Record) -- ^ take this to get the next element
    , r_nextOffset :: TVar Offset      -- ^ offset we expect in next element.
                                       --   change this to seek.
    , r_partition :: PartitionNumber
    , r_committed :: TVar Offset       -- ^ last committed offset
    }


newtype UUID = UUID Text
  deriving newtype (Eq, Ord, Hashable)

newUUID :: IO UUID
newUUID = do
  now <- nominalDiffTimeToSeconds <$> getPOSIXTime
  fixed <- randomIO @Int
  return $ UUID $ Text.pack $ show now <> show fixed

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

  case g_readers group of
    Just [] -> openReaders group
    _ -> return ()

  cid <- ConsumerId <$> newUUID
  bracket (add cid) (remove cid) act
  where
    -- retrieve an existing group or create a new one
    retrieve :: ConsumerGroupName -> STM ConsumerGroup
    retrieve name = do
      groups <- readTVar t_cgroups
      group <- case HashMap.lookup name groups of
        Just g -> return g
        Nothing -> do
          offsets <- traverse (const $ newTVar 0) t_partitions
          return ConsumerGroup
            { g_readers = Just []
            , g_comitted = offsets
            , g_consumers = mempty
            }
      writeTVar t_cgroups $ HashMap.insert name group groups
      return group

    -- open an existing group's readers
    openReaders :: ConsumerGroup -> IO ()
    openReaders group = do
      readers <- forConcurrently (HashMap.toList $ g_comitted group) $ \(pnumber, offsetVar) -> do
        let partition = t_partitions HashMap.! pnumber
        readerNew partition pnumber offsetVar
      atomically $ do
        groups <- readTVar t_cgroups
        let g = groups HashMap.! gname
            g' = g { g_readers = Just readers }
        writeTVar t_cgroups $ HashMap.insert gname g' groups

    -- re-distribute readers across all existing consumers
    -- resets moved readers to the latest checkpoint
    rebalance :: ConsumerGroupName -> STM ()
    rebalance name = do
      groups <- readTVar t_cgroups
      let group = groups HashMap.! name
          cids = HashMap.keys (g_consumers group)
          readerCount = maybe 0 length (g_readers group)
          consumerCount = length cids
          chunkSize = ceiling @Double $ fromIntegral readerCount / fromIntegral consumerCount
          readerLists = chunksOf chunkSize (fromMaybe [] $ g_readers group) ++ repeat []

      before <- assignments group

      -- update consumers reader lists
      forM_ (zip cids readerLists) $ \(c, readers) ->
        case HashMap.lookup c (g_consumers group) of
          Nothing -> error "rebalancing: missing consumer id"
          Just (Consumer _ rvar) -> writeTVar rvar (cycle readers)

      after <- assignments group

      -- readers which changed consumers should be reset to their last comitted offset.
      let changed = HashMap.elems $ after `HashMap.difference` before
      toReset <- forM changed $ \i -> do
        offset <- readTVar (r_committed i)
        return (offset, i)

      forM_ toReset $ \(offset, rinstance) ->
        readerSeek rinstance offset

    -- get readers assigned to each consumer id.
    assignments
      :: ConsumerGroup
      -> STM (HashMap (ConsumerId, PartitionNumber) ReaderInstance)
    assignments g = do
      xss <- forM (HashMap.toList (g_consumers g)) $ \(cid, Consumer _ rvar) -> do
        readers <- readTVar rvar
        let deduped = case readers of
              [] -> []
              x:xs -> x : takeWhile (\y -> r_partition y /= r_partition x) xs
        return $ flip fmap deduped $ \r -> ((cid, r_partition r), r)
      return $ HashMap.fromList (concat xss)

    add :: ConsumerId -> IO Consumer
    add cid = atomically $ do
      new <- addConsumer
      rebalance gname
      return new
      where
      addConsumer = do
        groups <- readTVar t_cgroups
        newConsumer <- do
          rvar <- newTVar []
          return $ Consumer topic rvar

        let group = groups HashMap.! gname
            group' = group { g_consumers = HashMap.insert cid newConsumer (g_consumers group) }

        -- add new consumer to group
        writeTVar t_cgroups $ HashMap.insert gname group' groups
        return newConsumer

    remove :: ConsumerId -> Consumer -> IO ()
    remove cid _ = do
      toClose <- atomically $ do
        toClose <- removeConsumer
        rebalance gname
        return toClose

      forM_ toClose $ \readers ->
        forConcurrently_ readers $ readerDestroy topic
      where
      removeConsumer :: STM (Maybe [ReaderInstance])
      removeConsumer = do
        groups <- readTVar t_cgroups
        let group = groups HashMap.! gname
            remaining = HashMap.delete cid (g_consumers group)
            group' = group
              { g_readers =
                  if HashMap.null remaining
                    then Nothing
                    else g_readers group
              , g_consumers = remaining
              }

        writeTVar t_cgroups $ HashMap.insert gname group' groups

        return $ if HashMap.null remaining
          then g_readers group
          else Nothing

data Meta = Meta TopicName PartitionNumber Offset

-- | Try to read all readers assigned to the consumer in round-robin fashion.
-- Blocks until there is a message.
read :: Consumer -> IO (ByteString, Meta)
read (Consumer topic var) = atomically $ do
  readers <- readTVar var
  go [] readers
  where
  go _ [] = retry
  go seen (r:rs) = do
    let pnumber = r_partition r
    when (pnumber `elem` seen) retry
    mval <- readerTryRead r
    case mval of
      Nothing -> go (pnumber : seen) rs
      Just (offset, Record bs) -> do
        writeTVar var rs
        return (bs, Meta (t_name topic) pnumber offset)

-- | If the partition was moved to a different consumer
-- the commit will fail silently.
commit :: Consumer -> Meta -> IO ()
commit (Consumer _ var) (Meta _ pnumber offset) = atomically $ do
  readers <- readTVar var
  let mreader = find (\r -> r_partition r == pnumber) readers
  forM_ mreader $ \ReaderInstance{..} ->
    writeTVar r_committed offset

readerSeek :: ReaderInstance -> Offset -> STM ()
readerSeek ReaderInstance{..} = writeTVar r_nextOffset

readerTryRead :: ReaderInstance -> STM (Maybe (Offset, Record))
readerTryRead ReaderInstance{..} = tryTakeTMVar r_next

readerDestroy :: Topic -> ReaderInstance -> IO ()
readerDestroy Topic{..} ReaderInstance{..} = do
  cancel r_worker
  Partition.closeReader r_reader
  atomically $ writeTMVar r_next $ error $ unwords
    [ "Topic (" <> Text.unpack (unTopicName t_name) <> "):"
    , "reading from destroyed reader"
    ]

readerNew :: PartitionInstance -> PartitionNumber -> TVar Offset -> IO ReaderInstance
readerNew (PartitionInstance partition) number offsetVar = do
  reader <- Partition.openReader partition
  start <- readTVarIO offsetVar
  next <- newEmptyTMVarIO
  nextOffset <- newTVarIO start

  let work mseek = do
        forM_ mseek $ \pos -> Partition.seek reader (At pos)
        -- blocks till a value is ready
        (offset, record) <- Partition.read reader
        mseek' <- atomically $ do
          expected <- readTVar nextOffset
          if offset == expected
            then do
              writeTVar nextOffset $ offset + 1
              -- block till previous value is consumed
              putTMVar next (offset, record)
              return Nothing
            else do
              return (Just expected)
        work mseek'

  worker <- async (work (Just start))

  return ReaderInstance
    { r_reader = reader
    , r_worker = worker
    , r_next = next
    , r_nextOffset = nextOffset
    , r_partition = number
    , r_committed = offsetVar
    }
