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

import Control.Concurrent.Async (forConcurrently_, forConcurrently)
import Control.Concurrent.STM
  ( STM
  , TVar
  , atomically
  , readTVar
  , writeTVar
  , newTVar
  , readTVarIO
  )
import Control.Exception (bracket)
import Control.Monad (forM_, forM)
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
  , Position(..)
  )
import qualified Queue.Partition as Partition

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
    , r_committed :: TVar Offset
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
  Partition.write partition (p_encode msg)
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
            , g_offsets = offsets
            , g_consumers = mempty
            }
      writeTVar t_cgroups $ HashMap.insert name group groups
      return group

    -- create a new consumer group
    openReaders :: ConsumerGroup -> IO ()
    openReaders group = do
      readers <- forConcurrently (HashMap.toList $ g_offsets group) $ \(pnumber, offsetVar) -> do
        let partition = t_partitions HashMap.! pnumber
        mkReader partition pnumber offsetVar
      atomically $ do
        groups <- readTVar t_cgroups
        let g = groups HashMap.! gname
            g' = g { g_readers = Just readers }
        writeTVar t_cgroups $ HashMap.insert gname g' groups

    mkReader :: PartitionInstance -> PartitionNumber -> TVar Offset -> IO ReaderInstance
    mkReader (PartitionInstance partition) number offsetVar = do
      reader <- Partition.openReader partition
      offset <- readTVarIO offsetVar
      Partition.seek reader (At offset)
      return ReaderInstance
        { r_reader = reader
        , r_partition = number
        , r_committed = offsetVar
        }

    -- re-distribute readers across all existing consumers
    -- returns an action to reset moved readers to the latest checkpoint
    rebalance :: ConsumerGroupName -> STM (IO ())
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

      return $ forConcurrently_ toReset $ \(offset, ReaderInstance{..}) ->
        Partition.seek r_reader (Partition.At offset)

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
    add cid = do
      (new, reset) <- atomically $ (,)
        <$> addConsumer
        <*> rebalance gname
      reset
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
      (toClose, reset) <- atomically $ (,)
        <$> removeConsumer
        <*> rebalance gname

      case toClose of
        Nothing -> reset
        Just readers ->
          forConcurrently_  readers $ \ReaderInstance{..} ->
            Partition.closeReader r_reader
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


data Meta = Meta Offset PartitionNumber

-- | blocks until there is a message.
read :: Consumer -> IO (ByteString, Meta)
read  = undefined

commit :: Consumer -> Meta -> IO ()
commit  = undefined
