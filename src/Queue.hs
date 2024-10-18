module Queue
  ( Queue
  , TopicName(..)
  , OpenQueueError(..)
  , PartitionCount(..)
  , withQueue
  , openTopic
  )
  where

import Control.Concurrent (MVar, newMVar, modifyMVar, withMVar)
import Control.Concurrent.Async (withAsync)
import Control.Exception (bracket, throwIO, Exception(..))
import Control.Monad (forM, forM_, forever, when)
import qualified Data.ByteString.Lazy as LB
import Data.Aeson (FromJSON, ToJSON, FromJSONKey, ToJSONKey)
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Encode.Pretty as Aeson (encodePretty)
import Data.Hashable (Hashable)
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Text (Text)
import qualified Data.Text as Text
import Data.Void (Void)
import System.Directory (doesFileExist, removeFile, createDirectoryIfMissing)
import System.FilePath ((</>))

import Queue.Topic
  ( Topic
  , TopicState(..)
  , PartitionNumber(..)
  )
import qualified Queue.Topic as T
import qualified Queue.Partition.File as FilePartition
import Queue.Partition.File (FilePartition)
import Utils.Some (Some(..))
import Utils.Thread (delay, Delay(..))

data Queue = Queue
  { q_store :: Store
  , q_count :: PartitionCount -- ^ default number of partitions in new topics
  , q_topics :: MVar (HashMap TopicName TopicData)
  }

data TopicData = TopicData
  { d_topic :: Topic
  , d_partitions :: HashMap PartitionNumber FilePartition
  }

-- | Where Queue information is stored.
newtype Store = Store FilePath

-- | The information being kept in the store.
-- Has enough information to re-instantiate a queue to resume
-- consumption and production from any arbitrary point.
newtype Inventory = Inventory (HashMap TopicName TopicState)
  deriving newtype (FromJSON, ToJSON)

newtype TopicName = TopicName { unTopicName :: Text }
  deriving Show
  deriving newtype (Eq, Ord, Hashable, FromJSON, ToJSON, FromJSONKey, ToJSONKey)

data OpenQueueError
  = CorruptedInventory String
  | QueueLocked FilePath
  deriving Show

instance Exception OpenQueueError where
  displayException = \case
    CorruptedInventory err -> "Inventory file is corrupetd: " <> err
    QueueLocked path -> "Queue locked. The queue is already open. Lock found at: " <> path

newtype PartitionCount = PartitionCount Int

withQueue
  :: FilePath
  -> PartitionCount  -- ^ default partition count for new topics
  -> (Queue -> IO a) -> IO a
withQueue path count act =
  bracket (open (Store path) count) close $ \queue ->
    withAsync (saver queue) $ \_ ->
      act queue
  where
  saver :: Queue -> IO Void
  saver queue = every (Seconds 5) (save queue)

open :: Store -> PartitionCount -> IO Queue
open store count = do
  inventoryLock store
  e <- inventoryLoad store
  inventory <- case e of
    Left Missing -> return $ Inventory mempty
    Left (Unreadable err) -> throwIO $ CorruptedInventory err
    Right i -> return i

  topics <- openTopics store inventory
  topicsVar <- newMVar topics
  return $ Queue
    { q_store = store
    , q_count = count
    , q_topics = topicsVar
    }

close :: Queue -> IO ()
close queue@(Queue store _ var) = do
  save queue
  modifyMVar var $ \topics -> do
    forM_ topics $ \tdata -> do
      T.closeTopic $ d_topic tdata
      traverse FilePartition.close $ d_partitions tdata
    return (error "Queue: use after closed", ())
  inventoryRelease store

save :: Queue -> IO ()
save (Queue store _ var) =
  withMVar var $ \topics -> do
  -- do it in separate STM transactions to minimise retries.
  -- any offset moved during a `getState` operation
  inventory <- Inventory <$> forM topics (T.getState . d_topic)
  inventoryWrite store inventory

every :: Delay -> IO a -> IO b
every period act = forever $ do
  delay period
  act

openTopics :: Store -> Inventory -> IO (HashMap TopicName TopicData)
openTopics store (Inventory inventory) = do
  flip HashMap.traverseWithKey inventory $ \name (TopicState ps cs) -> do
    partitions <- openPartitions store name ps
    topic <- T.openTopic (Some <$> partitions) cs
    return $ TopicData topic partitions

openPartitions
  :: Store
  -> TopicName
  -> Set PartitionNumber
  -> IO (HashMap PartitionNumber FilePartition)
openPartitions store name pset = forM hmap openPartition
  where
  numbers = Set.toList pset
  hmap = HashMap.fromList $ zip numbers numbers
  openPartition pnumber = do
    let (path, fname) = partitionPath store name pnumber
    createDirectoryIfMissing True path
    FilePartition.open path fname

partitionPath :: Store -> TopicName -> PartitionNumber -> (FilePath, String)
partitionPath (Store path) (TopicName name) (PartitionNumber n) =
  ( path </> Text.unpack name
  , show n <> ".partition"
  )

inventoryPath :: Store -> FilePath
inventoryPath (Store path) = path </> "inventory.json"

inventoryWrite :: Store -> Inventory -> IO ()
inventoryWrite store inventory = do
  LB.writeFile (inventoryPath store) (Aeson.encodePretty inventory)

data InventoryReadError
  = Missing
  | Unreadable String

inventoryLoad :: Store -> IO (Either InventoryReadError Inventory)
inventoryLoad store = do
  let path = inventoryPath store
  exists <- doesFileExist path
  if not exists
    then return (Left Missing)
    else do
      r <- Aeson.eitherDecodeFileStrict (inventoryPath store)
      return $ case r of
        Left err -> Left (Unreadable err)
        Right i -> Right i

openTopic :: Queue -> TopicName -> IO Topic
openTopic (Queue store (PartitionCount count) var) name =
  modifyMVar var $ \topics ->
  case HashMap.lookup name topics of
    Just (TopicData topic _) -> return (topics, topic)
    Nothing -> do
      let pnumbers = Set.fromList $ fmap PartitionNumber [0..count - 1]
      partitions <- openPartitions store name pnumbers
      topic <- T.openTopic (Some <$> partitions) mempty
      let tdata = TopicData topic partitions
      return (HashMap.insert name tdata topics, topic)

inventoryLock :: Store -> IO ()
inventoryLock (Store path) = do
  let lock = path </> "inventory.lock"
  exists <- doesFileExist lock
  when exists (throwIO $ QueueLocked lock)
  writeFile lock "locked"

inventoryRelease :: Store -> IO ()
inventoryRelease (Store path) = do
  let lock = path </> "inventory.lock"
  removeFile lock





