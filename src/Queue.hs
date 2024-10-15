module Queue
  ( Queue
  , withQueue
  , openTopic
  , TopicName(..)
  )
  where

import Control.Concurrent (threadDelay, MVar, newMVar, modifyMVar, withMVar)
import Control.Concurrent.Async (withAsync)
import Control.Exception (bracket, throwIO, Exception(..))
import Control.Monad (forM, forM_, forever)
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
import System.Directory (doesFileExist)
import System.FilePath ((</>))

import Queue.Topic
  ( Topic
  , TopicState(..)
  , PartitionNumber(..)
  , PartitionInstance(..)
  )
import qualified Queue.Topic as T
import qualified Queue.Partition.File as FilePartition

data Queue = Queue
  { q_store :: Store
  , q_topics :: MVar (HashMap TopicName Topic)
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

newtype OpenQueueError
  = CorruptedInventory String
  deriving Show

instance Exception OpenQueueError where
  displayException = \case
    CorruptedInventory err -> "Inventory file is corrupetd: " <> err

withQueue :: FilePath -> (Queue -> IO a) -> IO a
withQueue path act =
  bracket (open (Store path)) close $ \queue ->
    withAsync (saver queue) $ \_ ->
      act queue
  where
  saver :: Queue -> IO Void
  saver queue = every (Seconds 5) (save queue)

open :: Store -> IO Queue
open store = do
  e <- inventoryLoad store
  inventory <- case e of
    Left Missing -> return $ Inventory mempty
    Left (Unreadable err) -> throwIO $ CorruptedInventory err
    Right i -> return i

  topics <- openTopics store inventory
  topicsVar <- newMVar topics
  return $ Queue
    { q_store = store
    , q_topics = topicsVar
    }

close :: Queue -> IO ()
close queue@(Queue _ var) = do
  save queue
  modifyMVar var $ \topics -> do
    forM_ topics T.closeTopic
    return (error "Queue: use after closed", ())

save :: Queue -> IO ()
save (Queue store var) =
  withMVar var $ \topics -> do
  -- do it in separate STM transactions to minimise retries.
  -- any offset moved during a `getState` operation
  inventory <- Inventory <$> forM topics T.getState
  inventoryWrite store inventory

newtype Seconds = Seconds Int

every :: Seconds -> IO a -> IO b
every (Seconds s) act = forever $ do
  threadDelay nanoseconds
  act
  where nanoseconds = s * 1_000_000

openTopics :: Store -> Inventory -> IO (HashMap TopicName Topic)
openTopics store (Inventory inventory) = do
  flip HashMap.traverseWithKey inventory $ \name (TopicState ps cs) -> do
    partitions <- openPartitions store name ps
    T.openTopic partitions cs

openPartitions
  :: Store
  -> TopicName
  -> Set PartitionNumber
  -> IO (HashMap PartitionNumber PartitionInstance)
openPartitions store name pset = forM hmap openPartition
  where
  numbers = Set.toList pset
  hmap = HashMap.fromList $ zip numbers numbers
  openPartition pnumber = do
    let (fpath, fname) = partitionPath store name pnumber
    filePartition <- FilePartition.open fpath fname
    return (PartitionInstance filePartition)

partitionPath :: Store -> TopicName -> PartitionNumber -> (FilePath, String)
partitionPath (Store path) (TopicName name) (PartitionNumber n) =
  ( path </> Text.unpack name
  , show n <> ".partition"
  )

inventoryPath :: Store -> FilePath
inventoryPath (Store path) = path </> "inventory.bin"

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

_DEFAULT_PARTITION_COUNT :: Int
_DEFAULT_PARTITION_COUNT = 10

openTopic :: Queue -> TopicName -> IO Topic
openTopic (Queue store var) name =
  modifyMVar var $ \topics ->
  case HashMap.lookup name topics of
    Just topic -> return (topics, topic)
    Nothing -> do
      let pnumbers = Set.fromList $ fmap PartitionNumber [0.._DEFAULT_PARTITION_COUNT - 1]
      partitions <- openPartitions store name pnumbers
      topic <- T.openTopic partitions mempty
      return (HashMap.insert name topic topics, topic)





