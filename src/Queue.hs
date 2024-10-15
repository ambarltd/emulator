module Queue where

import Control.Concurrent (threadDelay, MVar, newMVar, modifyMVar, withMVar)
import Control.Concurrent.Async (withAsync)
import Control.Exception (bracket, throwIO, Exception(..))
import Control.Monad (forM, forM_, forever)
import Data.Hashable (Hashable)
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Text (Text)
import qualified Data.Text as Text
import Data.Void (Void)
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

newtype TopicName = TopicName { unTopicName :: Text }
  deriving Show
  deriving newtype (Eq, Ord, Hashable)

data OpenQueueError
  = CorruptedInventory
  deriving Show

instance Exception OpenQueueError where
  displayException = \case
    CorruptedInventory -> "Inventory file is corrupetd."

withQueue :: FilePath -> (Queue -> IO a) -> IO a
withQueue path act =
  bracket open close $ \queue ->
    withAsync (saver queue) $ \_ ->
      act queue
  where
  store = Store path

  open :: IO Queue
  open = do
    e <- inventoryLoad store
    inventory <- case e of
      Left Missing -> return $ Inventory mempty
      Left Unreadable -> throwIO CorruptedInventory
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

  saver :: Queue -> IO Void
  saver queue = every (Seconds 5) (save queue)

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
    partitions <- openPartitions name ps
    T.openTopic partitions cs
  where
  openPartitions
    :: TopicName
    -> Set PartitionNumber
    -> IO (HashMap PartitionNumber PartitionInstance)
  openPartitions name pset = do
    let numbers = Set.toList pset
        hmap = HashMap.fromList $ zip numbers numbers
    forM hmap $ \pnumber -> do
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
inventoryWrite = undefined

data InventoryReadError
  = Missing
  | Unreadable

inventoryLoad :: Store -> IO (Either InventoryReadError Inventory)
inventoryLoad = undefined


