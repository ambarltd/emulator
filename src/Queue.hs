module Queue where

import Control.Concurrent (MVar, newMVar, modifyMVar, readMVar)
import Control.Exception (bracket, throwIO, Exception(..))
import Control.Monad (forM, forM_)
import Data.Hashable (Hashable)
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.HashSet (HashSet)
import qualified Data.HashSet as HashSet
import Data.Text (Text)
import qualified Data.Text as Text
import System.FilePath ((</>))

import Queue.Topic
  ( Topic
  , TopicState
  , PartitionNumber(..)
  , PartitionInstance(..)
  )
import qualified Queue.Topic as T
import qualified Queue.Partition.File as FilePartition

newtype Queue = Queue (MVar QState)

newtype StorePath = StorePath FilePath

data QState = QState
  { q_store :: StorePath
  , q_topics :: MVar (HashMap TopicName Topic)
  }

newtype Inventory = Inventory
  (HashMap TopicName (HashSet PartitionNumber, TopicState))

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
withQueue path = bracket open close
  where
  store = StorePath path

  open :: IO Queue
  open = do
    e <- inventoryRead store
    inventory <- case e of
      Left Missing -> return $ Inventory mempty
      Left Unreadable -> throwIO CorruptedInventory
      Right i -> return i

    topics <- openTopics inventory
    topicsVar <- newMVar topics
    Queue <$> newMVar QState
      { q_store = store
      , q_topics = topicsVar
      }

  openTopics :: Inventory -> IO (HashMap TopicName Topic)
  openTopics (Inventory inventory) = do
    flip HashMap.traverseWithKey inventory $ \name (pset, state) -> do
      partitions <- openPartitions name pset
      T.openTopic partitions state

  openPartitions
    :: TopicName
    -> HashSet PartitionNumber
    -> IO (HashMap PartitionNumber PartitionInstance)
  openPartitions name pset = do
    let numbers = HashSet.toList pset
        hmap = HashMap.fromList $ zip numbers numbers
    forM hmap $ \pnumber -> do
      let (fpath, fname) = partitionPath store name pnumber
      filePartition <- FilePartition.open fpath fname
      return (PartitionInstance filePartition)

  close :: Queue -> IO ()
  close (Queue var) =
    modifyMVar var $ \queue -> do
      topics <- readMVar (q_topics queue)
      forM_ topics T.closeTopic
      return (error "Queue: used after closed", ())

partitionPath :: StorePath -> TopicName -> PartitionNumber -> (FilePath, String)
partitionPath (StorePath path) (TopicName name) (PartitionNumber n) =
  ( path </> Text.unpack name
  , show n <> ".partition"
  )

inventoryPath :: StorePath -> FilePath
inventoryPath (StorePath path) = path </> "inventory.bin"

inventoryWrite :: StorePath -> Inventory -> IO ()
inventoryWrite = undefined

data InventoryReadError
  = Missing
  | Unreadable

inventoryRead :: StorePath -> IO (Either InventoryReadError Inventory)
inventoryRead = undefined

