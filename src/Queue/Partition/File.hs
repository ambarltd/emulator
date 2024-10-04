module Queue.Partition.File where

import Control.Concurrent (MVar, newMVar, modifyMVar)
import Control.Concurrent.STM (TVar, readTVarIO, atomically, readTVar, retry)
import Control.Concurrent.Mutex (Mutex)
import Control.Monad (void, replicateM_, when)
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Lazy as B
import System.IO
  ( Handle
  , openFile
  , IOMode(..)
  )

import Queue.Partition

-- | A file-backed partition is optimised for sequential consumption.
-- It supports concurrent reads but not concurrent writes.
data FilePartition = FilePartition
  { p_records:: FilePath
  , p_recordsWrite :: Mutex
  , p_end :: TVar Index
  }

newtype FileReader = FileReader (MVar ReaderInfo)

data ReaderInfo = ReaderInfo
  { r_position :: Index
  , r_handle :: Handle
  , r_end :: Index -- partition end last time we checked.
  , r_partitionEnd :: TVar Index
  }

instance Partition FilePartition where
  type Reader FilePartition = FileReader

  seek :: Position -> FilePartition -> IO FileReader
  seek pos (FilePartition{..}) = case pos of
    At i -> readerFrom i
    Beginning -> readerFrom (Index 0)
    End -> readerFrom =<< readTVarIO p_end
    where
    readerFrom (Index i) =  do
      handle <- openFile p_records ReadMode
      skip i handle
      end <- readTVarIO p_end
      var <- newMVar $ ReaderInfo
        { r_position = Index i
        , r_handle = handle
        , r_end = end
        , r_partitionEnd = p_end
        }
      return $ FileReader var

    skip n h = replicateM_ n $ void $ Char8.hGetLine h

  -- | Reads one record and advances the Reader.
  -- Blocks if there are no more records.
  read :: FileReader -> IO Record
  read (FileReader var) =
    modifyMVar var $ \ReaderInfo{..} -> do
      end <- if r_end >= r_position
         then return r_end
         else do
           atomically $ do
             n <- readTVar r_partitionEnd
             when (n == r_position) retry
             return n
      record <- Record . B.fromStrict <$> Char8.hGetLine r_handle
      let info = ReaderInfo
            { r_position = r_position + 1
            , r_handle = r_handle
            , r_end = end
            , r_partitionEnd = r_partitionEnd
            }
      return (info, record)

  getIndex :: FileReader -> Index
  getIndex = undefined

  write :: Record -> FilePartition -> IO ()
  write = undefined
