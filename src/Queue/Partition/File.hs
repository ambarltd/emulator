module Queue.Partition.File where

import Control.Concurrent (MVar, newMVar)
import Control.Concurrent.STM (TVar, readTVarIO)
import Control.Concurrent.Mutex (Mutex)
import Control.Monad (void, replicateM_)
import Data.ByteString.Char8 as Char8
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
  read = undefined

  getIndex :: FileReader -> Index
  getIndex = undefined

  write :: Record -> FilePartition -> IO ()
  write = undefined
