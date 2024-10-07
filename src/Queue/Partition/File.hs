module Queue.Partition.File where

import Control.Concurrent (MVar, newMVar, modifyMVar, withMVar)
import Control.Concurrent.STM (TVar, readTVarIO, atomically, readTVar, retry, writeTVar)
import Control.Concurrent.Mutex (Mutex, withMutex)
import Control.Monad (unless, when)
import qualified Data.Binary as Binary
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Lazy as LB
import Data.Word (Word64)
import System.IO
  ( Handle
  , hSeek
  , withFile
  , IOMode(..)
  , SeekMode(..)
  )

import Queue.Partition

-- | A file-backed Partition for unformatted JSON entries.
--
-- Goals:
--  * Allow users to inspect the partition file in a text editor. One entry per line.
--  * Fast sequential reading
--  * Fast seek to position
--  * Support concurrent reads
--
-- Concurrent writes are not supported.
data FilePartition = FilePartition
  { p_records:: FilePath
  , p_index :: FilePath
  , p_lock :: Mutex  -- ^ write lock
  , p_info :: TVar (Count, Bytes) -- ^ record count and size
  }

-- | Size in bytes
newtype Bytes = Bytes Word64
  deriving (Eq, Show)

newtype Count = Count Int
  deriving (Eq, Show)

{-| Note [File Partition Design]

The implementation involves 2 files, one index and a records file.

The records file contains one record per line.
This allow for fast sequential consumption as we can just go through the file
reading line by line.

The only write operation allowed on the records file is to append a new record.
This allows for safe concurrent reads and for reads to happen concurrently with
writing.

To allow for fast seeks (jump to the nth entry) we use an index file. The index
file is a binary encoded sequence of unsigned 64 bit integers. The nth entry in
the index represents the byte offset of the nth entry in the records file. Like
the records file, only append append write operations are allowed in the index
file.

As a consequence of the 'readable in a text editor' requirement, the '\n'
character is used as the record separator and is therefore not allowed in the
record. That's why this structure is targets unformatted JSON records.

-}

-- A single-threaded file readed.
newtype FileReader = FileReader (MVar ReaderInfo)

data ReaderInfo = ReaderInfo
  { r_next :: Offset -- ^ offset of next record to be read
  , r_handle :: Handle
  , r_length :: Int -- ^ cached partition length
  , r_info :: TVar (Count, Bytes) -- ^ record count and size of partition.
  }

-- | Size in bytes of 64 bits unsigned integer.
_WORD64_SIZE :: Int
_WORD64_SIZE = 8

instance Partition FilePartition where
  type Reader FilePartition = FileReader

  seek :: Position -> FilePartition -> (FileReader -> IO a) -> IO a
  seek pos (FilePartition{..}) act =
    case pos of
      At i -> readerFrom i
      Beginning -> readerFrom (Offset 0)
      End -> do
        (Count len, _) <- readTVarIO p_info
        readerFrom $ Offset $ len - 1
    where
    readerFrom ix =  do
      (Count len, _) <- readTVarIO p_info
      offset <- withFile p_index ReadMode $ \handle -> do
        hSeek handle AbsoluteSeek $ fromIntegral $ len * _WORD64_SIZE
        bytes <- B.hGet handle _WORD64_SIZE
        case Binary.decodeOrFail $ LB.fromStrict bytes of
          Left (_,_,err) -> error $ "FilePartition: Error reading index: " <> err
          Right  (_,_,n) -> return n

      withFile p_records ReadMode $ \handle -> do
        hSeek handle AbsoluteSeek offset
        var <- newMVar $ ReaderInfo
          { r_next = ix
          , r_handle = handle
          , r_length = len
          , r_info = p_info
          }
        act $ FileReader var

  -- | Reads one record and advances the Reader.
  -- Blocks if there are no more records.
  read :: FileReader -> IO (Offset, Record)
  read (FileReader var) =
    modifyMVar var $ \ReaderInfo{..} -> do
      let offset = r_next -- offset to be read.
      len <- if r_length > unOffset offset
         then return r_length
         else do
           atomically $ do
             (Count newLen, _) <- readTVar r_info
             -- block till there are more elements to read.
             unless (newLen > unOffset offset) retry
             return newLen

      record <- Record <$> Char8.hGetLine r_handle

      let info = ReaderInfo
            { r_next = r_next + 1
            , r_handle = r_handle
            , r_length = len
            , r_info = r_info
            }
      return (info, (offset, record))

  getOffset :: FileReader -> IO Offset
  getOffset (FileReader var) = withMVar var $ return . r_next

  write :: Record -> FilePartition -> IO ()
  write (Record bs) (FilePartition{..}) = do
    when (Char8.elem '\n' bs) $
      error "FilePartition write: record contains newline character"

    withMutex p_lock $ do
      (Count count, Bytes partitionSize) <- readTVarIO p_info

      let entry = LB.fromStrict bs <> "\n"
          entrySize = fromIntegral (B.length bs)
          entryByteOffset = Binary.encode partitionSize
          newSize = entrySize + partitionSize
          newCount = count + 1

      LB.appendFile p_records entry
      LB.appendFile p_index entryByteOffset
      atomically $ writeTVar p_info (Count newCount, Bytes newSize)