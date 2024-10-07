module Queue.Partition.File where

import Control.Concurrent (MVar, newMVar, modifyMVar, withMVar)
import Control.Concurrent.STM (TVar, readTVarIO, atomically, readTVar, retry, writeTVar)
import Control.Concurrent.Mutex (Mutex, withMutex)
import Control.Monad (when)
import qualified Data.Binary as Binary
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Lazy as LB
import Data.Int (Int64)
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
  , p_length :: TVar Int -- ^ record count
  }

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
  { r_position :: Index -- ^ index of next record to be read
  , r_handle :: Handle
  , r_length :: Int -- ^ cached partition length
  , r_liveLength :: TVar Int
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
      Beginning -> readerFrom (Index 0)
      End -> do
        len <- readTVarIO p_length
        readerFrom $ Index $ len - 1
    where
    readerFrom ix =  do
      len <- readTVarIO p_length
      offset <- withFile p_index ReadMode $ \handle -> do
        hSeek handle AbsoluteSeek $ fromIntegral $ len * _WORD64_SIZE
        bytes <- B.hGet handle _WORD64_SIZE
        case Binary.decodeOrFail $ LB.fromStrict bytes of
          Left (_,_,err) -> error $ "FilePartition: Error reading index: " <> err
          Right  (_,_,n) -> return n

      withFile p_records ReadMode $ \handle -> do
        hSeek handle AbsoluteSeek offset
        var <- newMVar $ ReaderInfo
          { r_position = ix
          , r_handle = handle
          , r_length = len
          , r_liveLength = p_length
          }
        act $ FileReader var

  -- | Reads one record and advances the Reader.
  -- Blocks if there are no more records.
  read :: FileReader -> IO Record
  read (FileReader var) =
    modifyMVar var $ \ReaderInfo{..} -> do
      end <- if r_length >= r_position
         then return r_length
         else do
           atomically $ do
             n <- readTVar r_liveLength
             when (n == r_position) retry
             return n
      record <- Record <$> Char8.hGetLine r_handle
      let info = ReaderInfo
            { r_position = r_position + 1
            , r_handle = r_handle
            , r_length = end
            , r_liveLength = r_liveLength
            }
      return (info, record)

  getIndex :: FileReader -> IO Index
  getIndex (FileReader var) = withMVar var $ return . r_position

  write :: Record -> FilePartition -> IO ()
  write (Record bs) (FilePartition records index mutex endVar) =
    withMutex mutex $ do
    LB.appendFile records $ LB.fromStrict bs <> "\n"
    Index end <- readTVarIO endVar
    let newEnd = end + 1
    withFile index WriteMode $ \handle -> do
      B.hPut handle $ LB.toStrict $ Binary.encode @Int64 $ fromIntegral newEnd
      hSeek AbsoluteSeek newEnd
    atomically $ do
      writeTVar endVar $ Index end + 1
