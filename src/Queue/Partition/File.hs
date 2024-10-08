module Queue.Partition.File
  ( FilePartition
  , FileReader
  , withFilePartition
  , open
  , close
  ) where

import Control.Concurrent (MVar, newMVar, modifyMVar, withMVar, modifyMVar_)
import Control.Concurrent.STM (TVar, readTVarIO, atomically, readTVar, retry, writeTVar, newTVarIO)
import Control.Exception (bracket)
import Control.Monad (void, unless, when)
import qualified Data.Binary as Binary
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as B (unsafeUseAsCStringLen)
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Lazy as LB
import Data.Coerce (coerce)
import Data.Maybe (fromMaybe)
import Data.Word (Word64)
import GHC.IO.Handle (hLock, LockMode(..))
import GHC.IO.FD (FD)
import qualified GHC.IO.FD as FD
import qualified GHC.IO.Device as FD
import System.IO
  ( Handle
  , hSeek
  , withFile
  , IOMode(..)
  , SeekMode(..)
  )
import System.Directory (doesFileExist, getFileSize, removeFile)
import System.FilePath ((</>))

import Queue.Partition

-- | A file-backed Partition for unformatted JSON entries.
--
-- Goals:
--  * Allow users to inspect the partition file in a text editor. One entry per line.
--  * Fast sequential reading
--  * Fast seek to position
--  * Support parallel reads
--
-- Concurrent writes are not supported.
data FilePartition = FilePartition
  { p_records:: FilePath
  , p_index :: FilePath
  , p_lock :: FilePath
  , p_handles :: MVar (Maybe (FD.FD, FD.FD))
      -- ^ write handles (records, index)
      -- is Nothing when file partition is closed
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
This allows for safe parallel reads and for reads to happen concurrently with
writing.

To allow for fast seeks (jump to the nth entry) we use an index file. The index
file is a binary encoded sequence of unsigned 64 bit integers. The nth entry in
the index represents the byte offset of the nth entry in the records file. Like
the records file, only append append write operations are allowed in the index
file.

As a consequence of the 'readable in a text editor' requirement, the '\n'
character is used as the record separator and is therefore not allowed in the
record. That's why this structure is targets unformatted JSON records.

We create a lock file to prevent a partition to be opened more than once from
the same or different processes.

-}

withFilePartition :: FilePath -> String -> (FilePartition -> IO a) -> IO a
withFilePartition location name = bracket (open location name) close

open :: FilePath -> String -> IO FilePartition
open location name = do
  let records = location </> name <> ".records"
      index   = location </> name <> ".index"
      lock    = location </> name <> ".lock"
  exists <- checkExistence records index lock
  (count, size) <-
    if not exists
    then createIndex index
    else loadIndex index

  writeFile lock "locked"
  fd_records <- openNonLockingWritableFD records
  fd_index <- openNonLockingWritableFD index
  handles <- newMVar $ Just (fd_records, fd_index)
  var <- newTVarIO (count, size)
  return FilePartition
    { p_records = records
    , p_index = index
    , p_lock = lock
    , p_handles = handles
    , p_info = var
    }
  where
    checkExistence records index lock = do
      exists_records <- doesFileExist records
      exists_index <- doesFileExist index
      exists_lock <- doesFileExist lock
      when exists_lock $
        error $ "Lock found. Partition already open: " <> lock
      when (exists_records /= exists_index) $
        if not exists_records
        then error $ "Missing records file: " <> records
        else error $ "Missing index file: " <> index
      return exists_records

    createIndex index = do
      -- The index file starts with an entry for the position of the zeroeth element.
      -- Therefore the index always contains one more entry than the records file.
      LB.writeFile index $ Binary.encode @Word64 0
      return (Count 0, Bytes 0)

    loadIndex index = do
      indexSize <- fromIntegral <$> getFileSize index
      let count = (indexSize `div` _WORD64_SIZE) - 1
      byteOffsetOfNextEntry <- readIndexEntry index (Offset count)
      return (Count count, byteOffsetOfNextEntry)

close :: FilePartition -> IO ()
close FilePartition{..} =
  modifyMVar_ p_handles $ \case
    Nothing -> return Nothing -- already closed
    Just (fd_records, fd_index) -> do
      removeFile p_lock
      closeNonLockingWritableFD fd_records
      closeNonLockingWritableFD fd_index
      return Nothing

-- | GHC's implementation prevents the overlapping acquisition of write and
-- read handles for files. And write handles are exclusive.
-- To support reads in parallel to writes we need to use a lower level abstraction.
openNonLockingWritableFD :: FilePath -> IO FD
openNonLockingWritableFD path = do
    (fd, _) <- FD.openFile path AppendMode True
    FD.release fd
    return fd

closeNonLockingWritableFD :: FD -> IO ()
closeNonLockingWritableFD = FD.close

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

  seek :: FilePartition -> Position -> (FileReader -> IO a) -> IO a
  seek (FilePartition{..}) pos act =
    case pos of
      At i -> readerFrom i
      Beginning -> readerFrom (Offset 0)
      End -> do
        (Count count, _) <- readTVarIO p_info
        readerFrom $ Offset $ count - 1
    where
    readerFrom offset =  do
      (Count len, _) <- readTVarIO p_info
      Bytes byteOffset <- readIndexEntry p_index offset
      withFile p_records ReadMode $ \handle -> do
        hLock handle SharedLock
        hSeek handle AbsoluteSeek (fromIntegral byteOffset)
        var <- newMVar $ ReaderInfo
          { r_next = offset
          , r_handle = handle
          , r_length = len
          , r_info = p_info
          }
        act $ FileReader var

  -- | Reads one record and advances the Reader.
  -- Blocks if there are no more records.
  read :: FileReader -> IO (Offset, Record)
  read (FileReader var) = do
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

  write :: FilePartition -> Record -> IO ()
  write (FilePartition{..}) (Record bs)  = do
    when (Char8.elem '\n' bs) $
      error "FilePartition write: record contains newline character"

    withMVar p_handles $ \mhandles -> do
      let (fd_records, fd_index) = fromMaybe
              (error "FilePartition: writing to closed partition")
              mhandles
      (Count count, Bytes partitionSize) <- readTVarIO p_info

      let entry = bs <> "\n"
          entrySize = fromIntegral (B.length bs)
          entryByteOffset = B.toStrict $ Binary.encode partitionSize
          newSize = entrySize + partitionSize
          newCount = count + 1

      writeNonBlocking fd_records entry
      writeNonBlocking fd_index entryByteOffset
      atomically $ writeTVar p_info (Count newCount, Bytes newSize)

writeNonBlocking :: FD -> ByteString -> IO ()
writeNonBlocking fd bs =
  void $ B.unsafeUseAsCStringLen bs $ \(ptr, len) ->
  FD.writeNonBlocking fd (coerce ptr) 0 (fromIntegral len)

readIndexEntry :: FilePath -> Offset -> IO Bytes
readIndexEntry path (Offset offset)  =
  withFile path ReadMode $ \handle -> do
    hLock handle SharedLock
    hSeek handle AbsoluteSeek $ fromIntegral $ offset * _WORD64_SIZE
    bytes <- B.hGet handle _WORD64_SIZE
    byteOffset <- case Binary.decodeOrFail $ LB.fromStrict bytes of
      Left (_,_,err) -> error $ "FilePartition: Error reading index: " <> err
      Right  (_,_,n) -> return n
    return $ Bytes byteOffset
