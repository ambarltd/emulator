module Queue.Partition.File
  ( FilePartition
  , FileReader
  , withFilePartition
  , open
  , close
  , OpenError(..)
  , WriteError(..)
  ) where

import Control.Concurrent (MVar, newMVar, withMVar, modifyMVar_)
import qualified Control.Concurrent.STM as STM
import Control.Concurrent.STM
  ( TVar
  , TMVar
  , atomically
  )
import Control.Exception (Exception(..), bracket, bracketOnError, throwIO)
import Control.Monad (void, unless, when)
import qualified Data.Binary as Binary
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as B (unsafeUseAsCStringLen)
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Lazy as LB
import Data.Coerce (coerce)
import Data.Word (Word64)
import GHC.IO.Handle (hLock, LockMode(..))
import GHC.IO.FD (FD)
import GHC.Stack (HasCallStack)
import qualified GHC.IO.FD as FD
import qualified GHC.IO.Device as FD
import System.IO
  ( Handle
  , hSeek
  , withFile
  , openFile
  , hClose
  , IOMode(..)
  , SeekMode(..)
  )
import System.Directory (doesFileExist, getFileSize, removeFile)
import System.FilePath ((</>))

import Queue.Partition
import Utils.STM (atomicallyNamed)

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
  { p_name :: PartitionName
  , p_records:: FilePath
  , p_index :: FilePath
  , p_lock :: FilePath
  , p_handles :: MVar (Maybe (FD.FD, FD.FD))
      -- ^ write handles (records, index)
      -- is Nothing when file partition is closed
  , p_info :: TVar (Count, Bytes) -- ^ record count and size
  }

newtype PartitionName = PartitionName { unPartitionName :: String }

-- | Size in bytes
newtype Bytes = Bytes Word64
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

data OpenError
  = AlreadyOpen FilePath
  | MissingRecords FilePath
  | MissingIndex FilePath
  deriving Show

instance Exception OpenError where
  displayException = \case
    AlreadyOpen path -> "Lock found. Partition already open: " <> path
    MissingRecords path -> "Missing records file: " <> path
    MissingIndex path -> "Missing index file: " <> path

open :: HasCallStack => FilePath -> String -> IO FilePartition
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
  var <- STM.newTVarIO (count, size)
  return FilePartition
    { p_name = PartitionName name
    , p_records = records
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
      when exists_lock $ throwIO $ AlreadyOpen lock
      when (exists_records /= exists_index) $
        if not exists_records
        then throwIO $ MissingRecords records
        else throwIO $ MissingIndex index
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

close :: HasCallStack => FilePartition -> IO ()
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
openNonLockingWritableFD :: HasCallStack => FilePath -> IO FD
openNonLockingWritableFD path = do
    (fd, _) <- FD.openFile path AppendMode True
    FD.release fd
    return fd

closeNonLockingWritableFD :: FD -> IO ()
closeNonLockingWritableFD = FD.close

-- A single-threaded file readed.
-- If you use it from multiple threads you will have problems.
data FileReader = FileReader PartitionName (TMVar ReaderInfo)

data ReaderInfo = ReaderInfo
  { r_next :: TVar Offset -- ^ offset of next record to be read
  , r_handle :: Maybe Handle -- ^ Nothing if reader is closed
  , r_length :: Count -- ^ cached partition length
  , r_partition :: FilePartition
  }

-- | Size in bytes of 64 bits unsigned integer.
_WORD64_SIZE :: Int
_WORD64_SIZE = 8

data WriteError
  = ClosedPartition
  deriving Show

instance Exception WriteError where
  displayException = \case
    ClosedPartition -> "writing to closed partition"

instance Partition FilePartition where
  type Reader FilePartition = FileReader

  openReader :: HasCallStack => FilePartition -> IO FileReader
  openReader p@(FilePartition{..}) = do
    (len, _) <- STM.readTVarIO p_info
    handle <- openFile p_records ReadMode
    hLock handle SharedLock
    next <- STM.newTVarIO 0
    var <- STM.newTMVarIO $ ReaderInfo
      { r_next = next
      , r_handle = Just handle
      , r_length = len
      , r_partition = p
      }
    return $ FileReader p_name var

  closeReader :: HasCallStack => FileReader -> IO ()
  closeReader (FileReader _ var) =
    modifyTMVarIO_ var $ \info -> do
      maybe (return ()) hClose (r_handle info)
      return info { r_handle = Nothing }

  seek :: HasCallStack => FileReader -> Position -> IO ()
  seek (FileReader _ var) pos =
    bracketOnError acquire undo $ \(next, offset, info) -> do
      when (next /= offset) $
        case r_handle info of
          Nothing ->
            throwErr (r_partition info) "seek: closed reader."
          Just handle -> do
            Bytes byteOffset <- readIndexEntry (p_index $ r_partition info) offset
            hSeek handle AbsoluteSeek (fromIntegral byteOffset)

      atomicallyNamed "file.seek" $ do
        STM.putTMVar var info
    where
    acquire = atomicallyNamed "file.seek.acquire" $ do
      info <- STM.takeTMVar var
      (Count count, _) <- STM.readTVar $ p_info $ r_partition info
      let offset = case pos of
            At o -> o
            Beginning -> 0
            End -> Offset count - 1

      when (offset > fromIntegral count) $
        throwErr (r_partition info) $ unwords
          [ "seek: offset out of bounds."
          , "Entries:", show count
          , "Offset:", show offset
          ]

      next <- STM.readTVar (r_next info)
      STM.writeTVar (r_next info) offset
      return (next, offset, info)

    undo (next,_,info) = atomicallyNamed "file.undo" $ do
      STM.putTMVar var info
      STM.writeTVar (r_next info) next

  -- | Reads one record and advances the Reader.
  -- Blocks if there are no more records.
  read :: HasCallStack => FileReader -> IO (Offset, Record)
  read (FileReader _ var) =
    bracketOnError acquire undo $ \(len, next, info) -> do
      handle <- case r_handle info of
        Nothing -> error $ unwords
            [ "FilePartition: read: closed reader."
            , "Partition:", unPartitionName (p_name $ r_partition info)
            ]
        Just h -> return h

      record <- Record <$> Char8.hGetLine handle

      atomicallyNamed "file.read" $ do
        STM.writeTVar (r_next info) (next + 1)
        STM.putTMVar var info { r_length = len }

      return (next, record)
    where
      acquire = atomicallyNamed "file.read.acquire" $ do
        info <- STM.takeTMVar var
        next <- STM.readTVar (r_next info)
        partitionLength <-
          if r_length info > fromIntegral next
          then return (r_length info)
          else fst <$> STM.readTVar (p_info $ r_partition info)
        -- block till there are more elements to read.
        unless (partitionLength > fromIntegral next) STM.retry
        return (partitionLength, next, info)

      undo (_,_,info) = atomicallyNamed "file.read.undo" $ STM.putTMVar var info

  write :: HasCallStack => FilePartition -> Record -> IO ()
  write (FilePartition{..}) (Record bs)  = do
    when (Char8.elem '\n' bs) $
      error "FilePartition write: record contains newline character"

    withMVar p_handles $ \mhandles -> do
      (fd_records, fd_index) <- maybe (throwIO ClosedPartition) return mhandles
      (Count count, Bytes partitionSize) <- STM.readTVarIO p_info

      let entry = bs <> "\n"
          entrySize = fromIntegral (B.length bs)
          entryByteOffset = B.toStrict $ Binary.encode partitionSize
          newSize = entrySize + partitionSize
          newCount = count + 1

      writeNonBlocking fd_records entry
      writeNonBlocking fd_index entryByteOffset
      atomicallyNamed "file.write" $ STM.writeTVar p_info (Count newCount, Bytes newSize)

writeNonBlocking :: HasCallStack => FD -> ByteString -> IO ()
writeNonBlocking fd bs =
  void $ B.unsafeUseAsCStringLen bs $ \(ptr, len) ->
  FD.writeNonBlocking fd (coerce ptr) 0 (fromIntegral len)

readIndexEntry :: HasCallStack => FilePath -> Offset -> IO Bytes
readIndexEntry indexPath (Offset offset)  = do
  withFile indexPath ReadMode $ \handle -> do
    hLock handle SharedLock
    hSeek handle AbsoluteSeek $ fromIntegral $ offset * _WORD64_SIZE
    bytes <- B.hGet handle _WORD64_SIZE
    byteOffset <- case Binary.decodeOrFail $ LB.fromStrict bytes of
      Left (_,_,err) -> error $ "FilePartition: Error reading index: " <> err
      Right  (_,_,n) -> return n
    return $ Bytes byteOffset

throwErr :: FilePartition -> String -> a
throwErr p msg =
  error $
    "FilePartition (" <> unPartitionName (p_name p) <> "): " <> msg

modifyTMVarIO :: TMVar a -> (a -> IO (a, b)) -> IO b
modifyTMVarIO var act =
  bracketOnError
    (atomically $ STM.takeTMVar var)
    (atomically . STM.putTMVar var)
    $ \x -> do
      (x', y) <- act x
      atomically $ STM.putTMVar var x'
      return y

modifyTMVarIO_ :: TMVar a -> (a -> IO a) -> IO ()
modifyTMVarIO_ var act =
  modifyTMVarIO var $ fmap (,()) . act

