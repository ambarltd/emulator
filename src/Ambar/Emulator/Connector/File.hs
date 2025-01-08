module Ambar.Emulator.Connector.File
  ( FileConnector
  , FileConnectorState
  , FileRecord
  , mkFileConnector
  , write
  , c_path
  ) where

{-| File connector.
Read JSON values from a file.
One value per line.
-}

import Control.Concurrent (MVar, newMVar, withMVar)
import Control.Concurrent.STM
  ( STM
  , TMVar
  , TVar
  , newTVarIO
  , readTVar
  , atomically
  , writeTVar
  , newTMVarIO
  , modifyTVar
  , retry
  , takeTMVar
  , putTMVar
  )
import Control.Exception (bracket)
import Control.Monad (forever, when)
import qualified Data.Aeson as Json
import qualified Data.Aeson.KeyMap as KeyMap
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Lazy as LB
import Data.Default (Default)
import Data.Maybe (fromMaybe)
import Data.String (IsString(fromString))
import Data.Text (Text)
import qualified Data.Text.Lazy as LText
import qualified Data.Text.Lazy.Encoding as LText
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import GHC.Generics (Generic)
import GHC.IO.FD (FD)
import GHC.IO.Handle (HandlePosn(..), hGetPosn)
import System.Directory (getFileSize)
import System.IO
  ( Handle
  , hSeek
  , openFile
  , hSeek
  , hIsEOF
  , hClose
  , IOMode(..)
  , SeekMode(..)
  )
import Prettyprinter ((<+>))

import qualified Ambar.Emulator.Connector as C
import Ambar.Emulator.Queue.Partition.File
  ( openNonLockingWritableFD
  , writeFD
  )
import Ambar.Emulator.Queue.Topic (modPartitioner)
import Ambar.Emulator.Queue.Topic (Producer)
import qualified Ambar.Emulator.Queue.Topic as Topic
import Util.Async (withAsyncThrow)
import Util.Logger (SimpleLogger, fatal, logInfo)
import Util.Delay (Duration, delay, millis)
import Util.Prettyprinter (prettyJSON, renderPretty, commaSeparated)

_POLLING_INTERVAL :: Duration
_POLLING_INTERVAL = millis 50

data FileConnector = FileConnector
  { c_path :: FilePath
  , c_partitioningField :: Text
  , c_incrementingField :: Text
  , c_state :: TVar FileConnectorState
  , c_readHandle :: TMVar Handle
  , c_writeHandle :: MVar FD
  , c_getFileSize :: IO Integer
  }

-- | We don't close these file descriptors because we consider that
-- this is only used during tests.
mkFileConnector :: FilePath -> Text -> Text -> IO FileConnector
mkFileConnector path partitioningField incrementingField = do
  size <- getFileSize path
  varState <- newTVarIO (FileConnectorState size 0)
  varWriteHandle <- do
    fd <- openNonLockingWritableFD path
    newMVar fd
  varReadHandle <- do
    readHandle <- openFile path ReadMode
    newTMVarIO readHandle
  return $ FileConnector
    path
    partitioningField
    incrementingField
    varState
    varReadHandle
    varWriteHandle
    (getFileSize path)

-- Does not work in the presence of external writers to the same file.
write :: FileConnector -> Json.Value -> IO ()
write FileConnector{..} json = do
  withMVar c_writeHandle $ \fd -> do
    let entry = LB.toStrict (Json.encode json) <> "\n"
        entrySize = fromIntegral (BS.length entry)
    writeFD fd entry
    atomically $ modifyTVar c_state $ \state ->
      state { c_fileSize = c_fileSize state + entrySize }

data FileConnectorState = FileConnectorState
  { c_fileSize :: Integer
  , c_offset :: Integer
  }
  deriving (Show, Generic)
  deriving anyclass (Json.ToJSON, Json.FromJSON, Default)

newtype FileRecord = FileRecord Json.Value

instance C.Connector FileConnector where
  type ConnectorState FileConnector = FileConnectorState
  type ConnectorRecord FileConnector = FileRecord
  partitioner = modPartitioner (const 1)
  encoder (FileRecord value) = LB.toStrict $ Json.encode value
  connect = connect

connect
  :: FileConnector
  -> SimpleLogger
  -> FileConnectorState
  -> Producer (FileRecord)
  -> (STM FileConnectorState -> IO a)
  -> IO a
connect conn@(FileConnector {..}) logger initState producer f = do
  h <- atomically $ do
    writeTVar c_state initState
    takeTMVar c_readHandle
  hSeek h AbsoluteSeek (c_offset initState)
  atomically $ putTMVar c_readHandle h
  withAsyncThrow updateFileSize $
    withAsyncThrow worker $
      f (readTVar c_state)
  where
  updateFileSize = forever $ do
    newSize <- c_getFileSize
    delay _POLLING_INTERVAL -- also serves to wait until any writing finishes
    atomically $ do
      FileConnectorState fsize offset <- readTVar c_state
      when (fsize < newSize) $
        writeTVar c_state $ FileConnectorState newSize offset

  worker = forever $ do
    value <- readNext
    let record = FileRecord value
    Topic.write producer record
    logResult record

  logResult record =
    logInfo logger $ renderPretty $
      "ingested." <+> commaSeparated
        [ "incrementing_value:" <+> prettyJSON (incrementingValue conn record)
        , "partitioning_value:" <+> prettyJSON (partitioningValue conn record)
        ]

  -- | Blocks until there is something to read.
  readNext :: IO Json.Value
  readNext =
    withReadLock $ \readHandle -> do
    bs <- Char8.hGetLine readHandle
    value <- case Json.eitherDecode $ LB.fromStrict bs of
       Left e -> fatal logger $ unlines
          [ "Unable to decode value from source:"
          , show e
          , Text.unpack $ Text.decodeUtf8 bs
          ]
       Right v -> return v
    HandlePosn _ offset <- hGetPosn readHandle
    atomically $ modifyTVar c_state $ \state -> state { c_offset = offset }
    return value

  withReadLock :: (Handle -> IO a) -> IO a
  withReadLock = bracket acquire release
    where
    acquire = do
      -- wait till there is data to read and take the lock.
      (h, offset) <- atomically $ do
        FileConnectorState fsize offset <- readTVar c_state
        when (fsize <= offset) retry
        h <- takeTMVar c_readHandle
        return (h, offset)

      -- For some reason, if the file we are reading is updated by an external
      -- program (like the user manually adding an entry) the file reading library
      -- don't detect that EOF has moved. In this case we have to close this handle
      -- and open a new one.
      eof <- hIsEOF h
      if not eof
        then return h
        else do
          hClose h
          h' <- openFile c_path ReadMode
          hSeek h' AbsoluteSeek offset
          return h'

    release readHandle = atomically $
      putTMVar c_readHandle readHandle


partitioningValue :: FileConnector -> FileRecord -> Json.Value
partitioningValue FileConnector{..} r = getField c_partitioningField r

incrementingValue :: FileConnector -> FileRecord -> Json.Value
incrementingValue FileConnector{..} r = getField c_incrementingField r

getField :: Text -> FileRecord -> Json.Value
getField field (FileRecord json) =
  fromMaybe err $ do
    o <- getObject json
    let key = fromString $ Text.unpack field
    v <- KeyMap.lookup key o
    return $ v
  where
  err = error $ Text.unpack $  "invalid serial value in :" <> jsonToTxt json

  jsonToTxt :: Json.Value -> Text
  jsonToTxt = LText.toStrict . LText.decodeUtf8 . Json.encode

  getObject :: Json.Value -> Maybe Json.Object
  getObject = \case
    Json.Object o -> Just o
    _ -> Nothing

