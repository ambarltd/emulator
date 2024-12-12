module Ambar.Emulator.Connector.File
  ( FileConnector(..)
  , FileConnectorState(..)
  ) where

{-| File connector.
Read JSON values from a file.
One value per line.
-}

import Control.Concurrent.STM (STM, newTVarIO, readTVar, readTVarIO, atomically, writeTVar)
import Control.Monad (forever)
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
import System.IO (Handle, withFile, hFileSize, hSeek, IOMode(..), SeekMode(..))
import Prettyprinter ((<+>))

import qualified Ambar.Emulator.Connector as C
import Ambar.Emulator.Queue.Topic (modPartitioner)
import Ambar.Emulator.Queue.Topic (Producer)
import qualified Ambar.Emulator.Queue.Topic as Topic
import Ambar.Record (Value(..))
import Utils.Async (withAsyncThrow)
import Utils.Logger (SimpleLogger, fatal, logInfo)
import Utils.Delay (Duration, delay, millis)
import Utils.Prettyprinter (prettyJSON, renderPretty, commaSeparated)

_POLLING_INTERVAL :: Duration
_POLLING_INTERVAL = millis 50

data FileConnector = FileConnector
  { c_path :: FilePath
  , c_partitioningField :: Text
  , c_incrementingField :: Text
  }

data FileConnectorState = FileConnectorState
  { c_fileSize :: Integer
  , c_currentOffset :: Integer
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
  svar <- newTVarIO initState
  let initialOffset = c_currentOffset initState
  withFile c_path ReadMode $ \h -> do
    hSeek h AbsoluteSeek initialOffset
    withAsyncThrow (worker h svar) $ f (readTVar svar)
  where
  worker h svar = forever $ do
    FileConnectorState fsize0 offset <- readTVarIO svar
    fsize <- waitForData h fsize0 offset
    line <- Char8.hGetLine h
    value <- case Json.eitherDecode $ LB.fromStrict line of
       Left e -> fatal logger $ unlines
          [ "Unable to decode value from source:"
          , show e
          , Text.unpack $ Text.decodeUtf8 line
          ]
       Right v -> return v
    let record = FileRecord value
    Topic.write producer record
    logResult record
    let offset' = offset + fromIntegral (BS.length line)
    atomically $ writeTVar svar $ FileConnectorState fsize offset'

  waitForData :: Handle -> Integer -> Integer -> IO Integer
  waitForData h fsize offset =
    if fsize > offset
    then return fsize
    else do
      fsize' <- hFileSize h
      let stillNoData = fsize' == fsize
      if stillNoData
        then do
           delay _POLLING_INTERVAL
           waitForData h fsize offset
        else return fsize'

  logResult record =
    logInfo logger $ renderPretty $
      "ingested." <+> commaSeparated
        [ "incrementing_value:" <+> prettyJSON (incrementingValue conn record)
        , "partitioning_value:" <+> prettyJSON (partitioningValue conn record)
        ]

partitioningValue :: FileConnector -> FileRecord -> Value
partitioningValue FileConnector{..} r = getField c_partitioningField r

incrementingValue :: FileConnector -> FileRecord -> Value
incrementingValue FileConnector{..} r = getField c_incrementingField r

getField :: Text -> FileRecord -> Value
getField field (FileRecord json) =
  fromMaybe err $ do
    o <- getObject json
    let key = fromString $ Text.unpack field
    v <- KeyMap.lookup key o
    let txt = jsonToTxt v
    return $ Json txt v
  where
  err = error $ Text.unpack $  "invalid serial value in :" <> jsonToTxt json

  jsonToTxt :: Json.Value -> Text
  jsonToTxt = LText.toStrict . LText.decodeUtf8 . Json.encode

  getObject :: Json.Value -> Maybe Json.Object
  getObject = \case
    Json.Object o -> Just o
    _ -> Nothing

