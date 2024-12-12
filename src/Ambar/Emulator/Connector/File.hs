module Ambar.Emulator.Connector.File
   ( FileConnector(..)
   , FileConnectorState(..)
   ) where

{-| File connector.
Read JSON values from a file.
One value per line.
-}

import Control.Monad (forM_)
import qualified Data.Aeson as Json
import qualified Data.ByteString.Lazy.Char8 as Char8
import qualified Data.ByteString.Lazy as LB
import Data.Default (Default)
import Data.Int (Int64)
import Data.Text (Text)
import qualified Data.Text.Lazy as Text
import qualified Data.Text.Lazy.Encoding as Text
import GHC.Generics (Generic)

import qualified Ambar.Emulator.Connector as C
import Ambar.Emulator.Queue.Topic (modPartitioner)
import qualified Ambar.Emulator.Queue.Topic as Topic
import Utils.Async (withAsyncThrow)
import Utils.Logger (fatal, logInfo)

data FileConnector = FileConnector
   { c_path :: FilePath
   , c_partitioningField :: Text
   , c_incrementingField :: Text
   }

data FileConnectorState = FileConnectorState
   { c_fileSize :: Int64
   , c_currentOffset :: Int64
   }
   deriving (Show, Generic)
   deriving anyclass (Json.ToJSON, Json.FromJSON, Default)

newtype FileRecord = FileRecord Json.Value

instance C.Connector FileConnector where
  type ConnectorState FileConnector = FileConnectorState
  type ConnectorRecord FileConnector = FileRecord
  partitioner = modPartitioner (const 1)
  encoder (FileRecord value) = LB.toStrict $ Json.encode value
  connect (FileConnector {..}) logger initialState producer f =
    withAsyncThrow worker $ f (return initialState)
    where
    worker = do
       bs <- Char8.readFile c_path
       forM_ (Char8.lines bs) $ \line -> do
          value <- case Json.eitherDecode line of
             Left e -> fatal logger $ unlines
                [ "Unable to decode value from source:"
                , show e
                , Text.unpack $ Text.decodeUtf8 bs
                ]
             Right v -> return v
          Topic.write producer (FileRecord value)
          logInfo logger $ "ingested. " <> Text.decodeUtf8 line

