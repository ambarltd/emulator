module Ambar.Emulator.Connector.File
   ( connect
   , encoder
   , partitioner
   ) where

{-| File connector.
Read JSON values from a file.
One value per line.
-}

import qualified Data.Aeson as Json
import Control.Monad (forM_)
import qualified Data.ByteString.Lazy.Char8 as Char8
import qualified Data.ByteString.Lazy as LB
import qualified Data.Text.Lazy as Text
import qualified Data.Text.Lazy.Encoding as Text

import Ambar.Emulator.Queue.Topic (Producer, Encoder, Partitioner, modPartitioner)
import qualified Ambar.Emulator.Queue.Topic as Topic
import Utils.Logger (SimpleLogger, fatal, logInfo)

newtype FileRecord = FileRecord Json.Value

encoder :: Encoder FileRecord
encoder (FileRecord value) = LB.toStrict $ Json.encode value

partitioner :: Partitioner FileRecord
partitioner = modPartitioner (const 1)

connect :: SimpleLogger -> Producer FileRecord -> FilePath -> IO ()
connect logger producer path = do
   bs <- Char8.readFile path
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

