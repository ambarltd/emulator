module Ambar.Emulator.Connector.File
   ( connect
   , encoder
   , partitioner
   ) where

{-| File connector.
Read records from a file.
One record per line.
Records are treated as plain text.
-}

import Control.Monad (forM_)
import qualified Data.ByteString.Lazy.Char8 as Char8
import qualified Data.ByteString.Lazy as LB
import Data.ByteString (ByteString)

import Ambar.Emulator.Queue.Topic (Producer, Encoder, Partitioner, modPartitioner)
import qualified Ambar.Emulator.Queue.Topic as Topic

newtype FileRecord = FileRecord Char8.ByteString

encoder :: Encoder FileRecord
encoder (FileRecord bs) = LB.toStrict bs

partitioner :: Partitioner ByteString
partitioner = modPartitioner (const 1)

connect :: Producer FileRecord -> FilePath -> IO ()
connect producer path = do
   bs <- Char8.readFile path
   forM_ (Char8.lines bs) $ \line ->
      Topic.write producer (FileRecord line)

