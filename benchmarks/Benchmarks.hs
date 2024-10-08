module Main where

import Control.Concurrent.Async (concurrently_, replicateConcurrently_)
import Control.Monad (replicateM_, forM_)
import qualified Data.Text as Text
import Data.Text.Encoding as Text (encodeUtf8)
import Criterion (Benchmark)
import qualified Criterion
import qualified Criterion.Main as Criterion
import Data.Char (isAscii)
import System.IO.Temp (withSystemTempDirectory)

import Queue.Partition (Record(..), Position(..))
import qualified Queue.Partition as P
import Queue.Partition.File (FilePartition)
import qualified Queue.Partition.File as F

main :: IO ()
main = withPartition $ \partition -> do
  -- pre-fill partition
  forM_ messages $ P.write partition

  Criterion.defaultMain
    [ Criterion.bgroup "queue"
      [ benchPartition partition messages
      ]
    ]
  where
  messages = take messageCount messageStream

  messageCount = 10_000 :: Int

  -- | Infinite list of messages of varying lengths
  messageStream :: [Record]
  messageStream = toRecord <$> zipWith take lengths sentences
    where
      -- lengths in characters
      minMsgLength = 20
      maxMsgLength = 200
      toRecord str = Record $ Text.encodeUtf8 $ Text.pack str
      lengths = flip fmap [1..] $ \n ->
        minMsgLength + n `mod` (maxMsgLength - minMsgLength)
      sentences = "lorem ipsum" : fmap (fmap nextAscii) sentences
      nextAscii c =
        if isAscii (succ c)
        then succ c
        else '#' -- chr 35

type Messages = [Record]

benchPartition :: FilePartition -> Messages -> Benchmark
benchPartition preFilledPartition msgs = Criterion.bgroup "file partition"
  [ Criterion.bench (unwords ["read", show count, "messages"]) $
    Criterion.whnfIO $ do
        readFrom preFilledPartition

  , Criterion.bench (unwords ["read", show count, "messages 3x in parallel"]) $
    Criterion.whnfIO $ do
      replicateConcurrently_ 3 (readFrom preFilledPartition)

  , Criterion.bench (unwords ["write", show count, "messages"]) $
    Criterion.whnfIO $ do
      withPartition $ \partition ->
        writeTo partition

  , Criterion.bench (unwords ["write and read", show count, "messages in series"]) $
    Criterion.whnfIO $ do
      withPartition $ \partition -> do
        _ <- writeTo partition
        readFrom partition

  , Criterion.bench (unwords ["write and read", show count, "messages in parallel"]) $
    Criterion.whnfIO $ do
      withPartition $ \partition -> do
        concurrently_ (writeTo partition) (readFrom partition)
  ]
  where
  count = length msgs

  writeTo partition =
    traverse (P.write partition) msgs

  readFrom partition =
    P.seek partition Beginning $ \reader ->
      replicateM_ count (P.read reader)

withPartition :: (F.FilePartition -> IO b) -> IO b
withPartition f = withTempPath $ \path -> withFilePartition path f

withTempPath :: (FilePath -> IO a) -> IO a
withTempPath = withSystemTempDirectory "partition-XXXXX"

withFilePartition  :: FilePath -> (F.FilePartition -> IO b) -> IO b
withFilePartition path act =
  F.withFilePartition path "file-partition" act
