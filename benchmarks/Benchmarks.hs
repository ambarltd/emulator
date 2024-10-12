module Main where

import Control.Concurrent.Async (concurrently_, replicateConcurrently_, forConcurrently_)
import qualified Data.HashMap.Strict as HashMap
import Control.Monad (replicateM_, forM_)
import qualified Data.Text as Text
import Data.Text.Encoding as Text (encodeUtf8)
import Criterion (Benchmark)
import qualified Criterion
import qualified Criterion.Main as Criterion
import Data.Char (isAscii)
import Foreign.Marshal.Utils (withMany)
import System.IO.Temp (withSystemTempDirectory)

import qualified Queue.Topic as T
import Queue.Topic (ConsumerGroupName(..), Topic)
import Queue.Partition (Record(..), Position(..), Offset(..))
import qualified Queue.Partition as P
import Queue.Partition.File (FilePartition)
import qualified Queue.Partition.File as F

main :: IO ()
main =
  withPartition $ \partition ->
  withMany withPopulatedTopic [1, 10] $ \topics -> do
  -- pre-fill partition
  forM_ messages $ P.write partition

  Criterion.defaultMain
    [ Criterion.bgroup "queue" $ drop 1
      [ benchPartition partition messages
      , benchTopic topics messages
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

  withPopulatedTopic :: PartitionCount -> ((PartitionCount, Topic) -> IO a) -> IO a
  withPopulatedTopic n f =
    withFileTopic n $ \topic -> do
      T.withProducer topic (unOffset . fst) (unRecord . snd) $ \p ->
        forM_ (zip [0..] messages) $ T.write p
      f (n, topic)

type Messages = [Record]

benchTopic :: [(PartitionCount, Topic)] -> Messages -> Benchmark
benchTopic preFilled msgs = Criterion.bgroup "topic" $
  flip fmap preFilled (\(PartitionCount n, topic) ->
    Criterion.bench (unwords ["read", show count, "messages,", show n, "partitions, 1 consumer"]) $
    Criterion.whnfIO $ readFrom topic
  )
  ++
  flip fmap preFilled (\(PartitionCount n, topic) ->
    Criterion.bench (unwords ["read", show count, "messages,", show n, "partitions,", show n, "consumer" <> plural n]) $
    Criterion.whnfIO $
      replicateConcurrently_ n $ do
        T.withConsumer topic (ConsumerGroupName "bench-group") $ \consumer ->
          replicateM_ count (T.read consumer)
  )
  ++
  [ Criterion.bench (unwords ["write", show count, "messages to 1 partition in series"]) $
    Criterion.whnfIO $
      withFileTopic (PartitionCount 1) $ \topic -> do
      T.withProducer topic (unOffset . fst) (unRecord . snd) $ \p ->
        forM_ (zip [0..] msgs) $ T.write p
  , Criterion.bench (unwords ["write", show count, "messages to 5 partitions in series"]) $
    Criterion.whnfIO $
      withFileTopic (PartitionCount 5) $ \topic -> do
      T.withProducer topic (fst @Int) (unRecord . snd) $ \p ->
        forM_ (zip [0..] msgs) $ T.write p
  , let n = 10 in
    Criterion.bench (unwords ["write", show count, "messages to", show n, "partitions in parallel"]) $
    Criterion.whnfIO $
      withFileTopic (PartitionCount n) $ \topic ->
      forConcurrently_ [1..n] $ \k ->
        T.withProducer topic (const k) unRecord $ \p ->
          forM_ (take (count `div` 5) msgs) $ T.write p
  ]
  where
  count = length msgs

  readFrom topic =
    T.withConsumer topic (ConsumerGroupName "bench-group") $ \consumer ->
    replicateM_ count (T.read consumer)

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
    P.withReader partition Beginning $ \reader ->
      replicateM_ count (P.read reader)

withPartition :: (F.FilePartition -> IO b) -> IO b
withPartition f = withTempPath $ \path -> withFilePartition path f

withTempPath :: (FilePath -> IO a) -> IO a
withTempPath = withSystemTempDirectory "partition-XXXXX"

withFilePartition  :: FilePath -> (F.FilePartition -> IO b) -> IO b
withFilePartition path = F.withFilePartition path "file-partition"

plural :: Int -> String
plural n = if n == 1 then "" else "s"

newtype PartitionCount = PartitionCount Int
  deriving newtype (Num, Eq, Ord)

withFileTopic :: PartitionCount -> (Topic -> IO a) -> IO a
withFileTopic (PartitionCount n) act =
  withTempPath $ \path ->
  withMany (f path) [0..n-1] $ \pinstances ->
  T.withTopic
    (T.TopicName "test-topic")
    (HashMap.fromList $ zip [0..] pinstances)
    mempty
    act
  where
  f path pnumber g = do
    F.withFilePartition path (show pnumber) (g . T.PartitionInstance)

