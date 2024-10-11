module Test.Queue (testQueues) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (async, wait, concurrently)
import Control.Monad (replicateM)
import Data.Char (isAscii)
import qualified Data.HashMap.Strict as HashMap
import qualified Data.Text as Text
import Data.Text.Encoding as Text
import System.IO.Temp (withSystemTempDirectory)
import Test.Hspec (shouldBe, Spec, it, describe)
import Foreign.Marshal.Utils (withMany)

import Queue.Topic
  ( Topic
  , TopicName(..)
  , Meta(..)
  , ConsumerGroupName(..)
  , withTopic
  )
import qualified Queue.Topic as T
import Queue.Topic (PartitionInstance(..))
import Queue.Partition (Partition(..), Position(..), Record(..))
import qualified Queue.Partition as P
import qualified Queue.Partition.File as FilePartition

testQueues :: Spec
testQueues  = describe "queue" $ do
  describe "partition" $ do
    testPartition withFilePartition
  describe "topic" $ do
    testTopic withFileTopic
  where
  withFilePartition path act =
    FilePartition.withFilePartition path "file-partition" act

  withFileTopic :: PartitionCount -> (Topic -> IO a) -> IO a
  withFileTopic (PartitionCount n) act =
    withTempPath $ \path ->
    withMany (f path) [0..n-1] $ \pinstances ->
    withTopic
      (TopicName "test-topic")
      (HashMap.fromList $ zip [0..] pinstances)
      mempty
      act
    where
    f path pnumber g = do
      FilePartition.withFilePartition path (show pnumber) (g . PartitionInstance)

withTempPath :: (FilePath -> IO a) -> IO a
withTempPath = withSystemTempDirectory "partition-XXXXX"

-- | Infinite list of messages of varying lengths
messages :: [Record]
messages = toRecord <$> zipWith take lengths sentences
  where
    toRecord str = Record $ encodeUtf8 $ Text.pack str
    lengths = fmap (\n -> 1 + n `mod` 12) [1..]
    sentences = "lorem ipsum" : fmap (fmap nextAscii) sentences
    nextAscii c =
      if isAscii (succ c)
      then succ c
      else '#' -- chr 35

newtype PartitionCount = PartitionCount Int

testTopic :: (forall b. PartitionCount -> (Topic -> IO b) -> IO b) -> Spec
testTopic with = do
  it "reads from a single partition" $
    with (PartitionCount 1) $ \topic -> do
      one : two : three : _ <- return $ zip ([0..] :: [Int]) messages
      T.withProducer topic fst (unRecord . snd) $ \producer -> do
        putStrLn "writing one"
        T.write producer one
        putStrLn "writing two"
        T.write producer two
        putStrLn "writing three"
        T.write producer three
        putStrLn "done writing"
      T.withConsumer topic (ConsumerGroupName "test-group") $ \consumer -> do
          putStrLn "reading one"
          (one_, Meta _ _ n1) <- T.read consumer
          n1 `shouldBe` 0
          putStrLn $ show n1
          putStrLn "reading two"
          (two_, Meta _ _ n2) <- T.read consumer
          n2 `shouldBe` 1
          Record one_ `shouldBe` snd one
          Record two_ `shouldBe` snd two
          putStrLn "done reading"
      putStrLn "done"

testPartition :: Partition a => (forall b. FilePath -> (a -> IO b) -> IO b) -> Spec
testPartition with = do
  it "reads what is written" $
    withTempPath $ \path ->
    with path $ \partition -> do
      one : two : _ <- return messages
      P.write partition one
      P.write partition two
      P.withReader partition Beginning $ \reader -> do
        (o1, one_) <- P.read reader
        o1 `shouldBe` 0
        (o2, two_) <- P.read reader
        o2 `shouldBe` 1
        one_ `shouldBe` one
        two_ `shouldBe` two

  it "reopens" $
    withTempPath $ \path -> do
      one : two : _ <- return messages
      with path $ \partition -> do
        P.write partition one
        P.write partition two
      with path $ \partition ->
        P.withReader partition Beginning $ \reader -> do
          (_, one_) <- P.read reader
          (_, two_) <- P.read reader
          one_ `shouldBe` one
          two_ `shouldBe` two

  it "blocks reads when reaches the end" $
    withTempPath $ \path ->
    with path $ \partition -> do
      one : two : _ <- return messages
      let read' =
            P.withReader partition Beginning $ \reader -> do
            (_, one_) <- P.read reader
            (_, two_) <- P.read reader
            return (one_, two_)
          write' = do
            P.write partition one
            P.write partition two

      r <- async read'
      _ <- async $ do
        threadDelay 50
        write'
      (one_, two_) <- wait r
      one_ `shouldBe` one
      two_ `shouldBe` two

  it "reads concurrently" $
    withTempPath $ \path ->
    with path $ \partition -> do
      let len = 10
          selected = take len messages
          read' =
            P.withReader partition Beginning $ \reader ->
              replicateM len $ snd <$> P.read reader
          write' =
            traverse (P.write partition) selected

      r <- async $ concurrently read' read'
      _ <- async write'
      (left, right) <- wait r
      left `shouldBe` right
      left `shouldBe` selected

