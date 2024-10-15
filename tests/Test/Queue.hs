module Test.Queue (testQueues) where

import Prelude hiding (read)

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (async, wait, concurrently)
import Control.Exception (fromException)
import Control.Monad (replicateM, forM_, replicateM_)
import Data.ByteString (ByteString)
import Data.Char (isAscii)
import Data.Either (isRight)
import Data.IORef (newIORef, atomicModifyIORef)
import Data.List ((\\), sort)
import qualified Data.HashMap.Strict as HashMap
import qualified Data.Text as Text
import Data.Foldable (traverse_)
import Data.Text.Encoding as Text
import System.IO.Temp (withSystemTempDirectory)
import Test.Hspec
  ( Spec
  , it
  , describe
  , shouldBe
  , shouldNotBe
  , shouldSatisfy
  , shouldThrow
  , shouldContain
  , expectationFailure
  )
import Foreign.Marshal.Utils (withMany)

import Queue.Topic
  ( Topic
  , Meta(..)
  , ConsumerGroupName(..)
  , PartitionInstance(..)
  , PartitionNumber(..)
  , ReadError(..)
  , withTopic
  )
import qualified Queue.Topic as T
import Queue.Partition (Partition, Position(..), Record(..))
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
  it "1 consumer reads from 1 partition" $
    with (PartitionCount 1) $ \topic -> do
      one : two : _ <- return msgs
      withProducer topic $ \producer -> do
        T.write producer one
        T.write producer two
      T.withConsumer topic group $ \consumer -> do
        Right (one_, Meta _ n1) <- T.read consumer
        n1 `shouldBe` 0
        Record one_ `shouldBe` snd one
        Right (two_, Meta _ n2) <- T.read consumer
        n2 `shouldBe` 1
        Record two_ `shouldBe` snd two

  it "1 consumer reads from 2 partitions" $
    with (PartitionCount 2) $ \topic -> do
      one : two : _ <- return msgs
      withProducer topic $ \producer -> do
        T.write producer one
        T.write producer two
      T.withConsumer topic group $ \consumer -> do
        Right (one_, Meta p1 n1) <- T.read consumer
        Right (two_, Meta p2 n2) <- T.read consumer
        sort
          [ (p1,n1,one_)
          , (p2,n2,two_)
          ]
          `shouldBe`
          [ (0,0,unRecord $ snd one)
          , (1,0,unRecord $ snd two)
          ]

  it "2 producers write to 1 partition" $
    with (PartitionCount 1) $ \topic -> do
      let count = 20
          msgs' = take count msgs
      withProducer topic $ \producer1 ->
        withProducer topic $ \producer2 -> do
          let (one, two) = splitAt (count `div` 2) msgs'
          traverse_ (T.write producer1) one
          traverse_ (T.write producer2) two

      T.withConsumer topic group $ \consumer -> do
        Right xs <- sequenceA <$> replicateM count (T.read consumer)
        fmap fst xs `shouldBe` fmap (unRecord . snd) msgs'

  it "allows consumers without readers" $
    with (PartitionCount 0) $ \topic ->
      T.withConsumer topic group $ \_ -> return ()

  it "reads ordered per partition" $ do
    with (PartitionCount 2) $ \topic -> do
      let count = 20
          msgs' = take count msgs

      withProducer topic $ \producer ->
        traverse_ (T.write producer) msgs'

      T.withConsumer topic group $ \consumer1 ->
        T.withConsumer topic group $ \consumer2 ->
        forM_ [consumer1, consumer2] $ \consumer -> do
          rs <- replicateM (count `div` 2) (T.read consumer)
          -- all succeed
          rs `shouldSatisfy` all isRight
          -- keeps order in which was written
          Right recs <- return $ traverse (fmap $ Record . fst) rs
          let written = fmap snd msgs'
          recs `shouldBe` (written \\ (written \\ recs))

  it "rebalance splits partitions between consumers" $
    with (PartitionCount 2) $ \topic ->
      withProducer topic $ \producer -> do
      var <- newIORef msgs
      let write = do
            msg <- atomicModifyIORef var $ \xs -> (tail xs, head xs)
            T.write producer msg

          read consumer = pnumber <$> T.read consumer

      T.withConsumer topic group $ \consumer1 -> do
        replicateM_ 2 write

        -- first consumer reads from both partitions
        ps <- replicateM 2 (read consumer1)
        ps `shouldContain` [0]
        ps `shouldContain` [1]

        -- add a second consumer
        T.withConsumer topic group $ \consumer2 -> do
          replicateM_ 4 write
          -- now first consumer only reads from one partition
          [p1, p2] <- replicateM 2 (read consumer1)
          p1 `shouldBe` p2

          -- second consumer only reads from other partition
          [p3, p4] <- replicateM 2 (read consumer2)
          p3 `shouldBe` p4

          -- they read from different partitions
          p1 `shouldNotBe` p3

  it "rebalance joins partitions in consumers" $
    with (PartitionCount 2) $ \topic -> do
      withProducer topic $ \producer ->
        traverse_ (T.write producer) $ take 6 msgs

      T.withConsumer topic group $ \consumer1 -> do
        T.withConsumer topic group $ \consumer2 -> do
          p1 <- pnumber <$> T.read consumer1
          p2 <- pnumber <$> T.read consumer2
          -- first and second consumers read from different partitions
          p1 `shouldNotBe` p2
        ps <- fmap pnumber <$> replicateM 5 (T.read consumer1)
        -- now consumer 1 reads from both partitions
        ps `shouldContain` [0]
        ps `shouldContain` [1]

  it "rebalance rewinds to last comitted offset" $
    with (PartitionCount 2) $ \topic -> do
      withProducer topic $ \producer ->
        traverse_ (T.write producer) $ take 6 msgs

      T.withConsumer topic group $ \consumer -> do
        -- read all 6 messages
        replicateM_ 6 $ pnumber <$> T.read consumer
        -- commit the 2nd message from partition 1.
        T.commit consumer (Meta 1 1)

      -- with a new consumer
      T.withConsumer topic group $ \consumer -> do
        -- we should be able to read 4 messages.
        metas <- replicateM 4 $ mmeta <$> T.read consumer
        let metaPartition (Meta p _) = p
            metaOffset (Meta _ o) = o
        sort (metaOffset <$> metas) `shouldBe` [0,1,2,2]
        sort (metaPartition <$> metas) `shouldBe` [0,0,0,1]
  where
    mmeta :: Either ReadError (ByteString, Meta) -> Meta
    mmeta = \case
      Left err -> error (show err)
      Right (_, m) -> m


    pnumber :: Either ReadError (ByteString, Meta) -> PartitionNumber
    pnumber = \case
      Left err -> error (show err)
      Right (_, Meta p _) -> p

    msgs :: [(Int, Record)]
    msgs = zip ([0..] :: [Int]) messages

    group = ConsumerGroupName "test-group"

    withProducer topic f =
      T.withProducer topic (T.modPartitioner fst) (unRecord . snd) f

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

  it "no concurrent opens" $
    withTempPath $ \path ->
      let openError e
            | Just (FilePartition.AlreadyOpen _) <- fromException e = True
            | otherwise = False

          openTwice =
            with path $ \_ ->
            with path $ \_ ->
              expectationFailure "Should not have opened the partition twice"
      in
      openTwice `shouldThrow` openError

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

