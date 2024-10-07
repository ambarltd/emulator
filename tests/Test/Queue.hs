module Test.Queue (testQueues) where

import Data.Char (isAscii)
import qualified Data.Text as Text
import Data.Text.Encoding (encodeUtf8)
import System.IO.Temp (withSystemTempDirectory)
import Test.Hspec (shouldBe, Spec, it, describe)

import Queue.Partition (Partition(..), Position(..), Record(..))
import qualified Queue.Partition as P
import qualified Queue.Partition.File as FilePartition

testQueues :: Spec
testQueues  = describe "queue" $ do
  describe "partition" $
    testPartition withFilePartition
  where
  withFilePartition act =
    withSystemTempDirectory "partition-XXXXX" $ \path -> do
    partition <- FilePartition.open path "file-partition"
    act partition

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

testPartition :: Partition a => (forall b. (a -> IO b) -> IO b) -> Spec
testPartition with = do
  it "reads what is written" $ with $ \partition -> do
    one : two : _ <- return messages
    P.write partition one
    P.write partition two
    P.seek partition Beginning $ \reader -> do
      (o1, one_) <- P.read reader
      o1 `shouldBe` 0
      (o2, two_) <- P.read reader
      o2 `shouldBe` 1
      one_ `shouldBe` one
      two_ `shouldBe` two



