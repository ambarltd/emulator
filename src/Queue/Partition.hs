module Queue.Partition
  ( Partition(..)
  , Position(..)
  , Offset(..)
  , Record(..)
  ) where

import Data.ByteString (ByteString)

data Position
  = At Offset
  | Beginning
  | End

newtype Offset = Offset { unOffset :: Int }
  deriving Show
  deriving newtype (Num, Eq, Ord)

newtype Record = Record ByteString
  deriving newtype (Eq, Ord, Show)

-- | A Partition contains a sequence of records.
class Partition a where
  type Reader a = b | b -> a
  seek :: a -> Position -> (Reader a -> IO b) -> IO b
  -- | Reads one record and advances the Reader.
  -- Blocks if there are no more records.
  read :: Reader a -> IO (Offset, Record)

  getOffset :: Reader a -> IO Offset

  write :: a -> Record -> IO ()
