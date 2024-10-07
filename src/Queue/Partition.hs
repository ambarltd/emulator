module Queue.Partition where

import Data.ByteString (ByteString)

data Position
  = At Index
  | Beginning
  | End

newtype Index = Index Int
  deriving Show
  deriving newtype (Num, Eq, Ord)

newtype Record = Record ByteString

-- | A Partition contains a sequence of records.
class Partition a where
  type Reader a = b | b -> a
  seek :: Position -> a -> IO (Reader a)
  -- | Reads one record and advances the Reader.
  -- Blocks if there are no more records.
  read :: Reader a -> IO Record

  getIndex :: Reader a -> IO Index

  write :: Record -> a -> IO ()
