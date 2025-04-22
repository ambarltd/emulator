module Ambar.Emulator.Queue.Partition
  ( Partition(..)
  , Position(..)
  , Offset(..)
  , Record(..)
  , Count(..)
  , withReader
  ) where

import Control.Exception (bracket)
import Data.Aeson (FromJSON, ToJSON, FromJSONKey, ToJSONKey)
import Data.ByteString (ByteString)

data Position
  = At Offset
  | Beginning
  | End
  deriving (Eq, Ord)

-- | A reader's position in the records sequence.
newtype Offset = Offset { unOffset :: Int }
  deriving Show
  deriving newtype (Eq, Ord, Enum, Integral, Real, Num, FromJSON, ToJSON, FromJSONKey, ToJSONKey)

-- The total number of records in a partition.
newtype Count = Count Int
  deriving (Show)
  deriving newtype (Eq, Ord, Enum, Integral, Real, Num, ToJSON)

newtype Record = Record { unRecord :: ByteString }
  deriving newtype (Eq, Ord, Show)

-- | A Partition contains a sequence of records.
class Partition a where
  type Reader a = b | b -> a

  openReader :: a -> IO (Reader a)

  closeReader :: Reader a -> IO ()

  seek :: Reader a -> Position -> IO ()
  -- | Reads one record and advances the Reader.
  -- Blocks if there are no more records.
  read :: Reader a -> IO (Offset, Record)

  write :: a -> Record -> IO ()

withReader :: Partition a => a -> Position -> (Reader a -> IO b) -> IO b
withReader partition position act =
  bracket (openReader partition) closeReader $ \reader -> do
    seek reader position
    act reader

