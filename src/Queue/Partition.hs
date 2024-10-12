module Queue.Partition
  ( Partition(..)
  , Position(..)
  , Offset(..)
  , Record(..)
  , withReader
  , atomicallyNamed
  ) where

import qualified Control.Exception as E
import qualified Control.Concurrent.STM as STM

import Control.Exception (bracket)
import Data.ByteString (ByteString)

data Position
  = At Offset
  | Beginning
  | End
  deriving (Eq, Ord)

newtype Offset = Offset { unOffset :: Int }
  deriving Show
  deriving newtype (Eq, Ord, Enum, Integral, Real, Num)

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

  getOffset :: Reader a -> IO Offset

  write :: a -> Record -> IO ()

withReader :: Partition a => a -> Position -> (Reader a -> IO b) -> IO b
withReader partition position act =
  bracket (openReader partition) closeReader $ \reader -> do
    seek reader position
    act reader

atomicallyNamed :: String -> STM.STM a -> IO a
atomicallyNamed msg = E.handle f . STM.atomically
  where
  f :: E.BlockedIndefinitelyOnSTM -> IO a
  f = E.throwIO . AnnotatedException msg . E.toException

data AnnotatedException = AnnotatedException String E.SomeException
  deriving Show

instance E.Exception AnnotatedException where
  displayException (AnnotatedException msg ex) =
    unlines
      [ msg
      , E.displayException ex
      ]

