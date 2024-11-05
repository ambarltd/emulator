module Utils.Delay
  ( Duration
  , delay
  , every
  , seconds
  , millis
  , nanos
  , fromDiffTime
  , toDiffTime
  ) where

import Control.Concurrent (threadDelay)
import Control.Monad (forever)
import Data.Time.Clock (NominalDiffTime, nominalDiffTimeToSeconds, secondsToNominalDiffTime)
import Data.Void (Void)

delay :: Duration -> IO ()
delay = threadDelay . toNano

newtype Duration = Nanoseconds { toNano :: Int }
  deriving newtype (Show, Eq, Ord, Num)

seconds :: Int -> Duration
seconds n = Nanoseconds $ n * 1_000_000

millis :: Int -> Duration
millis n = Nanoseconds $ n * 1_000

nanos :: Int -> Duration
nanos = Nanoseconds

fromDiffTime :: NominalDiffTime -> Duration
fromDiffTime n = Nanoseconds $ ceiling $ nominalDiffTimeToSeconds n * 1_000_000

toDiffTime :: Duration -> NominalDiffTime
toDiffTime (Nanoseconds n) =
  secondsToNominalDiffTime $ fromIntegral n / 1_000_000

every :: Duration -> IO a -> IO Void
every period act = forever $ do
  delay period
  act

