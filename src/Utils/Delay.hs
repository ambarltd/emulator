module Utils.Delay
  ( Duration
  , delay
  , every
  , timeout
  , hang
  , seconds
  , millis
  , micros
  , fromDiffTime
  , toDiffTime
  ) where

import Control.Concurrent (threadDelay)
import Control.Monad (forever)
import Data.Time.Clock (NominalDiffTime, nominalDiffTimeToSeconds, secondsToNominalDiffTime)
import Data.Void (Void)
import qualified System.Timeout as T

delay :: Duration -> IO ()
delay = threadDelay . toMicro

newtype Duration = Microseconds { toMicro :: Int }
  deriving newtype (Show, Eq, Ord, Num)

seconds :: Int -> Duration
seconds n = Microseconds $ n * 1_000_000

millis :: Int -> Duration
millis n = Microseconds $ n * 1_000

micros :: Int -> Duration
micros = Microseconds

fromDiffTime :: NominalDiffTime -> Duration
fromDiffTime n = Microseconds $ ceiling $ nominalDiffTimeToSeconds n * 1_000_000

toDiffTime :: Duration -> NominalDiffTime
toDiffTime (Microseconds n) =
  secondsToNominalDiffTime $ fromIntegral n / 1_000_000

every :: Duration -> IO a -> IO Void
every period act = forever $ do
  delay period
  act

-- | Version of timeout that throws on timeout
timeout :: Duration -> IO a -> IO a
timeout time act = do
  r <- T.timeout (toMicro time) act
  case r of
    Nothing -> error "timed out"
    Just v -> return v

-- | Never return
hang :: IO a
hang = forever $ threadDelay maxBound
