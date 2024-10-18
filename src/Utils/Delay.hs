module Utils.Delay
  ( Duration(..)
  , delay
  , every
  ) where

import Control.Concurrent (threadDelay)
import Control.Monad (forever)

delay :: Duration -> IO ()
delay = threadDelay . toNano

data Duration
  = Seconds Int
  | Milliseconds Int
  | Nanoseconds Int

toNano :: Duration -> Int
toNano = \case
  Seconds n -> n * 1_000_000
  Milliseconds n -> n * 1_000
  Nanoseconds n -> n

every :: Duration -> IO a -> IO b
every period act = forever $ do
  delay period
  act

