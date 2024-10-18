module Utils.Thread
  ( Delay(..)
  , delay
  ) where

import Control.Concurrent (threadDelay)

delay :: Delay -> IO ()
delay = threadDelay . toNano

data Delay
  = Seconds Int
  | Milliseconds Int
  | Nanoseconds Int

toNano :: Delay -> Int
toNano = \case
  Seconds n -> n * 1_000_000
  Milliseconds n -> n * 1_000
  Nanoseconds n -> n
