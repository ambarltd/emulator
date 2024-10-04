module Control.Concurrent.Mutex
  ( Mutex
  , newMutex
  , withMutex
  )
  where

import Control.Concurrent (MVar, newMVar, withMVar)

newtype Mutex = Mutex (MVar ())

newMutex :: IO Mutex
newMutex = Mutex <$> newMVar ()

withMutex :: Mutex -> IO a -> IO a
withMutex (Mutex var) act = do withMVar var (const act)

