module Utils.Async where

import Control.Concurrent.Async (withAsync, withAsyncBound, wait, waitEither)

-- version of withAsync which throws if left throws
withAsyncThrow :: IO a -> IO b -> IO b
withAsyncThrow left right =
  withAsync right $ \r ->
  withAsync left $ \l -> do
  e <- waitEither l r
  case e of
    Left _ -> wait r
    Right v -> return v

-- version of withAsyncThrow using a bound thread for the background task.
withAsyncBoundThrow :: IO a -> IO b -> IO b
withAsyncBoundThrow left right =
  withAsyncBound right $ \r ->
  withAsync left $ \l -> do
  e <- waitEither l r
  case e of
    Left _ -> wait r
    Right v -> return v
