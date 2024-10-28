module Utils.Async where

import Control.Concurrent.Async (withAsync, wait, waitEither)

-- version of withAsync which throws if left throws
withAsyncThrow :: IO a -> IO b -> IO b
withAsyncThrow left right =
  withAsync right $ \r ->
  withAsync left $ \l -> do
  e <- waitEither l r
  case e of
    Left _ -> wait r
    Right v -> return v
