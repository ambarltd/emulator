module Utils.Exception
  ( AnnotatedException(..)
  , annotateWith
  , catchAll
  , tryAll
  ) where

import Control.Exception
  ( SomeException(..)
  , SomeAsyncException(..)
  , Exception(..)
  , catch
  , handle
  , displayException
  , throwIO
  )

data AnnotatedException = AnnotatedException String SomeException
  deriving Show

instance Exception AnnotatedException where
  displayException (AnnotatedException msg ex) =
    unlines
      [ msg
      , displayException ex
      ]

annotateWith :: Exception a => (a -> String) -> IO b -> IO b
annotateWith f act = handle g act
  where
  g e = throwIO $ AnnotatedException (f e) (toException e)


-- | Catch all exceptions *except* asynchronous exceptions
-- (technically, children of 'SomeAsyncException').  Catching
-- asynchronous exceptions is almost never what you want to do: it can
-- result in ignoring 'ThreadKilled' which can lead to deadlock.
--
-- Use this instead of the raw 'catch' when catching 'SomeException'.
--
catchAll :: IO a -> (SomeException -> IO a) -> IO a
catchAll action handler =
  action `catch` \ex ->
    case fromException ex of
      Just (_ :: SomeAsyncException) -> throwIO ex
      Nothing -> handler ex

-- | The "try" version of 'catchAll'
tryAll :: IO a -> IO (Either SomeException a)
tryAll action = (Right <$> action) `catchAll` (return . Left)
