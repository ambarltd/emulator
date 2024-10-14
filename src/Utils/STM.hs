module Utils.STM
  ( atomicallyNamed
  ) where

import qualified Control.Exception as E
import qualified Control.Concurrent.STM as STM

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
