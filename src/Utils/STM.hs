module Utils.STM
  ( atomicallyNamed
  ) where

import qualified Control.Exception as E
import qualified Control.Concurrent.STM as STM

import Utils.Exception (annotateWith)

atomicallyNamed :: String -> STM.STM a -> IO a
atomicallyNamed msg = annotateWith f . STM.atomically
  where
  f :: E.BlockedIndefinitelyOnSTM -> String
  f _ = msg

data AnnotatedException = AnnotatedException String E.SomeException
  deriving Show

instance E.Exception AnnotatedException where
  displayException (AnnotatedException msg ex) =
    unlines
      [ msg
      , E.displayException ex
      ]

