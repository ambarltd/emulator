module Utils.Exception
  ( AnnotatedException(..)
  , annotateWith
  ) where

import qualified Control.Exception as E

data AnnotatedException = AnnotatedException String E.SomeException
  deriving Show

instance E.Exception AnnotatedException where
  displayException (AnnotatedException msg ex) =
    unlines
      [ msg
      , E.displayException ex
      ]

annotateWith :: E.Exception a => (a -> String) -> IO b -> IO b
annotateWith f act = E.handle g act
  where
  g e = E.throwIO $ AnnotatedException (f e) (E.toException e)
