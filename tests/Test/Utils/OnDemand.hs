{-# LANGUAGE RecursiveDo #-}
{- |
A module that allows you to initialise a resource on demand and reutilise it
across different invocations.

If you have a function such as

  withResource :: (Resource -> IO b) -> IO b

It will initialise the resource on every invocation. To share an instance
across invocations one would have to wrap all invocations in a single
`withResource`, but that would cause the resource to be initialised regardless
of whether it is used or not.

This module allows on-demand initialisation and instance sharing by using the
following pattern:

  -- nothing happens
  onDemand <- OnDemand.lazy withResource

  -- instance is initialised
  resource <- OnDemand.get onDemand

  -- instance is reused
  resource <- OnDemand.get onDemand
  resource <- OnDemand.get onDemand

-}
module Test.Utils.OnDemand
  ( OnDemand
  , lazy
  , get
  ) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (Async, async, cancel)
import Control.Concurrent.STM (TVar, throwSTM, atomically, readTVar, writeTVar, newTVarIO, retry)
import Control.Exception (ErrorCall(..), bracket, onException, bracketOnError)
import Control.Monad (forever, forM_)
import Data.Void (Void)

data Initializable a = NotRequested | Initializing | Ready a

-- | A reference to a thread that runs forever
type LongRunning = Async Void

-- | A function of the pattern `withResource`
type Initializer a = (forall b. (a -> IO b) -> IO b)

data OnDemand a = OnDemand
  (Initializer a)
  (TVar (Initializable (LongRunning, a)))

get :: OnDemand a -> IO a
get (OnDemand with var) =
  bracketOnError acquire release initialize
  where
    initialize mval =
      case mval of
        Just val -> return val
        Nothing -> mdo
          t <- async
              $ (`onException` release Nothing)
              $ with $ \resource -> do
                atomically $ writeTVar var (Ready (t, resource))
                forever $ threadDelay 1000000
          getResource

    -- wait till ready
    getResource = atomically $ do
      r <- readTVar var
      case r of
        Initializing -> retry
        Ready (_, resource) -> return resource
        NotRequested ->
          throwSTM $ ErrorCall "resource initialization failure"

    acquire = atomically $ do
      r <- readTVar var
      case r of
        NotRequested -> do
          writeTVar var Initializing
          return Nothing
        Initializing -> retry
        Ready (_, x) -> return (Just x)

    release mval = do
      case mval of
        -- unable to initialise. Reset var.
        Nothing -> atomically $ writeTVar var NotRequested
        -- was initialised but received some exception. Nothing to do.
        Just _ -> return ()

lazy :: Initializer a -> (OnDemand a -> IO b) -> IO b
lazy with act =
  bracket acquire release $ \var ->
    act (OnDemand with var)
  where
  acquire = newTVarIO NotRequested

  -- kill long-running thread
  release var = do
    putStrLn "releasing"
    mt <- atomically $ do
      state <- readTVar var
      case state of
        Ready (t, _) -> return (Just t)
        _ -> return Nothing
    forM_ mt cancel



