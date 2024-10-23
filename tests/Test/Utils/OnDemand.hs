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
  OnDemand.lazy withResource $ \onDemand -> do

    -- instance is initialised
    OnDemand.with onDemand $ \resource -> ...

    -- instance is reused
    OnDemand.with onDemand $ \resource -> ...
    OnDemand.with onDemand $ \resource -> ...

-}
module Test.Utils.OnDemand
  ( OnDemand
  , lazy
  , lazyW
  , with
  ) where

import Prelude hiding (init)
import Control.Concurrent.Async (async, withAsync)
import Control.Concurrent.STM
import Control.Exception
import Control.Monad (when)

data Initializable a = NotRequested | Initializing | Ready a

-- | A function of the pattern `withResource`
type Initializer a = (forall b. (a -> IO b) -> IO b)

data OnDemand a = OnDemand
  (TVar (Initializable a))
  (TVar Int)

with :: OnDemand a -> (a -> IO b) -> IO b
with (OnDemand var count) f = bracket acquire release f
  where
    acquire = do
      atomically $ modifyTVar count succ
      atomically $ do
        r <- readTVar var
        case r of
          NotRequested -> retry
          Initializing -> retry
          Ready v -> return v

    release _ =
      atomically $ modifyTVar count pred

lazy :: Initializer a -> IO (OnDemand a)
lazy init = do
  count <- newTVarIO 1
  var  <- newTVarIO NotRequested
  _    <- mkWeakTVar var (sub count)
  _ <- async (initialise var count)
  return (OnDemand var count)
  where
  sub count =
    atomically $ modifyTVar count pred

  initialise var count = do
    -- wait till first 'with'
    atomically $ do
      c <- readTVar count
      when (c < 2) retry
      writeTVar var Initializing

    init $ \resource -> do
      atomically $ writeTVar var (Ready resource)
      -- wait till last 'with'
      atomically $ do
        c <- readTVar count
        when (c > 0) retry

lazyW :: Initializer a -> (OnDemand a -> IO b) -> IO b
lazyW init act = do
  count <- newTVarIO 1
  var  <- newTVarIO NotRequested
  _    <- mkWeakTVar var (sub count)
  withAsync (initialise var count) $ \_ ->
   act (OnDemand var count)
  where
  sub count =
    atomically $ modifyTVar count pred

  initialise var count = do
    -- wait till first 'with'
    atomically $ do
      c <- readTVar count
      when (c < 2) retry
      writeTVar var Initializing

    init $ \resource -> do
      atomically $ writeTVar var (Ready resource)
      -- wait till last 'with'
      atomically $ do
        c <- readTVar count
        when (c > 0) retry
