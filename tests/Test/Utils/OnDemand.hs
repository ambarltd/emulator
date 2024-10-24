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
  , withLazy
  , with
  -- for testing
  , lazy_
  ) where

import Prelude hiding (init)
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import System.Mem.Weak

data OnDemand a = OnDemand (TMVar a) (TVar Int)

with :: OnDemand a -> (a -> IO b) -> IO b
with (OnDemand var refs) = bracket acquire release
  where
  acquire = do
    atomically $ modifyTVar refs succ
    atomically $ readTMVar var

  release _ = atomically $ modifyTVar refs pred

-- | A function of the pattern `withResource`
type Initializer a = (forall b. (a -> IO b) -> IO b)

lazy_ :: Initializer a -> IO (Async (), OnDemand a)
lazy_ init = mdo
  alive <- newTVarIO True -- live state of OnDemand
  refs <- newTVarIO 0     -- resource references
  var  <- newEmptyTMVarIO
  wvar <- mkWeakTMVar var $ join $ atomically $ do
    modifyTVar alive (const False)
    count <- readTVar refs
    return $ when (count == 0) (cancel t)
  t <- async (initialise wvar alive refs)
  link t -- make sure init exceptions are thrown in the main thread.
  return (t, OnDemand var refs)
  where
  initialise wvar aliveVar refsVar = do
    -- wait till first 'with'
    atomically $ do
      refs <- readTVar refsVar
      alive <- readTVar aliveVar
      when (alive && refs == 0) retry

    init $ \resource -> do
      mvar <- deRefWeak wvar
      forM_ mvar $ \var -> do
        atomically $ putTMVar var resource
        -- wait till last 'with'
        atomically $ do
          alive <- readTVar aliveVar
          refs <- readTVar refsVar
          unless (not alive && refs == 0) retry

lazy :: Initializer a -> IO (OnDemand a)
lazy init = snd <$> lazy_ init

-- | A version of lazy that doesn't wait for the garbage collector
-- to finish the resource thread.
-- before returning we return.
withLazy :: Initializer a -> (OnDemand a -> IO b) -> IO b
withLazy init act = do
  (thread, od) <- lazy_ init
  act od `finally` cancel thread
