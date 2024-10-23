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
  ) where

import Prelude hiding (init)
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Exception
import Control.Monad

data Initializable a = NotRequested | Initializing | Ready a

-- | A function of the pattern `withResource`
type Initializer a = (forall b. (a -> IO b) -> IO b)

data OnDemand a = OnDemand
  (TVar (Initializable a))
  (TVar Int)

with :: OnDemand a -> (a -> IO b) -> IO b
with (OnDemand var refs) f = bracket acquire release f
  where
    acquire = do
      atomically $ modifyTVar refs succ
      atomically $ do
        r <- readTVar var
        case r of
          NotRequested -> retry
          Initializing -> retry
          Ready v -> return v

    release _ =
      atomically $ modifyTVar refs pred

lazy_ :: Initializer a -> IO (Async (), OnDemand a)
lazy_ init = do
  alive <- newTVarIO True
  refs <- newTVarIO 0
  var  <- newTVarIO NotRequested
  t <- async (initialise var alive refs)
  _ <- mkWeakTVar var $ join $ atomically $ do
      count <- readTVar refs
      if count == 0
        then return $ cancel t
        else do
          modifyTVar alive (const False)
          return (return ())
  return (t, OnDemand var refs)
  where
  initialise var aliveVar refs = do
    -- wait till first 'with'
    atomically $ do
      c <- readTVar refs
      when (c == 0) retry

    init $ \resource -> do
      atomically $ writeTVar var (Ready resource)
      -- wait till last 'with'
      atomically $ do
        alive <- readTVar aliveVar
        c <- readTVar refs
        unless (not alive && c == 0) retry

lazy :: Initializer a -> IO (OnDemand a)
lazy init = snd <$> lazy_ init

-- | A version of lazy where the resource thread is guaranteed to die
-- before returning we return.
withLazy :: Initializer a -> (OnDemand a -> IO b) -> IO b
withLazy init act = do
  (thread, od) <- lazy_ init
  act od `finally` cancel thread
