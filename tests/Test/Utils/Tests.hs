module Test.Utils.Tests (testUtils) where

import Test.Hspec
  ( Spec
  , it
  , describe
  , shouldBe
  , expectationFailure
  )
import Test.Hspec.Expectations.Contrib (annotate)

import Control.Concurrent.Async
import Control.Monad
import Control.Exception

import Control.Concurrent.STM (newTVarIO, atomically, readTVarIO, modifyTVar)
import Data.Maybe (isJust)
import qualified Test.Utils.OnDemand as OnDemand
import System.Mem (performMajorGC)

import Utils.Delay (delay, millis)

testUtils :: Spec
testUtils = do
  describe "OnDemand" $ do
    describe "lazy" $ do
      it "doesn't instantiate if `with` is not called" $
        withFun $ \create inits ends -> do
        _ <- OnDemand.lazy create
        collectGarbage
        expectM "initialisations" inits  0
        expectM "finalisations" ends 0

      it "instantiates if `with` is called" $
        withFun $ \create inits ends -> do
        d <- OnDemand.lazy create
        OnDemand.with d return
        collectGarbage
        expectM "initialisations" inits 1
        expectM "finalisations" ends 1

      it "runs finalizers once at the end" $
        withFun $ \create _ ends -> do
        d <- OnDemand.lazy create
        OnDemand.with d return
        collectGarbage
        expectM "finalisations" ends 0
        OnDemand.with d return
        collectGarbage
        expectM "finalisations" ends 0
        OnDemand.with d return
        collectGarbage
        expectM "finalisations" ends 1

      it "doesn't run finalizers if ref is garbage but we are still inside `with`" $
        withFun $ \create _ ends -> do
        d <- OnDemand.lazy create
        OnDemand.with d $ \() -> do
          collectGarbage
          expectM "finalisations" ends 0

      it "no thread leak without initialisation" $
        withFun $ \create _ _ -> do
        (t, _) <- OnDemand.lazy_ create
        collectGarbage
        r <- poll t
        annotate "thread stopped" $ isJust r `shouldBe` True

      it "no thread leak with initialisation" $
        withFun $ \create _ _ -> do
        (t, d) <- OnDemand.lazy_ create
        OnDemand.with d return
        collectGarbage
        r <- poll t
        annotate "thread stopped" $ isJust r `shouldBe` True

      it "throws init exception in all demanding threads" $ do
        let err = "Fake Error"
        r <- OnDemand.lazy $ \_ -> throwIO (ErrorCall err)

        [t1, t2] <- replicateM 2 $ async $
          OnDemand.with r return

        delay (millis 10)
        forM_ (zip @Int [0..] [t1, t2]) $ \(n, t) -> do
          outcome <- poll t
          annotate ("thread " <> show n) $
            case outcome  of
              Just (Left e)
                | Just (ErrorCall msg) <- fromException e
                , msg == err -> return ()
                | otherwise ->
                  expectationFailure (show e)
              Just (Right _) ->
                  expectationFailure "unexpected success"
              Nothing -> do
                  cancel t
                  expectationFailure "thread not finished"

    describe "withLazy" $ do
      it "ensures cleanup is run before returning" $
        withFun $ \create _ ends -> do
        () <- OnDemand.withLazy create $ \d ->
          OnDemand.with d return
        expectM "finalisations" ends 1
    where
    expectM msg m val = do
      r <- m
      annotate msg $ r `shouldBe` val

collectGarbage :: IO ()
collectGarbage = do
  -- once we move to ghc-9.10 we can use `performBlockingMajorGC`
  -- and do away with the threadDelay.
  performMajorGC
  delay (millis 10)

type F a =
  (forall b. (() -> IO b) -> IO b)
  -> IO Int
  -> IO Int
  -> IO a

withFun :: F a -> IO a
withFun f = do
  startVar <- newTVarIO 0
  endVar <- newTVarIO 0
  let create :: forall a. (() -> IO a) -> IO a
      create = bracket
        (atomically $ modifyTVar startVar (+ 1))
        (\_ -> atomically $ modifyTVar endVar (+ 1))
      initialised = readTVarIO startVar
      finalised = readTVarIO endVar
  f create initialised finalised
