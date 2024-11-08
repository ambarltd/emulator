module Test.Warden (testWarden) where

import Control.Concurrent
import qualified Control.Concurrent.Async as Async
import Control.Exception
import Control.Monad
import Test.Hspec
  ( Spec
  , it
  , describe
  , shouldBe
  , shouldThrow
  )
import qualified Utils.Warden as Warden
import Utils.Delay

testWarden :: Spec
testWarden = describe "Warden" $ do
  it "kills spawned on normal termination" $
    deadline (seconds 1) $ do
    v <- newQSem 0
    Warden.withWarden $ \w ->
      Warden.spawn_ w $ hang `finally` signalQSem v
    r <- waitQSem v
    r `shouldBe` ()

  it "kills spawned on forced termination" $
    deadline (seconds 1) $ do
    started <- newQSem 0
    done <- newQSem 0

    let run = Warden.withWarden $ \w -> do
          Warden.spawn_ w $ do
            signalQSem started
            hang `finally` signalQSem done
          hang
    r <- Async.withAsync run $ \task -> do
      waitQSem started
      Async.cancel task
      waitQSem done
    r `shouldBe` ()

  it "spawn doesn't propagate exceptions" $ do
    r <- Warden.withWarden $ \w -> do
      task <- Warden.spawn w $ throwIO $ ErrorCall "self-kill"
      void $ Async.waitCatch task
    r `shouldBe` ()


  it "spawnLinked propagates exceptions" $ do
    let err = "self-kill"
        childError e
          | Just (ErrorCall msg) <- fromException e = msg == err
          | otherwise = False
        run = Warden.withWarden $ \w -> do
          void $ Warden.spawnLinked w $ throwIO $ ErrorCall err
          hang

    run `shouldThrow`childError

  it "spawnLinked doesn't propagate AsyncCancelled" $ do
    r <- Warden.withWarden $ \w -> do
      started <- newQSem 0
      task <- Warden.spawn w $ signalQSem started *> hang
      waitQSem started
      Async.cancel task
      void $ Async.waitCatch task

    r `shouldBe` ()
