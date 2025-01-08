{- | An STM abstraction on top of the Partition Reader -}

module Ambar.Emulator.Queue.Partition.STMReader
  ( STMReader
  , new
  , seek
  , tryRead
  , destroy
  ) where

import Prelude hiding (read)

import qualified Control.Concurrent.Async as Async
import Control.Concurrent.Async (Async)
import qualified Control.Concurrent.STM as STM
import Control.Concurrent.STM
  ( STM
  , TVar
  , TMVar
  )
import Control.Exception (finally)
import Control.Monad (forM_, when)

import Ambar.Emulator.Queue.Partition
  ( Partition
  , Offset
  , Record(..)
  , Position(..)
  )
import Util.STM (atomicallyNamed)
import qualified Util.Warden as Warden
import Util.Warden (Warden)

import qualified Ambar.Emulator.Queue.Partition as Partition

-- | A single (totally ordered) stream of data from a partition.
data STMReader = STMReader
  { r_worker :: Async ()
  , r_next :: TMVar (Next (Offset, Record)) -- ^ take this to get the next element.
  , r_expected :: TVar Offset               -- ^ next offset to be read. Change this to seek.
  }

data Next a
  = Next a
  | ClosedReader

seek :: STMReader -> Offset -> STM ()
seek STMReader{..} offset = do
  next <- STM.readTVar r_expected
  when (next /= offset) $ do
    STM.writeTVar r_expected offset

tryRead :: STMReader -> STM (Maybe (Offset, Record))
tryRead STMReader{..} = do
  enext <- STM.tryTakeTMVar r_next
  case enext of
    Just (Next r) -> do
      STM.modifyTVar r_expected (+1)
      return (Just r)
    Just ClosedReader -> return Nothing
    Nothing -> return Nothing

destroy :: STMReader -> IO ()
destroy STMReader{..} = Async.cancel r_worker

new
  :: Partition a
  => Warden
  -> a
  -> Offset  -- ^ variable we will use to keep track of comitted offsets
  -> IO STMReader
new warden partition start = do
  reader <- Partition.openReader partition
  expectedVar <- STM.newTVarIO start
  nextVar <- STM.newEmptyTMVarIO :: IO (TMVar (Next (Offset, Record)))

  -- the reader is only controlled by the worker
  let work needle = do
        -- check if seek is needed and if value in nextVar is still valid.
        nseek <- atomicallyNamed "STMReader" $ do
          mval <- STM.tryReadTMVar nextVar
          expected <- STM.readTVar expectedVar
          case mval of
            Nothing -> return $ Next $
              if needle /= expected
              then Just expected
              else Nothing
            Just (Next (offset, _)) ->
              if offset == expected
              then STM.retry -- wait till value is consumed
              else do
                -- there was a seek since last read.
                -- Let's discard the value read and
                -- move the needle to the new position.
                _ <- STM.takeTMVar nextVar
                return $ Next $ Just expected
            Just ClosedReader -> return ClosedReader

        case nseek of
          ClosedReader -> return ()
          Next mseek -> do
            forM_ mseek $ \pos -> Partition.seek reader (At pos)

            -- block till a value is read
            (offset, record) <- Partition.read reader

            atomicallyNamed "STMReader" $ do
              expected <- STM.readTVar expectedVar
              when (expected == offset) $ STM.putTMVar nextVar (Next (offset, record))

            work (offset + 1)

      cleanup = do
        Partition.closeReader reader
        atomicallyNamed "STMReader" $ STM.writeTMVar nextVar ClosedReader

  -- TODO: Errors from this worker are swalloed.
  -- We MUST implement something to make the whole queue seize-up.
  worker <- Warden.spawnLinked warden (work 0 `finally` cleanup)
  return STMReader
    { r_worker = worker
    , r_next = nextVar
    , r_expected = expectedVar
    }
