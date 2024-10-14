{- | An STM abstraction on top of the Partition Reader -}

module Queue.Partition.STMReader
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
import Data.Maybe (isJust)
import Data.Void (Void)

import Queue.Partition
  ( Partition
  , Offset
  , Record(..)
  , Position(..)
  )
import Utils.STM (atomicallyNamed)

import qualified Queue.Partition as Partition

-- | A single (totally ordered) stream of data from a partition.
data STMReader = STMReader
  { r_worker :: Async Void
  , r_next :: TMVar (Offset, Record) -- ^ take this to get the next element
  , r_expected :: TVar Offset        -- ^ next offset to be read. Change this to seek.
  }

seek :: STMReader -> Offset -> STM ()
seek STMReader{..} offset = do
  next <- STM.readTVar r_expected
  when (next /= offset) $ do
    STM.writeTVar r_expected offset

tryRead :: STMReader -> STM (Maybe (Offset, Record))
tryRead STMReader{..} = do
  mval <- STM.tryTakeTMVar r_next
  when (isJust mval) $ STM.modifyTVar r_expected (+1)
  return mval

destroy :: STMReader -> IO ()
destroy STMReader{..} = Async.cancel r_worker

new
  :: Partition a
  => a
  -> Offset  -- ^ variable we will use to keep track of comitted offsets
  -> IO STMReader
new partition start = do
  reader <- Partition.openReader partition
  expectedVar <- STM.newTVarIO start
  nextVar <- STM.newEmptyTMVarIO

  -- the reader is only controlled by the worker
  let work needle = do
        -- check if seek is needed and if value in nextVar is still valid.
        mseek <- atomicallyNamed "STMReader" $ do
          mval <- STM.tryReadTMVar nextVar
          expected <- STM.readTVar expectedVar
          case mval of
            Nothing ->
              if needle /= expected
              then return $ Just expected
              else return Nothing
            Just (offset, _) ->
              if offset == expected
              then STM.retry -- wait till value is consumed
              else do
                -- there was a seek since last read.
                -- Let's discard the value read and
                -- move the needle to the new position.
                _ <- STM.takeTMVar nextVar
                return $ Just expected

        forM_ mseek $ \pos -> Partition.seek reader (At pos)

        -- block till a value is read
        (offset, record) <- Partition.read reader

        atomicallyNamed "STMReader" $ do
          expected <- STM.readTVar expectedVar
          when (expected == offset ) $ STM.putTMVar nextVar (offset, record)

        work (offset + 1)

      cleanup = do
        Partition.closeReader reader
        atomicallyNamed "STMReader" $ STM.writeTMVar nextVar $ error $ unwords
          [ "reading from destroyed reader" ]

  worker <- Async.async (work 0 `finally` cleanup)
  return STMReader
    { r_worker = worker
    , r_next = nextVar
    , r_expected = expectedVar
    }
