module Connector.SQL where

{-| SQL polling connector -}

import Control.Monad (forM_)
import Data.Time.Clock.POSIX (POSIXTime, getPOSIXTime)
import Data.Void (Void)

import qualified Queue.Topic as Topic
import Utils.Delay (Duration, delay, fromDiffTime)

data Boundaries id = Boundaries
  { b_greater :: [id]
  , b_lower   :: [id]
  , b_exclude :: [id]
  }

-- | Tracks the id boundaries considering that IDs can only
-- be committed a limited amount of time after a higher ID
-- has been committed.
class BoundaryTracker a where
   type Tracker a = b | b -> a
   mark     :: POSIXTime -> a -> Tracker a -> Tracker a
   boundary :: POSIXTime -> Tracker a -> Boundaries a

data SQLConnector item id = SQLConnector
   { c_getId :: item -> id
   , c_poll :: Boundaries id -> IO [item]  -- ^ run query
   , c_pollingInterval :: Duration
   , c_producer :: Topic.Producer item
   }

connect :: BoundaryTracker id => Tracker id -> SQLConnector item id -> IO Void
connect tracker_ (SQLConnector getId poll interval producer) = do
   now <- getPOSIXTime
   go now tracker_
   where
   go before tracker = do
      now <- getPOSIXTime
      delay $ max 0 $ interval - fromDiffTime (now - before)
      items <- poll (boundary now tracker)
      forM_ items (Topic.write producer)
      let tracker' = foldr (mark now . getId) tracker items
      go now tracker'
