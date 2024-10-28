module Lite.Connector.Poll
   ( Boundaries(..)
   , PollingConnector(..)
   , connect
   , BoundaryTracker(..)
   , rangeTracker
   ) where

{-| General polling connector -}

import Control.Monad (forM_)
import Data.Time.Clock.POSIX (POSIXTime, getPOSIXTime)
import Data.Void (Void)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map

import qualified Lite.Queue.Topic as Topic
import Utils.Delay (Duration, delay, fromDiffTime, toDiffTime)

-- | List of ID ranges to exclude
newtype Boundaries id = Boundaries [(id, id)]
   deriving (Show, Eq)

data PollingConnector item id = PollingConnector
   { c_getId :: item -> id
   , c_poll :: Boundaries id -> IO [item]  -- ^ run query
   , c_pollingInterval :: Duration
   , c_maxTransactionTime :: Duration
   , c_producer :: Topic.Producer item
   }

-- | Tracks seen ID boundaries
data BoundaryTracker id = forall state. Monoid state => BoundaryTracker
   { t_mark :: POSIXTime -> id -> state -> state
   , t_boundaries :: state -> Boundaries id
   -- remove all relevant entries before given time.
   , t_cleanup :: POSIXTime -> state -> state
   }

connect :: BoundaryTracker id -> PollingConnector item id -> IO Void
connect
   (BoundaryTracker mark boundaries cleanup)
   (PollingConnector getId poll interval maxTransTime producer) = do
   now <- getPOSIXTime
   go now mempty
   where
   diff = toDiffTime maxTransTime
   go before tracker_ = do
      now <- getPOSIXTime
      delay $ max 0 $ interval - fromDiffTime (now - before)
      let tracker = cleanup (now - diff) tracker_
      items <- poll (boundaries tracker)
      forM_ items (Topic.write producer)
      go now $ foldr (mark now . getId) tracker items

-- | Boundary tracker for enumerable ids. Takes advantage of ranges.
newtype EnumTracker a = EnumTracker (Map a (Range a)) -- map by range low Id
   deriving newtype (Semigroup, Monoid)

data Range a = Range
   { _r_low :: (POSIXTime, a)
   , _r_high :: (POSIXTime, a)
   }
   deriving Show

-- | Track id ranges.
rangeTracker :: (Show a, Ord a, Enum a) => BoundaryTracker a
rangeTracker = BoundaryTracker mark boundaries cleanup
   where
   mark :: (Ord a, Enum a) => POSIXTime -> a -> EnumTracker a -> EnumTracker a
   mark time el (EnumTracker m) = EnumTracker $
      case Map.lookupLE el m of
        Nothing -> checkAbove
        Just (_, Range (low_t, low) (_, high))
            -- already in range
            | el <= high -> m
            | el == succ high ->
               case Map.lookup (succ el) m of
                  Just (Range _ h) ->
                     -- join ranges
                     Map.insert low (Range (low_t, low) h) $
                     Map.delete (succ el) m
                  Nothing ->
                     -- extend range upwards
                     Map.insert low (Range (low_t, low) (time , el)) m
            | otherwise -> checkAbove
      where
      checkAbove =
         case Map.lookupGT el m of
            Nothing ->
               -- disjoint range
               Map.insert el (Range (time, el) (time, el)) m
            Just (_, Range (_, low) (high_t, high))
               -- extends range downwards
               | el == pred low ->
                  Map.insert el (Range (time, el) (high_t, high)) $
                  Map.delete low m
               -- disjoint range
               | otherwise ->
                  Map.insert el (Range (time, el) (time, el)) m

   boundaries :: Ord a => EnumTracker a -> Boundaries a
   boundaries (EnumTracker m) =
      Boundaries
      [ (low, high)
      | Range (_, low) (_, high) <- Map.elems m
      ]

   cleanup :: Show a => POSIXTime -> EnumTracker a -> EnumTracker a
   cleanup bound (EnumTracker m) =
      EnumTracker $ Map.filter (\(Range _ (t_high,_)) -> t_high > bound) m


