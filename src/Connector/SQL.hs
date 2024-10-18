module Connector.SQL
   ( Boundaries(..)
   , SQLConnector(..)
   , connect
   , BoundaryTracker
   , enumTracker
   ) where

{-| SQL polling connector -}

import Control.Monad (forM_)
import Data.Time.Clock.POSIX (POSIXTime, getPOSIXTime)
import Data.Void (Void)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map

import qualified Queue.Topic as Topic
import Utils.Delay (Duration, delay, fromDiffTime, toDiffTime)

-- | List of ID ranges to exclude
newtype Boundaries id = Boundaries [(id, id)]

data SQLConnector item id = SQLConnector
   { c_getId :: item -> id
   , c_poll :: Boundaries id -> IO [item]  -- ^ run query
   , c_pollingInterval :: Duration
   , c_maxTransactionTime :: Duration
   , c_producer :: Topic.Producer item
   }

-- | Tracks seen ID boundaries
data BoundaryTracker id = forall state. Monoid state => BoundaryTracker
   { _t_mark :: POSIXTime -> id -> state -> state
   , _t_boundary :: POSIXTime -> state -> Boundaries id
   -- remove all relevant entries before given time.
   , _t_cleanup :: POSIXTime -> state -> state
   }

connect :: BoundaryTracker id -> SQLConnector item id -> IO Void
connect
   (BoundaryTracker mark boundary cleanup)
   (SQLConnector getId poll interval maxTransTime producer) = do
   now <- getPOSIXTime
   go now mempty
   where
   diff = toDiffTime maxTransTime
   go before tracker = do
      now <- getPOSIXTime
      delay $ max 0 $ interval - fromDiffTime (now - before)
      items <- poll (boundary now tracker)
      forM_ items (Topic.write producer)
      let tracker' = cleanup (now - diff) $ foldr (mark now . getId) tracker items
      go now tracker'

-- | Boundary tracker for enumerable ids. Takes advantage of ranges.
newtype EnumTracker a = EnumTracker (Map a (Range a)) -- map by range low Id
   deriving newtype (Semigroup, Monoid)

data Range a = Range
   { _r_low :: (POSIXTime, a)
   , _r_high :: (POSIXTime, a)
   }

enumTracker :: (Ord a, Enum a) => BoundaryTracker a
enumTracker = BoundaryTracker mark boundary cleanup
   where
   mark :: (Ord a, Enum a) => POSIXTime -> a -> EnumTracker a -> EnumTracker a
   mark time el (EnumTracker m) = EnumTracker $
      case Map.lookupLE el m of
        Nothing -> checkAbove
        Just (_, Range (low_t, low) (_, high))
            -- already in range
            | el <= high -> m
            -- extends range upwards
            | el == succ high ->
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

   boundary :: Ord a => POSIXTime -> EnumTracker a -> Boundaries a
   boundary time (EnumTracker m) =
      Boundaries
      [ (low, high)
      | Range (_, low) (t_high, high) <- Map.elems m
      , t_high > time
      ]

   cleanup :: POSIXTime -> EnumTracker a -> EnumTracker a
   cleanup bound (EnumTracker m) =
      EnumTracker $ Map.filter (\(Range _ (t_high,_)) -> t_high > bound) m


