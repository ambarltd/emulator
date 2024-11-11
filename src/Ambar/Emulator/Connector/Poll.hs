module Ambar.Emulator.Connector.Poll
   ( Boundaries(..)
   , PollingConnector(..)
   , EntryId(..)
   , connect
   , BoundaryTracker
   , mark
   , boundaries
   , cleanup
   ) where

{-| General polling connector -}

import Control.Concurrent.STM (TVar, atomically, writeTVar, readTVarIO)
import Control.Monad (forM_)
import Data.Aeson (ToJSON, FromJSON, FromJSONKey, ToJSONKey)
import Data.Default (Default)
import Data.Time.Clock.POSIX (POSIXTime, getPOSIXTime)
import Data.Void (Void)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import GHC.Generics (Generic)
import Prettyprinter (Pretty, pretty)

import qualified Ambar.Emulator.Queue.Topic as Topic
import Utils.Delay (Duration, delay, fromDiffTime, toDiffTime)

-- | List of ID ranges to exclude
newtype Boundaries = Boundaries [(EntryId, EntryId)]
   deriving (Show, Eq)

newtype EntryId = EntryId Integer
   deriving (Show)
   deriving newtype (Eq, Enum, Ord, Num, FromJSON, ToJSON, FromJSONKey, ToJSONKey)

instance Pretty EntryId where
   pretty (EntryId n) = pretty n

data PollingConnector entry = PollingConnector
   { c_getId :: entry -> EntryId
   , c_poll :: Boundaries -> IO [entry]  -- ^ run query
   , c_pollingInterval :: Duration
   , c_maxTransactionTime :: Duration
   , c_producer :: Topic.Producer entry
   }

connect :: TVar BoundaryTracker -> PollingConnector entry -> IO Void
connect trackerVar (PollingConnector getId poll interval maxTransTime producer) = do
   loop
   where
   diff = toDiffTime maxTransTime
   loop = do
      before <- getPOSIXTime
      tracker <- readTVarIO trackerVar
      items <- poll (boundaries tracker)
      forM_ items (Topic.write producer)
      atomically $
         writeTVar trackerVar $
         cleanup (before - diff) $
         foldr (mark before . getId) tracker items
      after <- getPOSIXTime
      delay $ max 0 $ interval - fromDiffTime (after - before)
      loop

-- | Boundary tracker for enumerable ids. Takes advantage of ranges.
-- Map by range's low Id
newtype BoundaryTracker = BoundaryTracker (Map EntryId Range)
   deriving (Generic)
   deriving newtype (Semigroup, Monoid, Default)
   deriving anyclass (FromJSON, ToJSON)

data Range = Range
   { _r_low :: (POSIXTime, EntryId)
   , _r_high :: (POSIXTime, EntryId)
   }
   deriving (Generic, Show)
   deriving anyclass (FromJSON, ToJSON)

mark :: POSIXTime -> EntryId -> BoundaryTracker -> BoundaryTracker
mark time el (BoundaryTracker m) = BoundaryTracker $
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

boundaries :: BoundaryTracker -> Boundaries
boundaries (BoundaryTracker m) =
   Boundaries
   [ (low, high)
   | Range (_, low) (_, high) <- Map.elems m
   ]

cleanup :: POSIXTime -> BoundaryTracker -> BoundaryTracker
cleanup bound (BoundaryTracker m) =
   BoundaryTracker $ Map.filter (\range -> highTime range >= safeBound) m
   where
   highTime (Range _ (t_high,_)) = t_high
   -- a safe bound always leaves one element in the BoundaryTracker to
   -- avoid fetching everything from the beginning.
   highestTime = maximum $ highTime <$> Map.elems m
   safeBound =
      if Map.null m
      then bound
      else min bound highestTime




