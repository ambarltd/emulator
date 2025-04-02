module Ambar.Emulator.Connector.Poll
   ( Boundaries(..)
   , PollingConnector(..)
   , EntryId(..)
   , connect
   , BoundaryTracker
   , mark
   , boundaries
   , compact
   , Stream
   , streamed
   , PollingInterval(..)
   ) where

{-| General polling connector -}

import Control.Monad (foldM)
import Control.Concurrent.STM (TVar, atomically, readTVarIO, modifyTVar)
import Data.Aeson (ToJSON, FromJSON, FromJSONKey, ToJSONKey, toJSON)
import Data.Default (Default(..))
import qualified Data.Aeson as Aeson
import Data.Time.Clock.POSIX (POSIXTime, getPOSIXTime)
import Data.Time.Clock (nominalDiffTimeToSeconds, secondsToNominalDiffTime)
import Data.Void (Void)
import Data.Maybe (fromMaybe)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import GHC.Generics (Generic)
import Prettyprinter (Pretty, pretty)

import qualified Ambar.Emulator.Queue.Topic as Topic
import Util.Delay (Duration, delay, fromDiffTime, toDiffTime)

-- | List of ID ranges to exclude
newtype Boundaries = Boundaries [(EntryId, EntryId)]
   deriving (Show, Eq)

newtype EntryId = EntryId Integer
   deriving (Show)
   deriving newtype (Eq, Enum, Ord, Num, FromJSON, ToJSON, FromJSONKey, ToJSONKey)

instance Pretty EntryId where
   pretty (EntryId n) = pretty n

-- | Maximum interval between polling requests
-- Encoded and decoded as seconds in JSON
newtype PollingInterval = PollingInterval Duration
   deriving (Show)
   deriving newtype (Eq)

instance FromJSON PollingInterval where
  parseJSON value = parse <$> Aeson.parseJSON value
    where
    parse = PollingInterval . fromDiffTime . secondsToNominalDiffTime

instance ToJSON PollingInterval where
  toJSON (PollingInterval duration) =
    Aeson.toJSON $ nominalDiffTimeToSeconds $ toDiffTime duration

data PollingConnector entry = PollingConnector
   { c_getId :: entry -> EntryId
   , c_poll :: Boundaries -> Stream entry
      -- ^ run query, optionally streaming results.
   , c_pollingInterval :: PollingInterval
   , c_maxTransactionTime :: Duration
   , c_producer :: Topic.Producer entry
   }

type Stream a = forall b. b -> (b -> a -> IO b) -> IO b

-- | Take a batched computation and stream its results.
streamed :: IO [a] -> Stream a
streamed act acc f = do
   xs <- act
   foldM f acc xs

connect :: TVar BoundaryTracker -> PollingConnector entry -> IO Void
connect trackerVar (PollingConnector getId poll (PollingInterval interval) maxTransTime producer) = do
   loop
   where
   diff = toDiffTime maxTransTime
   loop = do
      before <- getPOSIXTime
      tracker <- readTVarIO trackerVar
      poll (boundaries tracker) () $ \() item -> do
        Topic.write producer item
        atomically $ modifyTVar trackerVar $ mark before (getId item)
      atomically $ modifyTVar trackerVar $ compact (before - diff)
      after <- getPOSIXTime
      delay $ max 0 $ interval - fromDiffTime (after - before)
      loop

-- | Boundary tracker for enumerable ids. Takes advantage of ranges.
data BoundaryTracker
  = EmptyTracker
  | BoundaryTracker
     { b_lowest   :: EntryId -- ^ the lowest entry we ever saw
     , b_baseline :: Maybe EntryId -- ^ we only care about ids higher than this
     , b_ranges :: Map EntryId Range -- ^ Map by range's low Id
     }
     deriving (Generic, Show)
     deriving anyclass (FromJSON, ToJSON)

instance Default BoundaryTracker where
  def = EmptyTracker

data Range = Range
   { _r_low :: EntryId
   , r_high :: EntryId
   , r_highTime :: POSIXTime
   }
   deriving (Generic, Show)
   deriving anyclass (FromJSON, ToJSON)

mark :: POSIXTime -> EntryId -> BoundaryTracker -> BoundaryTracker
mark time el EmptyTracker =
  BoundaryTracker el Nothing (Map.singleton el (Range el el time))
mark time el (BoundaryTracker lowest baseline m) =
  if el < fromMaybe el baseline
  then BoundaryTracker (min lowest el) baseline m
  else BoundaryTracker (min lowest el) baseline rangeMap
  where
  rangeMap =
      case Map.lookupLE el m of
        Nothing -> checkAbove
        Just (_, Range low high _)
            -- already in range
            | el <= high -> m
            | el == succ high ->
               case Map.lookup (succ el) m of
                  Just (Range _ h t) ->
                     -- join ranges
                     Map.insert low (Range low h t) $
                     Map.delete (succ el) m
                  Nothing ->
                     -- extend range upwards
                     Map.insert low (Range low el time) m
            | otherwise -> checkAbove

  checkAbove =
     case Map.lookupGT el m of
        Nothing ->
           -- disjoint range
           Map.insert el (Range el el time) m
        Just (_, Range low high high_t)
           | el == pred low ->
             -- extend range downwards
             Map.insert el (Range el high high_t) $
             Map.delete low m
           | otherwise ->
             -- disjoint range
             Map.insert el (Range el el time) m

boundaries :: BoundaryTracker -> Boundaries
boundaries EmptyTracker = Boundaries []
boundaries (BoundaryTracker lowest baseline m) = Boundaries $
  case [(l, h) | Range l h _ <- Map.elems m] of
    [] ->
      case baseline of
        Nothing -> [(lowest, lowest)]
        Just b -> [(lowest, b)]
    (low, high) : xs ->
      case baseline of
        Nothing -> (lowest, high) : xs
        Just b
          | low <= succ b -> (lowest, high) : xs
          | otherwise -> (lowest, b) : (low, high) : xs

-- | To avoid having our list of ranges grow indefinitely, we will
-- clean it up by ignoring all range gaps below some safe time boundary.
compact :: POSIXTime -> BoundaryTracker -> BoundaryTracker
compact _ EmptyTracker = EmptyTracker
compact bound (BoundaryTracker lowest baseline m) =
  BoundaryTracker lowest newBaseline (Map.fromList over)
  where
  -- split between ranges where the high's value was collected under or over the time boundary.
  (under, over) = span (\(_, range) -> r_highTime range <= bound) $ Map.toList m

  newBaseline =
    if null under
    then baseline
    else
      let maxUnder = maximum $ fmap (r_high . snd) under in
      case baseline of
        Nothing -> Just maxUnder
        Just b -> Just $ max b maxUnder

