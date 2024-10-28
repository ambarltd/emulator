module Ambar.Lite.Projector where

{-| A projector reads messages from multiple queues, applies a filter to the
stream and submits passing messages to a single data destination.

We do not implement filters for now.
-}

import qualified Data.Aeson as Json
import Data.Aeson (ToJSON, FromJSON)
import qualified Data.ByteString.Lazy as LB
import Data.Maybe (listToMaybe, fromMaybe)
import Data.Text (Text)
import Control.Concurrent.Async (replicateConcurrently_, forConcurrently_)
import Control.Monad.Extra (whileM)
import GHC.Generics (Generic)

import Ambar.Lite.Queue.Topic (Topic, ReadError(..))
import qualified Ambar.Lite.Queue.Topic as Topic
import Ambar.Transport (Transport)
import qualified Ambar.Transport as Transport
import Utils.Some (Some)
import Utils.Logger (SimpleLogger, logFatal, logWarn, fatal, annotate)
import Utils.Delay (Duration, delay, millis, seconds)

data DataSource = DataSource
  { s_id :: Text
  , s_description :: Text
  }

data DataDestination = DataDestination
  { d_id :: Text
  , d_description :: Text
  }

data Projection = Projection
  { p_id :: Text
  , p_destination :: DataDestination
  , p_sources :: [(DataSource, Topic)]
  , p_parallelism :: Int
  , p_transport :: Some Transport
  }

-- | A record enriched with more information to send to the client.
data Message = Message
  { data_source_id :: Text
  , data_source_description :: Text
  , data_destination_id :: Text
  , data_destination_description :: Text
  , payload :: Record
  }
  deriving (Generic, Show)
  deriving anyclass (ToJSON, FromJSON)

newtype Record = Record Json.Value
  deriving newtype (ToJSON, FromJSON)
  deriving Show

project :: SimpleLogger -> Projection -> IO ()
project logger_ Projection{..} =
  forConcurrently_ p_sources $ \(source, topic) ->
  replicateConcurrently_ p_parallelism $
  Topic.withConsumer topic group $ \consumer -> do
  let logger =
        annotate ("source:" <> s_id source) $
        annotate ("destination:" <> d_id p_destination)
        logger_
  whileM $ consume logger consumer source
  where
  consume logger consumer source = do
    r <- Topic.read consumer
    case r of
      Left EndOfPartition -> return False
      Left err -> fatal logger (show err)
      Right (bs, meta) -> do
        record <- decode logger bs
        retrying logger $ Transport.sendJSON p_transport (toMsg source record)
        Topic.commit consumer meta
        return True

  group = Topic.ConsumerGroupName p_id

  toMsg source record = LB.toStrict $ Json.encode $ Message
    { data_source_id = s_id source
    , data_source_description = s_description source
    , data_destination_id = d_id p_destination
    , data_destination_description = d_description p_destination
    , payload = record
    }

  decode logger bs = case Json.eitherDecode $ LB.fromStrict bs of
    Left err -> fatal logger $ "decoding error: " <> err
    Right v -> return v

-- | Retry forever
retrying :: Show err => SimpleLogger -> IO (Maybe err) -> IO ()
retrying logger act = go backoff
  where
    go waits = do
      r <- act
      case r of
        Nothing -> return ()
        Just err -> do
          let wait = fromMaybe maxInterval $ listToMaybe waits
              msg = "Retrying in " <> show wait <> "ms. Error: " <> show err

          -- On too many retries start sounding the alarm.
          if wait == maxInterval
             then logFatal logger msg
             else logWarn logger msg

          delay wait
          go (drop 1 waits)

maxInterval :: Duration
maxInterval = seconds 60

-- fibonacci backoff of up to maxInterval. In microseconds
backoff :: [Duration]
backoff
  = takeWhile (< maxInterval)
  $ dropWhile (< 5)
  $ map millis fibs
  where
  fibs = 1 : 1 : zipWith (+) fibs (tail fibs)
