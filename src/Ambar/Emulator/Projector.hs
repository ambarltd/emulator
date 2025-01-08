module Ambar.Emulator.Projector
  ( Projection(..)
  , project
  , Message(..)
  , Payload(..)
  ) where

{-| A projector reads messages from multiple queues, applies a filter to the
stream and submits passing messages to a single data destination.

We do not implement filters for now.
-}

import qualified Data.Aeson as Json
import Data.Aeson (ToJSON, FromJSON)
import qualified Data.Aeson.KeyMap as KeyMap
import qualified Data.ByteString.Lazy as LB
import Data.Maybe (listToMaybe, fromMaybe)
import Data.String (fromString)
import Data.Text (Text)
import qualified Data.Text as Text
import Control.Concurrent.Async (forConcurrently_)
import Control.Monad.Extra (whileM)
import GHC.Generics (Generic)

import Ambar.Emulator.Config (Id(..), DataDestination, DataSource(..), Source(..))
import Ambar.Emulator.Queue.Topic (Topic, ReadError(..), PartitionCount(..))
import qualified Ambar.Emulator.Queue.Topic as Topic
import Ambar.Emulator.Connector.MicrosoftSQLServer (SQLServer(..))
import Ambar.Emulator.Connector.Postgres (PostgreSQL(..))
import Ambar.Emulator.Connector.MySQL (MySQL(..))
import Ambar.Transport (Transport)
import qualified Ambar.Transport as Transport
import Util.Some (Some)
import Util.Logger (SimpleLogger, logFatal, logWarn, logInfo, fatal, annotate)
import Util.Delay (Duration, delay, millis, seconds)
import Util.Prettyprinter (prettyJSON, renderPretty)
import Prettyprinter (pretty, fillSep, (<+>))

data Projection = Projection
  { p_id :: Id Projection
  , p_destination :: Id DataDestination
  , p_destinationDescription :: Text
  , p_sources :: [(DataSource, Topic)]
  , p_transport :: Some Transport
  }

-- | A record enriched with more information to send to the client.
data Message = Message
  { data_source_id :: Text
  , data_source_description :: Text
  , data_destination_id :: Text
  , data_destination_description :: Text
  , payload :: Payload
  }
  deriving (Generic, Show)
  deriving anyclass (ToJSON, FromJSON)

newtype Payload = Payload Json.Value
  deriving (Show)
  deriving newtype (ToJSON, FromJSON)

project :: SimpleLogger -> Projection -> IO ()
project logger_ Projection{..} =
  forConcurrently_ p_sources projectSource
  where
  projectSource (source, topic) =
    -- one consumer per partition
    Topic.withConsumers topic group pcount $ \consumers ->
    forConcurrently_ consumers $ \consumer ->
    whileM $ consume logger consumer source
    where
      PartitionCount pcount = Topic.partitionCount topic
      logger =
        annotate ("src: " <> unId (s_id source)) $
        annotate ("dst: " <> unId  p_destination)
        logger_

  consume logger consumer source = do
    r <- Topic.read consumer
    case r of
      Left EndOfPartition -> return False
      Left err -> fatal logger (show err)
      Right (bs, meta) -> do
        record <- decode logger bs
        retrying logger $ Transport.sendJSON p_transport (toMsg source record)
        logInfo logger $ "sent. " <> relevantFields (s_source source) record
        Topic.commit consumer meta
        return True

  group = Topic.ConsumerGroupName $ unId p_id

  toMsg source record = LB.toStrict $ Json.encode $ Message
    { data_source_id = unId (s_id source)
    , data_source_description = s_description source
    , data_destination_id = unId p_destination
    , data_destination_description = p_destinationDescription
    , payload = record
    }

  decode logger bs = case Json.eitherDecode $ LB.fromStrict bs of
    Left err -> fatal logger $ "decoding error: " <> err
    Right v -> return v

-- | Fields to print when a record is sent.
relevantFields :: Source -> Payload -> Text
relevantFields source (Payload value) = renderPretty $
  withObject $ \o ->
  case source of
    SourceFile{} -> prettyJSON value
    SourceMySQL MySQL{..} ->
      fillSep $
        [ pretty field <> ":" <+> prettyJSON v
        | field <- [c_incrementingColumn, c_partitioningColumn]
        , Just v <- [KeyMap.lookup (fromString $ Text.unpack field) o]
        ]
    SourcePostgreSQL PostgreSQL{..} ->
      fillSep $
        [ pretty field <> ":" <+> prettyJSON v
        | field <- [c_serialColumn, c_partitioningColumn]
        , Just v <- [KeyMap.lookup (fromString $ Text.unpack field) o]
        ]
    SourceSQLServer SQLServer{..} ->
      fillSep $
        [ pretty field <> ":" <+> prettyJSON v
        | field <- [c_incrementingColumn, c_partitioningColumn]
        , Just v <- [KeyMap.lookup (fromString $ Text.unpack field) o]
        ]
  where
  withObject f =
    case value of
      Json.Object o -> f o
      _ -> prettyJSON value


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
  fibs = 1 : 1 : zipWith (+) fibs (drop 1 fibs)
