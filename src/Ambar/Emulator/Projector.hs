module Ambar.Emulator.Projector
  ( Projection(..)
  , project
  , filterVerdict
  , FilterVerdict(..)
  , Message(..)
  , Payload(..)
  ) where

{-| A projector reads messages from multiple queues, applies a filter to the
stream and submits passing messages to a single data destination.

A destination with no filter receives every record. A destination with a
filter receives only records whose filter-column value is in the allowed
set; filtered-out records are committed without being sent, so the
consumer cursor always advances.
-}

import qualified Data.Aeson as Json
import Data.Aeson (ToJSON, FromJSON)
import qualified Data.Aeson.KeyMap as KeyMap
import qualified Data.ByteString.Lazy as LB
import Data.Maybe (listToMaybe, fromMaybe)
import Data.String (fromString)
import Data.Text (Text)
import qualified Data.Text.Encoding as Text
import qualified Data.Text as Text
import Control.Concurrent.Async (forConcurrently_)
import Control.Monad (when)
import Control.Monad.Extra (whileM)
import Data.IORef (newIORef, atomicModifyIORef')
import GHC.Generics (Generic)

import qualified Data.Set as Set

import Ambar.Emulator.Config (Id(..), DataDestination, DataSource(..), Source(..), DestinationFilter(..))
import Ambar.Emulator.Queue.Topic (Topic, ReadError(..), PartitionCount(..))
import qualified Ambar.Emulator.Queue.Topic as Topic
import Ambar.Emulator.Connector.MicrosoftSQLServer (SQLServer(..))
import Ambar.Emulator.Connector.Postgres (PostgreSQL(..))
import Ambar.Emulator.Connector.MySQL (MySQL(..))
import Ambar.Transport (Transport)
import qualified Ambar.Transport as Transport
import Util.Some (Some)
import Util.Logger (SimpleLogger, logFatal, logWarn, logInfo, fatal, annotate)
import Util.Delay (Duration, delay, millis, seconds, toDiffTime)
import Util.Prettyprinter (prettyJSON, renderPretty)
import Prettyprinter (pretty, fillSep, (<+>))

data Projection = Projection
  { p_id :: Id Projection
  , p_destination :: Id DataDestination
  , p_destinationDescription :: Text
  , p_sources :: [(DataSource, Topic)]
  , p_transport :: Some Transport
  , p_filter :: Maybe DestinationFilter
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
project logger_ Projection{..} = do
  -- One warning per destination per emulator run when the filter fails
  -- open (reviewer note on PR #56): a typo'd filter column silently
  -- restores full delivery, which looks exactly like a normal match.
  -- Warning on every record would flood the log at full event rate, so
  -- the first fail-open claims this flag and later ones stay quiet.
  warnedFailOpen <- newIORef False
  let warnFailOpenOnce logger reason = do
        firstWarn <- atomicModifyIORef' warnedFailOpen (\claimed -> (True, not claimed))
        when firstWarn $
          logWarn logger $
            "filter fail-open: " <> reason
            <> ". Delivering the record; the destination filter is not being applied."
            <> " Further fail-open warnings for this destination are suppressed."
  forConcurrently_ p_sources (projectSource warnFailOpenOnce)
  where
  projectSource warnFailOpenOnce (source, topic) =
    -- one consumer per partition
    Topic.withConsumers topic group pcount $ \consumers ->
    forConcurrently_ consumers $ \consumer ->
    whileM $ consume warnFailOpenOnce logger consumer source
    where
      PartitionCount pcount = Topic.partitionCount topic
      logger =
        annotate ("src: " <> unId (s_id source)) $
        annotate ("dst: " <> unId  p_destination)
        logger_

  consume warnFailOpenOnce logger consumer source = do
    r <- Topic.read consumer
    case r of
      Left EndOfPartition -> return False
      Left err -> fatal logger (show err)
      Right (bs, meta) -> do
        record <- decode logger bs
        let logger' = annotate (relevantFields (s_source source) record) logger
            send = do
              retrying logger' $ Transport.sendJSON p_transport (toMsg source record)
              logInfo logger' ("sent." :: Text)
        case filterVerdict p_filter record of
          Matched -> send
          Skipped -> logInfo logger' ("filtered." :: Text)
          FailedOpen reason -> do
            warnFailOpenOnce logger' reason
            send
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
    Left err ->
      let
        raw_chars_to_show = 1000
        raw = take raw_chars_to_show $ Text.unpack $ Text.decodeUtf8 bs
      in
      fatal logger $ "decoding error: " <> err <> "\nraw: " <> raw
    Right v -> return v

-- | The filter's decision for a record.
--
-- 'FailedOpen' means the record is DELIVERED even though the filter could
-- not be applied (missing column, null or non-string value, non-object
-- record): a misconfigured column name must degrade to full delivery (the
-- pre-filter behaviour), never to silently dropped records. The reason is
-- carried so the projector can warn that the filter is not doing its job.
data FilterVerdict
  = Matched            -- ^ deliver: no filter, or the column value is allowed
  | Skipped            -- ^ commit without delivering
  | FailedOpen Text    -- ^ deliver, but the filter could not be applied
  deriving (Eq, Show)

filterVerdict :: Maybe DestinationFilter -> Payload -> FilterVerdict
filterVerdict Nothing _ = Matched
filterVerdict (Just DestinationFilter{..}) (Payload value) =
  case value of
    Json.Object o ->
      case KeyMap.lookup (fromString $ Text.unpack f_column) o of
        Just (Json.String v)
          | Set.member v f_values -> Matched
          | otherwise -> Skipped
        Just Json.Null -> FailedOpen $ "filter column '" <> f_column <> "' is null"
        Just _ -> FailedOpen $ "filter column '" <> f_column <> "' has a non-string value"
        Nothing -> FailedOpen $ "filter column '" <> f_column <> "' is missing from the record"
    _ -> FailedOpen "record is not a JSON object"

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
              msg = "Retrying in " <> show (toDiffTime wait) <> ". Error: " <> show err

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
