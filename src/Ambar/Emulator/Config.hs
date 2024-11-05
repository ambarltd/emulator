module Ambar.Emulator.Config
  ( parseEnvConfigFile
  , Id(..)
  , EmulatorConfig(..)
  , EnvironmentConfig(..)
  , DataSource(..)
  , Source(..)
  , DataDestination(..)
  , Destination(..)
  )
  where

{-| Parsing of the configuration file
-}

import Control.Monad (forM_, when)
import Control.Exception (throwIO, ErrorCall(..))
import Data.Aeson (ToJSON, FromJSON, (.:), FromJSONKey, ToJSONKey)
import qualified Data.Aeson as Json
import qualified Data.ByteString as BS
import qualified Data.Text as Text
import Data.Text (Text)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Yaml as Yaml

import qualified Ambar.Emulator.Connector.Postgres as Pg
import Ambar.Transport.Http (Endpoint, User, Password)

newtype Id a = Id { unId :: Text }
  deriving newtype (ToJSON, FromJSON, FromJSONKey, ToJSONKey, Ord, Eq)

-- | Configures the internals of the emulator
data EmulatorConfig = EmulatorConfig
  { c_partitionsPerTopic :: Int
  , c_maxParallelism :: Maybe Int
  , c_dataPath :: FilePath
  }

-- | An environment's configuration.
-- Describes the sources, destinations and their links.
data EnvironmentConfig = EnvironmentConfig
  { c_sources :: Map (Id DataSource) DataSource
  , c_destinations :: Map (Id DataDestination) DataDestination
  }

data DataSource = DataSource
  { s_id :: Id DataSource
  , s_description :: Text
  , s_source :: Source
  }

data Source
  = SourcePostgreSQL Pg.ConnectorConfig
  | SourceFile FilePath

data DataDestination = DataDestination
  { d_id :: Id DataDestination
  , d_sources :: [Id DataSource]
  , d_description :: Text
  , d_destination :: Destination
  }

data Destination
  = DestinationFile FilePath
  | DestinationHttp
      { d_endpoint :: Endpoint
      , d_username :: User
      , d_password :: Password
      }

instance FromJSON EnvironmentConfig where
  parseJSON = Json.withObject "EnvironmentConfig" $ \o -> do
    c_sources <- do
      sources <- o .: "data_sources"
      let multimap = Map.fromListWith (++) [ (s_id s, [s]) | s <- sources ]
      forM_ multimap $ \xs ->
        when (length xs > 1) $
        fail $ Text.unpack $ "Multiple data sources with ID '" <> unId (s_id $ head xs) <> "'"
      return $ fmap head multimap
    c_destinations <- do
      dsts <- o .: "data_destinations"
      let multimap = Map.fromListWith (++) [ (d_id s, [s]) | s <- dsts ]
      forM_ multimap $ \xs ->
        when (length xs > 1) $
        fail $ Text.unpack $ "Multiple data destinations with ID '" <> unId (d_id $ head xs) <> "'"
      return $ fmap head multimap
    forM_ c_destinations $ \dest ->
      forM_ (d_sources dest) $ \sid ->
      when (sid `Map.notMember` c_sources) $
        fail $ Text.unpack $ "Unknown data source: '" <> unId sid <> "'"
    return EnvironmentConfig{..}

instance FromJSON DataSource where
  parseJSON = Json.withObject "DataSource" $ \o -> do
    s_id <- o .: "id"
    s_description <- o .: "description"
    s_source <- (o .: "type") >>= \t ->
      case t of
        "postgres" -> parsePostgreSQL o
        "file" -> parseFile o
        _ -> fail $ unwords
          [ "Invalid data source type: '" <> t <> "'."
          , "Expected one of: postgres, file."
          ]
    return DataSource{..}
    where
    parsePostgreSQL o = do
      c_host <- o .: "host"
      c_port <- o .: "port"
      c_username <- o .: "username"
      c_password <- o .: "password"
      c_database <- o .: "database"
      c_table <- o .: "table"
      c_columns <- o .: "columns"
      c_partitioningColumn <- o .: "partitioningColumn"
      c_serialColumn <- o .: "serialColumn"
      return $ SourcePostgreSQL Pg.ConnectorConfig{..}

    parseFile o = SourceFile <$> (o .: "path")

instance FromJSON DataDestination where
  parseJSON = Json.withObject "DataSource" $ \o -> do
    d_id <- o .: "id"
    d_sources <- o .: "sources"
    d_description <- o .: "description"
    d_destination <- (o .: "type") >>= \t ->
      case t of
        "http-push" -> parseHTTPPush o
        "file" -> parseFile o
        _ -> fail $ unwords
          [ "Invalid data destination type: '" <> t <> "'."
          , "Expected one of: http-push, file."
          ]
    return DataDestination{..}
    where
    parseHTTPPush o = do
      d_endpoint <- o .: "endpoint"
      d_username <- o .: "username"
      d_password <- o .: "password"
      return $ DestinationHttp {..}

    parseFile o = DestinationFile <$> (o .: "path")

parseEnvConfigFile :: FilePath -> IO EnvironmentConfig
parseEnvConfigFile path = do
  bs <- BS.readFile path
  case Yaml.decodeEither' bs of
    Left err -> throwIO $ ErrorCall $ "Unable to parse config file: " <> show err
    Right v -> return v

