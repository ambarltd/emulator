module Ambar.Emulator.Config where

{-| Parsing of the configuration file
-}

import Data.Aeson (ToJSON, FromJSON, (.:))
import qualified Data.Aeson as Json
import Data.Text (Text)
import Data.Map.Strict (Map)

import qualified Ambar.Emulator.Connector.Postgres as Pg
import Ambar.Transport.Http (Endpoint, Port, User, Password)

newtype Id a = Id { unId :: Text }
  deriving newtype (ToJSON, FromJSON, Ord, Eq)

data Config = Config
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

instance FromJSON DataSource where
  parseJSON = Json.withObject "DataSource" $ \o -> do
    s_id <- o .: "id"
    s_description <- o .: "description"
    s_source <- (o .: "type") >>= \t ->
      case t of
        "PostgreSQL" -> parsePostgreSQL o
        "file" -> parseFile o
        _ -> fail ("Invalid data source type: " <> t)
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
      , d_port :: Port
      , d_user :: User
      , d_password :: Password
      }


parse :: FilePath -> IO Config
parse path = error "TODO"
