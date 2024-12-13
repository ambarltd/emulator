{-# OPTIONS_GHC -Wno-orphans #-}
module Test.Connector.File
  ( testFileConnector
  , withFileConnectorInfo
  , FileConnectorInfo
  ) where

import Control.Concurrent (MVar, newMVar, modifyMVar)
import Data.ByteString.Lazy.Char8 as Char8
import qualified Data.Aeson as Json
import Data.Map (Map)
import qualified Data.Map as Map
import Control.Monad (forM_, forM)
import Data.Text (Text)
import System.FilePath ((</>))
import System.IO (withFile, IOMode(..))
import System.IO.Temp (withSystemTempFile)
import Test.Hspec (Spec, describe)

import Ambar.Emulator.Connector.File (FileConnector(..))

import Test.Utils.OnDemand (OnDemand)
import Test.Utils.SQL hiding (Connection)
import qualified Test.Utils.SQL as TS

testFileConnector :: OnDemand FileConnectorInfo -> Spec
testFileConnector od =
  describe "FileConnector" $ do
    testGenericSQL @(EventsTable FileConnector) od withConnection mkFileConnector ()

type TableName = String

data FileConnectorInfo = FileConnectorInfo
  { c_directory :: FilePath
  , c_partitioningField :: Text
  , c_incrementingField :: Text
  , c_maxIds :: MVar (Map TableName Int)
  }

withFileConnectorInfo :: (ConnectInfo -> IO a) -> IO a
withFileConnectorInfo f =
  withSystemTempFile "file-db-xxx" $ \path _ -> do
    var <- newMVar mempty
    f $ FileConnectorInfo path "aggregate_id" "id" var

type ConnectInfo = FileConnectorInfo
type Connection = FileConnectorInfo

withConnection :: ConnectInfo -> (Connection -> IO a) -> IO a
withConnection cinfo f = f cinfo

mkFileConnector :: Table t => Connection -> t -> FileConnector
mkFileConnector FileConnectorInfo{..} table =
  FileConnector
    { c_path = c_directory </> tableName table
    , c_partitioningField = c_partitioningField
    , c_incrementingField = c_incrementingField
    }

instance Table (EventsTable FileConnector) where
  type Entry (EventsTable FileConnector) = Event
  type Config (EventsTable FileConnector) = ()
  type Connection (EventsTable FileConnector) = FileConnectorInfo
  tableName (EventsTable name) = name
  tableCols _ = ["id", "aggregate_id", "sequence_number"]
  withTable () conn f = do
    name <- mkTableName
    let table = EventsTable name
        connector = mkFileConnector conn table
    withFile (c_path connector) ReadMode $ \_ -> f table
  mocks _ =
    [ [ Event err agg_id seq_id | seq_id <- [0..] ]
      | agg_id <- [0..]
    ]
    where err = error "aggregate id is determined by mysql"
  insert conn table events =
    forM_ events $ \(Event _ agg_id seq_id) -> do
      modifyMVar (c_maxIds conn) $ \ids -> do
        let nxt = Map.findWithDefault 0 tname ids
            encoded = Json.encode $ Event nxt agg_id seq_id
        Char8.appendFile (c_path connector) (encoded <> "\n")
        return (Map.insert tname (nxt + 1) ids, nxt)
    where
    tname = tableName table
    connector = mkFileConnector conn table

  selectAll conn table = do
    bs <- Char8.readFile (c_path connector)
    forM (Char8.lines bs) $ \line ->
      case Json.eitherDecode' line of
        Left err -> error $ "unable to decode file entry: " <> err
        Right v -> return v
    where
    connector = mkFileConnector conn table




