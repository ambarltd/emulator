{-# OPTIONS_GHC -Wno-orphans #-}
module Test.Connector.MicrosoftSQLServer
  ( testMicrosoftSQLServer
  , MicrosoftSQLServerCreds
  , withMicrosoftSQLServer
  )
  where

import Control.Exception (bracket)
import Control.Monad (void)
import Data.List (isInfixOf)
import Data.String (fromString)
import qualified Data.Text as Text
import System.IO (hGetLine)
import Test.Hspec
  ( Spec
  , describe
  )

import Ambar.Emulator.Connector.MicrosoftSQLServer (SQLServer(..))
import Database.MicrosoftSQLServer as S

import Test.Utils.Docker (DockerCommand(..), withDocker)
import Test.Utils.OnDemand (OnDemand)
import Test.Utils.SQL
  ( EventsTable(..)
  , Table(..)
  , Event(..)
  , testGenericSQL
  , mkTableName
  )

import Utils.Delay (deadline, seconds)

testMicrosoftSQLServer :: OnDemand MicrosoftSQLServerCreds -> Spec
testMicrosoftSQLServer od = do
  describe "MicrosoftSQLServer" $ do
    testGenericSQL @(EventsTable SQLServer) od withConnection mkConnector ()

mkConnector :: Table t => ConnectionInfo -> t -> SQLServer
mkConnector ConnectionInfo{..} table = SQLServer
  { c_host = conn_host
  , c_port = conn_port
  , c_username = conn_username
  , c_password = conn_password
  , c_database = conn_database
  , c_table = Text.pack $ tableName table
  , c_columns = tableCols table
  , c_partitioningColumn = "aggregate_id"
  , c_incrementingColumn = "id"
  }

instance Table (EventsTable SQLServer) where
  type Entry (EventsTable SQLServer) = Event
  type Config (EventsTable SQLServer) = ()
  type Connection (EventsTable SQLServer) = S.Connection
  tableName (EventsTable name) = name
  tableCols _ = ["id", "aggregate_id", "sequence_number"]
  mocks _ =
    -- the aggregate_id is given when the records are inserted into the database
    [ [ Event err agg_id seq_id | seq_id <- [0..] ]
      | agg_id <- [0..]
    ]
    where err = error "aggregate id is determined by SQLServer"

  selectAll conn (EventsTable table) = do
    rs <- S.query conn (fromString $ "SELECT * FROM " <> table)
    return $ fmap (\(i, agg_id, seq_num) -> Event i agg_id seq_num) rs

  insert conn (EventsTable table) events =
    void $ S.execute conn q
    where
    q = fromString $ unlines $
      ["BEGIN TRAN;"] ++
      [ unwords
        [ "INSERT INTO", table
        ,"(aggregate_id, sequence_number)"
        ,"VALUES (", show agg_id, ", ", show seq_num, " );"
        ]
      | Event _ agg_id seq_num <- events
      ] ++
      ["COMMIT TRAN;"]

  withTable _ conn f =
    withSQLServerTable conn schema $ \name -> f (EventsTable name)
    where
    schema = unwords
        [ "( id               INT IDENTITY(1,1) PRIMARY KEY"
        , ", aggregate_id     INTEGER NOT NULL"
        , ", sequence_number  INTEGER NOT NULL"
        , ")"
        ]

type Schema = String

-- | Creates a new table on every invocation.
withSQLServerTable :: S.Connection -> Schema -> (String -> IO a) -> IO a
withSQLServerTable conn schema f = bracket create destroy f
  where
  create = do
    name <- mkTableName
    execute conn $ fromString $ unwords [ "CREATE TABLE", name, schema ]
    return name

  destroy name =
    execute conn $ fromString $ "DROP TABLE " <> name

type MicrosoftSQLServerCreds = ConnectionInfo

-- | Use MicrosoftSQLServer from Docker. Not used for now.
withMicrosoftSQLServer :: (MicrosoftSQLServerCreds -> IO a) -> IO a
withMicrosoftSQLServer f = do
  let cmd = DockerRun
        { run_image = "mcr.microsoft.com/azure-sql-edge:latest"
        , run_args =
          [ "--env", "ACCEPT_EULA=" <> "Y"
          , "--env", "MSSQL_SA_PASSWORD=" <> Text.unpack conn_password
          , "--env", "MSSQL_AGENT_ENABLED=TRUE"
          , "--env", "ClientTransportType=AMQP_TCP_Only"
          , "--env", "MSSQL_PID=Premium"
          , "--publish",  show conn_port <> ":1433"
          ]
        }
  r <- withDocker False "MicrosoftSQLServer" cmd $ \h -> do
    waitTillReady h
    r <- f creds
    putStrLn "Shutting down MicrosoftSQLServer"
    return r
  return r
  where
  waitTillReady h = do
    putStrLn "Waiting for MicrosoftSQLServer docker..."
    deadline (seconds 60) $ do
      whileM $ do
        line <- hGetLine h
        return $ not $ isReadyNotice line
    putStrLn "MicrosoftSQLServer docker is ready."

  whileM m = do
    r <- m
    if r then whileM m else return ()

  isReadyNotice str =
    "EdgeTelemetry starting up" `isInfixOf` str

  creds@ConnectionInfo{..} = ConnectionInfo
    { conn_database = "master"
    , conn_username = "sa"
    , conn_password = "TestPass1234"
    , conn_host =  "0.0.0.0"
    , conn_port = 6666
    }

