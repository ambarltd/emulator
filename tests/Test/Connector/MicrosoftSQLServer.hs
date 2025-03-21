{-# OPTIONS_GHC -Wno-orphans #-}
module Test.Connector.MicrosoftSQLServer
  ( testMicrosoftSQLServer
  , MicrosoftSQLServerCreds
  , withMicrosoftSQLServer
  )
  where

import Control.Exception (bracket)
import Control.Monad (void)
import Data.Aeson (FromJSON, ToJSON)
import qualified Data.ByteString as BS
import Data.List (isInfixOf)
import qualified Data.Text as Text
import System.IO (hGetLine)
import Test.Hspec
  ( Spec
  , describe
  , shouldBe
  , it
  )

import qualified Ambar.Emulator.Queue.Topic as Topic
import Ambar.Emulator.Queue.Topic (Topic, PartitionCount(..))
import Ambar.Emulator.Connector.Poll (PollingInterval(..))
import Ambar.Emulator.Connector.MicrosoftSQLServer (SQLServer(..))
import Ambar.Record (Bytes(..))
import Database.SQLServer as S

import Util.Docker (DockerCommand(..), withDocker)
import Util.OnDemand (OnDemand)
import Test.Util.SQL
  ( EventsTable(..)
  , Table(..)
  , Event(..)
  , TTable(..)
  , TEntry(..)
  , testGenericSQL
  , mkTableName
  , withConnector
  , readEntry
  , group
  )

import Util.Delay (deadline, seconds, millis)

testMicrosoftSQLServer :: OnDemand MicrosoftSQLServerCreds -> Spec
testMicrosoftSQLServer od = do
  describe "MicrosoftSQLServer" $ do
    testGenericSQL with
    describe "decodes" $ do
      -- Integers
      supported "TINYINT"                 (1 :: Int)
      supported "SMALLINT"                (1 :: Int)
      supported "INT"                     (1 :: Int)
      supported "BIGINT"                  (1 :: Int)
      supported "DECIMAL"                 (1 :: Int)
      supported "DECIMAL(2)"              (1 :: Int)
      supported "DECIMAL(2, 1)"           (1 :: Int)
      supported "NUMERIC"                 (1 :: Int)

      -- Doubles
      supported "REAL"                    (1.0 :: Double)
      supported "MONEY"                   (1.0 :: Double)
      supported "FLOAT"                   (1.0 :: Double)
      supported "SMALLMONEY"              (1.0 :: Double)

      -- Binary
      supported "UNIQUEIDENTIFIER"        (BytesRow (Bytes (BS.pack [1..16])))
      supported "BIT"                     (BytesRow (Bytes (BS.pack [1])))
      supported "BINARY"                  (BytesRow (Bytes "A"))
      supported "BINARY(4)"               (BytesRow (Bytes "AAAA"))
      supported "VARBINARY(64)"           (BytesRow (Bytes "AAAA"))

      -- Strings
      supported "TEXT"                    ("AAAA" :: String)
      supported "NTEXT"                   ("AAAA" :: String)
      supported "CHAR"                    ("A" :: String)
      supported "CHAR(4)"                 ("AAAA" :: String)
      supported "VARCHAR"                 ("A" :: String)
      supported "VARCHAR(4)"              ("AAAA" :: String)
      supported "NATIONAL CHAR"           ("A" :: String)
      supported "NATIONAL CHARACTER"      ("A" :: String)
      supported "NCHAR"                   ("A" :: String)
      supported "NCHAR(4)"                ("AAAA" :: String)
      supported "NATIONAL CHAR VARYING"   ("A" :: String)
      supported "NATIONAL CHARACTER VARYING" ("A" :: String)
      supported "NVARCHAR"                ("A" :: String)
      supported "NVARCHAR(4)"             ("AAAA" :: String)
      supported "XML"                     ("<xml><tag>wat</tag></xml>" :: String)

      -- dates
      supported "DATETIME"                ("1999-01-08T00:00:00Z" :: String)
      supported "SMALLDATETIME"           ("1999-01-08T00:00:00Z" :: String)
      supported "DATE"                    ("1999-01-08" :: String)
      supported "TIME"                    ("04:05:06.7890000" :: String)
      supported "TIME(3)"                 ("04:05:06.789" :: String)
      supported "DATETIME2"               ("1999-01-08 00:00:00.0000000" :: String)
      supported "DATETIMEOFFSET"          ("1999-01-08 00:00:00.0000000 +00:00" :: String)
      supported "DATETIMEOFFSET(2)"       ("1999-01-08 00:00:00.00 +00:00" :: String)

      -- JSON is still in preview mode in SQL server. I wasn't able to test it.
      -- supported "JSON"                    ("{\"a\": 1}" :: String)
      -- unsupported "CLR UDT"              _NULL
      -- unsupported "IMAGE"                _NULL
      -- unsupported "SQL_VARIANT"          _NULL
  where
  with
    :: PartitionCount
    -> ( S.Connection
      -> EventsTable SQLServer
      -> Topic
      -> (IO b -> IO b)
      -> IO a )
    -> IO a
  with = withConnector od withConnection mkConnector ()

  _NULL :: Maybe String
  _NULL = Nothing

  supported :: (FromField a, FromJSON a, ToField a, Show a, Eq a) => String -> a -> Spec
  supported ty val = it ty $ roundTrip ty val

  -- Write a value of a given type to a database table, then read it from the Topic.
  roundTrip :: forall a. (FromField a, FromJSON a, ToField a, Show a, Eq a) => String -> a -> IO ()
  roundTrip ty val =
    withConnector @(TTable SQLServer a) od withConnection mkConnector (SQLServerType ty) (PartitionCount 1) $ \conn table topic connected -> do
    let record = TEntry 1 1 1 val
    insert conn table [record]
    connected $ deadline (seconds 1) $ do
      Topic.withConsumer topic group $ \consumer -> do
        (entry, _) <- readEntry consumer
        entry `shouldBe` record

-- | Binary data saved in MySQL.
-- Use it to read and write binary data. Does not perform base64 conversion.
newtype BytesRow = BytesRow Bytes
  deriving stock (Eq, Show)
  deriving newtype (FromJSON, ToJSON)

instance ToField BytesRow where
  toField (BytesRow (Bytes bs)) = toField bs

instance FromField BytesRow where
  fieldParser = BytesRow . Bytes <$> fieldParser

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
  , c_pollingInterval = PollingInterval (millis 10)
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
    let q = S.mkQuery_ $ "SELECT * FROM " <> table
    rs <- S.query conn q
    return $ fmap (\(i, agg_id, seq_num) -> Event i agg_id seq_num) rs

  insert conn (EventsTable table) events =
    void $ S.execute conn q
    where
    q = S.mkQueryMany
      [(agg_id, seq_num) | Event _ agg_id seq_num <- events ]
      $ Text.unlines $
        [ "INSERT INTO", Text.pack table
        ,"(aggregate_id, sequence_number)"
        ,"VALUES ( ?, ? );"
        ]

  withTable _ conn f =
    withSQLServerTable conn schema $ \name -> f (EventsTable name)
    where
    schema = unwords
        [ "( id               INT IDENTITY(1,1) PRIMARY KEY"
        , ", aggregate_id     INTEGER NOT NULL"
        , ", sequence_number  INTEGER NOT NULL"
        , ")"
        ]

newtype SQLServerType = SQLServerType String

instance (ToField a, FromField a) => Table (TTable SQLServer a) where
  type Entry (TTable SQLServer a) = TEntry a
  type Config (TTable SQLServer a) = SQLServerType
  type Connection (TTable SQLServer a) = S.Connection
  tableName (TTable name _) = name
  tableCols _ = ["id", "aggregate_id", "sequence_number", "value"]
  mocks _ = error "no mock for TTable"
  selectAll conn (TTable table _) =
    S.query conn $ S.mkQuery_ $ "SELECT * FROM " <> table

  insert conn (TTable table _) events =
    void $ S.execute conn $ mkQueryMany args q
    where
    args = [(agg_id, seq_num, val) | TEntry _ agg_id seq_num val <- events ]
    q = Text.pack $ unwords
      [ "INSERT INTO", table
      ,"(aggregate_id, sequence_number, value)"
      ,"VALUES (?, ?, ?)"
      ]

  withTable (SQLServerType ty) conn f =
    withSQLServerTable conn schema $ \name -> f (TTable name ty)
    where
    schema = unwords
        [ "( id               INT IDENTITY(1,1) PRIMARY KEY"
        , ", aggregate_id     INTEGER NOT NULL"
        , ", sequence_number  INTEGER NOT NULL"
        , ", value            " <> ty
        , ")"
        ]

instance FromField a => FromRow (TEntry a) where
  rowParser =
    TEntry <$> parseField <*> parseField <*> parseField <*> parseField



type Schema = String

-- | Creates a new table on every invocation.
withSQLServerTable :: S.Connection -> Schema -> (String -> IO a) -> IO a
withSQLServerTable conn schema f = bracket create destroy f
  where
  create = do
    name <- mkTableName
    execute conn $ S.mkQuery_ $ unwords [ "CREATE TABLE", name, schema ]
    return name

  destroy name =
    execute conn $ S.mkQuery_ $ "DROP TABLE " <> name

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
    f creds
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

