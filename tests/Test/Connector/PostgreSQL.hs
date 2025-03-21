{-# OPTIONS_GHC -Wno-orphans #-}
module Test.Connector.PostgreSQL
  ( testPostgreSQL

  , PostgresCreds
  , withPostgreSQL
  , mkPostgreSQL

  , withEventsTable
  , Event(..)
  , Table(..)
  )
  where

import Control.Concurrent (MVar, newMVar, modifyMVar)
import Control.Exception (bracket, throwIO, ErrorCall(..), fromException)
import Control.Monad (void, forM_)
import qualified Data.Aeson as Aeson
import Data.Aeson (FromJSON)
import Data.List (isInfixOf)
import Data.Scientific (Scientific)
import Data.String (fromString)
import qualified Data.Text as Text
import Data.Word (Word16)
import System.IO (hGetLine)
import Test.Hspec
  ( Spec
  , it
  , describe
  , shouldBe
  , shouldThrow
  )
import qualified Database.PostgreSQL.Simple as P
import qualified Database.PostgreSQL.Simple.ToField as P
import qualified Database.PostgreSQL.Simple.FromField as P hiding (Binary)
import System.IO.Unsafe (unsafePerformIO)
import System.Exit (ExitCode(..))
import System.Process (readProcessWithExitCode)

import qualified Ambar.Emulator.Connector as Connector
import Ambar.Emulator.Connector.Postgres (PostgreSQL(..))
import Ambar.Emulator.Connector.Poll (PollingInterval(..))
import Ambar.Emulator.Queue.Topic (Topic, PartitionCount(..))
import qualified Ambar.Emulator.Queue.Topic as Topic
import Ambar.Record (Bytes(..))
import Util.Docker (DockerCommand(..), withDocker)
import Util.OnDemand (OnDemand)
import Test.Util.SQL
import qualified Util.OnDemand as OnDemand

import Util.Delay (deadline, seconds, delay, millis)

testPostgreSQL :: OnDemand PostgresCreds -> Spec
testPostgreSQL p = do
  describe "PostgreSQL" $ do
    testGenericSQL with

    -- Test that column types are supported/unsupported by
    -- creating database entries with the value and reporting
    -- on the emulator's behaviour when trying to decode them.
    describe "decodes" $ do
      describe "Numeric" $ do
        supported "SMALLINT"         (1 :: Int)
        supported "INTEGER"          (1 :: Int)
        supported "BIGINT"           (1 :: Int)
        supported "REAL"             (1.0 :: Double)
        supported "DOUBLE PRECISION" (1.5 :: Double)
        supported "SMALLSERIAL"      (1 :: Int)
        supported "SERIAL"           (1 :: Int)
        supported "BIGSERIAL"        (1 :: Int)
        unsupported "DECIMAL"        (1.5 :: Scientific)
        unsupported "NUMERIC"        (1.5 :: Scientific)

      describe "Monetary" $ do
        unsupported "MONEY" (1.5 :: Double)

      describe "Strings" $ do
        supported "TEXT"                         ("tryme" :: String)
        unsupported "CHARACTER VARYING(5)"       ("tryme" :: String)
        unsupported "VARCHAR(5)"                 ("tryme" :: String)
        unsupported "CHARACTER(5)"               ("tryme" :: String)
        unsupported "CHAR(5)"                    ("tryme" :: String)
        unsupported "BPCHAR(5)"                  ("tryme" :: String)
        unsupported "BPCHAR"                     ("tryme" :: String)

      describe "Binary" $ do
        supported "BYTEA"                        (BytesRow (Bytes "AAAA"))

      describe "Time" $ do
        unsupported "DATE"                       ("1999-01-08" :: String)
        unsupported "DATE"                       ("January 8, 1999" :: String)

        -- Time
        unsupported "TIME"                       ("04:05:06.789" :: String)
        unsupported "TIME"                       ("04:05:06.789" :: String)
        unsupported "TIME"                       ("04:05 PM" :: String)
        unsupported "TIME"                       ("04:05:06 PST" :: String)
        -- time zone ignored
        unsupported "TIME"                       ("04:05:06 PST" :: String)
        -- date is taken into account for daylight savings rules.
        unsupported "TIME"                       ("2003-04-12 04:05:06 America/New_York" :: String)
        unsupported "TIME(0)"                    ("04:05" :: String)
        unsupported "TIME WITHOUT TIME ZONE"     ("04:05 PM" :: String)
        unsupported "TIMETZ"                     ("04:05:06 PST" :: String)
        unsupported "TIME WITH TIME ZONE"        ("04:05:06 PST" :: String)
        unsupported "TIME(0) WITH TIME ZONE"     ("04:05:06 PST" :: String)

        -- Timestamps
        supported "TIMESTAMP"                    ("1999-01-08 04:05:06" :: String)
        supported "TIMESTAMP"                    ("1999-01-08 04:05:06" :: String)
        supported "TIMESTAMP(0)"                 ("1999-01-08 04:05:06" :: String)
        supported "TIMESTAMP WITHOUT TIME ZONE"  ("1999-01-08 04:05:06" :: String)
        supported "TIMESTAMPTZ"                  ("1999-01-08 12:05:06+00" :: String)
        supported "TIMESTAMPTZ(0)"               ("1999-01-08 04:05:06+00" :: String)
        supported "TIMESTAMP WITH TIME ZONE"     ("1999-01-08 04:05:06+00" :: String)

        unsupported "INTERVAL"                   ("3 years 3 mons 700 days 133:17:36.789" :: String)
        unsupported "INTERVAL(0)"                ("3 years 3 mons 700 days 133:17:36.789" :: String)
        unsupported "INTERVAL YEAR"              ("3 years" :: String)

      describe "Boolean" $ do
        supported "BOOLEAN"                      True

      describe "Custom" $ do -- Custom types are not supported.
        it "unsupported ENUM" $
          withType "MOOD" "CREATE TYPE MOOD AS ENUM ('sad', 'ok', 'happy')" $
          roundTrip "MOOD" ("ok" :: String) `shouldThrow` unsupportedType

        -- composite types are not supported.
        it "unsupported - composit types" $
          withType "complex" "CREATE TYPE complex AS ( r DOUBLE PRECISION, i DOUBLE PRECISION )" $
          roundTrip "COMPLEX" ("( 1.0 , 1.0 )" :: String) `shouldThrow` unsupportedType

        it "unsupported - domain types" $
          withType "positive_int" "CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0)" $
          roundTrip "POSITIVE_INT" (10 :: Int) `shouldThrow` unsupportedType

      describe "Geometric" $ do
        unsupported "POINT"                      ("( 1,2 )" :: String)
        unsupported "LINE"                       ("{ 1,2,3 }" :: String)
        unsupported "LSEG"                       ("[ (1,2), (3,4) ]" :: String)
        unsupported "BOX"                        ("(1,2), (3,4)" :: String)
        unsupported "PATH"                       ("[ (1,2), (3,4) ]" :: String)
        unsupported "POLYGON"                    ("( (1,2), (3,4) )" :: String)
        unsupported "CIRCLE"                     ("<(1,2), 3>" :: String)

      describe "Network" $ do
        unsupported "INET"                       ("127.0.0.1" :: String)
        unsupported "INET"                       ("2001:0000:130F:0000:0000:09C0:876A:130B" :: String)
        unsupported "CIDR"                       ("::ffff:1.2.3.0/120" :: String)
        unsupported "MACADDR"                    ("08:00:2b:01:02:03" :: String)
        unsupported "MACADDR"                    ("08002b010203" :: String)
        unsupported "MACADDR8"                   ("08:00:2b:01:02:03:04:05" :: String)
        unsupported "MACADDR8"                   ("08002b0102030405" :: String)

      describe "Bit String" $ do
        unsupported "BIT"                        ("0" :: String)
        unsupported "BIT(3)"                     ("111" :: String)
        unsupported "BIT VARYING(5)"             ("111" :: String)

      describe "UUID Type" $ do
        unsupported "UUID"                       ("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" :: String)

      describe "XML Type" $ do
        unsupported "XML"                        ("<?xml version=\"1.0\"?><book><title>Manual</title></book>" :: String)

      describe "JSON" $ do
        -- JSON values are delivered to the client as a string.
        supported "JSON"                         ("{\"one\":1}" :: String)
        unsupported "JSONB"                      ("{ \"one\": 1 }" :: String)

      describe "Arrays" $ do
        unsupported "INTEGER[]"                  ("{1,2,3}" :: String)
        unsupported "INTEGER ARRAY"              ("{1,2,3}" :: String)
        unsupported "INTEGER[3]"                 ("{1,2,3}" :: String)
        unsupported "INTEGER ARRAY[3]"           ("{1,2,3}" :: String)
        unsupported "INTEGER[][]"                ("{ {1,2,3}, {4,5,6} }" :: String)
        unsupported "INTEGER[3][3]"              ("{ {1,2,3}, {4,5,6} }" :: String)

      describe "Ranges" $ do
        unsupported "INT4RANGE"                  ("[1,3)" :: String)
        unsupported "INT4RANGE"                  ("empty" :: String)
        unsupported "INT4MULTIRANGE"             ("{ [1,3) }" :: String)
        unsupported "INT8RANGE"                  ("[1,3)" :: String)
        unsupported "INT8MULTIRANGE"             ("{ [1,3) }" :: String)
        unsupported "NUMRANGE"                   ("[ 1.0, 3.0 )" :: String)
        unsupported "NUMMULTIRANGE"              ("{ [1.0, 3.0 ) }" :: String)
        unsupported "TSRANGE"                    ("[1999-01-08 04:05:06, 2000-01-08 04:05:06)" :: String)
        unsupported "TSMULTIRANGE"               ("{ [1999-01-08 04:05:06, 2000-01-08 04:05:06) }" :: String)
        unsupported "TSTZRANGE"                  ("[1999-01-08 12:05:06+00, 2000-01-08 12:05:06+00)" :: String)
        unsupported "TSTZMULTIRANGE"             ("{ [1999-01-08 12:05:06+00, 2000-01-08 12:05:06+00) }" :: String)
        unsupported "DATERANGE"                  ("[1999-01-08, 2000-01-08)" :: String)
        unsupported "DATEMULTIRANGE"             ("{ [1999-01-08, 2000-01-08) }" :: String)

      describe "Object Identifier" $ do
        unsupported "OID"                       (564182 :: Int)
        unsupported "REGCLASS"                  ("pg_type" :: String)
        unsupported "REGCOLLATION"              ("\"POSIX\"" :: String)
        unsupported "REGCONFIG"                 ("english" :: String)
        unsupported "REGDICTIONARY"             ("simple" :: String)
        unsupported "REGNAMESPACE"              ("pg_catalog" :: String)
        unsupported "REGOPERATOR"               ("*(integer,integer)" :: String)
        unsupported "REGPROC"                   ("xml" :: String)
        unsupported "REGPROCEDURE"              ("sum(int4)" :: String)
        -- unsupported "REGROLE"                   ("postgres" :: String)
        unsupported "REGTYPE"                   ("integer" :: String)
        unsupported "PG_LSN"                    ("AAA/AAA" :: String)
  where
  with
    :: PartitionCount
    -> ( P.Connection
      -> EventsTable PostgreSQL
      -> Topic
      -> (IO b -> IO b)
      -> IO a )
    -> IO a
  with = withConnector p withConnection mkPostgreSQL ()

  withType ty definition act =
    OnDemand.with p $ \creds ->
    withConnection creds $ \conn -> do
      let create = P.execute_ conn definition
          destroy _ = P.execute_ conn $ "DROP TYPE " <> ty
      bracket create destroy (const act)

  unsupported :: ValidTy a => String -> a -> Spec
  unsupported ty val =
    it ("unsupported " <> ty) $
      roundTrip ty val `shouldThrow` unsupportedType

  unsupportedType e
    | Just (Connector.UnsupportedType _) <- fromException e = True
    | otherwise = False

  supported :: ValidTy a => String -> a -> Spec
  supported ty val = it ty $ roundTrip ty val

  -- Write a value of a given type to a database table, then read it from the Topic.
  roundTrip :: forall a. ValidTy a => String -> a -> IO ()
  roundTrip ty val =
    withConnector @(TTable PostgreSQL a) p withConnection mkPostgreSQL (PostgresType ty) (PartitionCount 1) $ \conn table topic connected -> do
    let record = TEntry 1 1 1 val
    insert conn table [record]
    connected $ deadline (seconds 1) $ do
      Topic.withConsumer topic group $ \consumer -> do
        (entry, _) <- readEntry consumer
        entry `shouldBe` record

type ValidTy a = (FromJSON a, P.FromField a, P.ToField a, Show a, Eq a)



mkPostgreSQL :: Table t => PostgresCreds -> t -> PostgreSQL
mkPostgreSQL PostgresCreds{..} table = PostgreSQL
  { c_host = Text.pack p_host
  , c_port = p_port
  , c_username = Text.pack p_username
  , c_password = Text.pack p_password
  , c_database = Text.pack p_database
  , c_table = Text.pack (tableName table)
  , c_columns = tableCols table
  , c_partitioningColumn = "aggregate_id"
  , c_serialColumn = "id"
  , c_pollingInterval = PollingInterval (millis 10)
  }

data PostgresCreds = PostgresCreds
  { p_database :: String
  , p_username :: String
  , p_password :: String
  , p_host :: String
  , p_port :: Word16
  }

newtype PostgresType a = PostgresType String

instance P.FromField a => P.FromRow (TEntry a)
instance P.FromRow Event

instance (P.FromField a, P.ToField a) => Table (TTable PostgreSQL a) where
  type Entry (TTable PostgreSQL a) = TEntry a
  type Config (TTable PostgreSQL a) = PostgresType a
  type Connection (TTable PostgreSQL a) = P.Connection
  tableName (TTable name _) = name
  tableCols _ = ["id", "aggregate_id", "sequence_number", "value"]
  mocks = error "no mocks for TTable"
  selectAll conn table =
    P.query_ @(TEntry a) conn (fromString $ "SELECT * FROM " <> tableName table)
  insert conn t entries =
    void $ P.executeMany conn query [(agg_id, seq_num, val) | TEntry _ agg_id seq_num val <- entries ]
    where
    query = fromString $ unwords
      [ "INSERT INTO", tableName t
      ,"(aggregate_id, sequence_number, value)"
      ,"VALUES (?, ?, ?)"
      ]
  withTable (PostgresType ty) conn f =
    withPgTable conn schema $ \name -> f (TTable name ty)
    where
    schema = unwords
        [ "( id               SERIAL"
        , ", aggregate_id     INTEGER NOT NULL"
        , ", sequence_number  INTEGER NOT NULL"
        , ", value            " <> ty
        , ", PRIMARY KEY (id)"
        , ", UNIQUE (aggregate_id, sequence_number)"
        , ")"
        ]

instance Table (EventsTable PostgreSQL) where
  type (Entry (EventsTable PostgreSQL)) = Event
  type (Config (EventsTable PostgreSQL)) = ()
  type (Connection (EventsTable PostgreSQL)) = P.Connection
  tableName (EventsTable name) = name
  tableCols _ = ["id", "aggregate_id", "sequence_number"]
  mocks _ =
    -- the aggregate_id is given when the records are inserted into the database
    [ [ Event err agg_id seq_id | seq_id <- [0..] ]
      | agg_id <- [0..]
    ]
    where err = error "aggregate id is determined by postgres"

  selectAll conn table =
    P.query_ @Event conn (fromString $ "SELECT * FROM " <> tableName table)

  insert conn (EventsTable table) events =
    void $ P.executeMany conn query [(agg_id, seq_num) | Event _ agg_id seq_num <- events ]
    where
    query = fromString $ unwords
      [ "INSERT INTO", table
      ,"(aggregate_id, sequence_number)"
      ,"VALUES ( ?, ? )"
      ]

  withTable _ conn f =
    withPgTable conn schema $ \name -> f (EventsTable name)
    where
    schema = unwords
        [ "( id               SERIAL"
        , ", aggregate_id     INTEGER NOT NULL"
        , ", sequence_number  INTEGER NOT NULL"
        , ", PRIMARY KEY (id)"
        , ", UNIQUE (aggregate_id, sequence_number)"
        , ")"
        ]

withEventsTable :: PostgresCreds -> (P.Connection -> EventsTable PostgreSQL -> IO a) -> IO a
withEventsTable creds f =
  withConnection creds $ \conn ->
  withTable () conn $ \table ->
  f conn table

{-# NOINLINE tableNumber #-}
tableNumber :: MVar Int
tableNumber = unsafePerformIO (newMVar 0)

-- | Binary data saved in PostgreSQL.
-- Use it to read and write binary data. Does not perform base64 conversion.
newtype BytesRow = BytesRow Bytes
  deriving stock (Eq, Show)
  deriving newtype (Aeson.FromJSON, Aeson.ToJSON)

instance P.ToField BytesRow where
  toField (BytesRow (Bytes bs)) = P.toField (P.Binary bs)

instance P.FromField BytesRow where
  fromField = error "not used"

type Schema = String

withConnection :: PostgresCreds -> (P.Connection -> IO a) -> IO a
withConnection creds act = do
  conn <- P.connect P.ConnectInfo
    { P.connectUser = p_username creds
    , P.connectPassword = p_password creds
    , P.connectDatabase = p_database creds
    , P.connectHost = p_host creds
    , P.connectPort = p_port creds
    }
  act conn

-- | Creates a new table on every invocation.
withPgTable :: P.Connection -> Schema -> (String -> IO a) -> IO a
withPgTable conn schema f = bracket create destroy f
  where
  execute q = void $ P.execute_ conn (fromString q)
  create = do
    name <- takeName
    execute $ unwords [ "CREATE TABLE IF NOT EXISTS", name, schema ]
    return name

  destroy name =
    execute $ "DROP TABLE " <> name

  takeName = do
    number <- modifyMVar tableNumber $ \n -> return (n + 1, n)
    return $ "table_" <> show number

-- | Create a PostgreSQL database and delete it upon completion.
withPostgreSQL :: (PostgresCreds -> IO a) -> IO a
withPostgreSQL f = bracket setup teardown f
  where
  setup = do
    let creds@PostgresCreds{..} = PostgresCreds
          { p_database = "db_test"
          , p_username = "test_user"
          , p_password = "test_pass"
          , p_host =  P.connectHost P.defaultConnectInfo
          , p_port = P.connectPort P.defaultConnectInfo
          }
    putStrLn "creating user..."
    createUser p_username p_password
    putStrLn "creating database..."
    createDatabase p_username p_database
    putStrLn "database ready."
    return creds

  teardown PostgresCreds{..} = do
    deleteDatabase p_database
    dropUser p_username

  psql cmd = do
    (code, _, err) <- readProcessWithExitCode "psql"
      [ "--dbname", "postgres"
      , "--command", cmd
      ] ""
    case code of
      ExitSuccess -> return Nothing
      ExitFailure _ -> return (Just err)

  createUser name pass = do
    r <- psql $ unwords [ "CREATE USER", name, "WITH SUPERUSER PASSWORD '" <> pass <> "'"]
    forM_ r $ \err ->
      if "already exists" `isInfixOf` err
      then return ()
      else throwIO $ ErrorCall $ "Unable to create PostgreSQL user: " <> err

  createDatabase user name = do
    r <- psql $ unwords ["CREATE DATABASE", name, "WITH OWNER '" <> user <> "'"]
    forM_ r $ \err ->
      if "already exists" `isInfixOf` err
      then return ()
      else throwIO $ ErrorCall $ "Unable to create PostgreSQL database: " <> err

  dropUser name = do
    (code, _, err) <- readProcessWithExitCode "dropuser" [name] ""
    case code of
      ExitSuccess -> return ()
      _ -> throwIO $ ErrorCall $ "Unable to delete PostgreSQL user: " <> err

  deleteDatabase name = do
    (code, _, err) <- readProcessWithExitCode "dropdb" [name] ""
    case code of
      ExitSuccess -> return ()
      _ -> throwIO $ ErrorCall $ "Unable to delete PostgreSQL database: " <> err

-- | Use PostgreSQL from Docker. Not used for now.
_withPostgreSQLDocker :: (PostgresCreds -> IO a) -> IO a
_withPostgreSQLDocker f = do
  let cmd = DockerRun
        { run_image = "postgres:14.10"
        , run_args =
          [ "--env", "POSTGRES_PASSWORD=" <> p_password
          , "--env", "POSTGRES_USER=" <> p_username
          , "--env", "POSTGRES_DB=" <> p_database
          , "--publish",  show p_port <> ":5432"
          ]
        }
  r <- withDocker False "PostgreSQL" cmd $ \h -> do
    waitTillReady h
    f creds
  return r
  where
  waitTillReady h = do
    putStrLn "Waiting for PostgreSQL docker..."
    deadline (seconds 60) $ do
      whileM $ do
        line <- hGetLine h
        return $ not $ isReadyNotice line
    delay (seconds 1)
    putStrLn "PostgreSQL docker is ready."

  whileM m = do
    r <- m
    if r then whileM m else return ()

  isReadyNotice str =
    "database system is ready to accept connections" `isInfixOf` str

  creds@PostgresCreds{..} = PostgresCreds
    { p_database = "test_db"
    , p_username = "test_user"
    , p_password = "test_pass"
    , p_host =  P.connectHost P.defaultConnectInfo
    , p_port = 5555
    }

