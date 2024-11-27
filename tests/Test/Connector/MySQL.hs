{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module Test.Connector.MySQL
  ( testMySQL
  , MySQLCreds
  , withMySQL
  , mkMySQL
  )
  where

import Control.Exception (bracket, throwIO, ErrorCall(..), uninterruptibleMask_, fromException)
import Control.Monad (void, forM_)
import Data.Aeson (FromJSON, ToJSON)
import Data.String (fromString)
import qualified Data.Text as Text
import System.Exit (ExitCode(..))
import System.Process (readProcessWithExitCode)
import Test.Hspec
  ( Spec
  , it
  , describe
  , shouldBe
  , shouldThrow
  )

import Ambar.Emulator.Connector as Connector
import Ambar.Emulator.Connector.MySQL (MySQL(..))
import Database.MySQL
   ( ConnectionInfo(..)
   , Connection
   , FromRow(..)
   , FromField(..)
   , ToField
   , Binary(..)
   , Param(..)
   , parseField
   , executeMany
   , execute_
   , query_
   , withConnection
   , defaultConnectionInfo
   )
import qualified Ambar.Emulator.Queue.Topic as Topic
import Ambar.Emulator.Queue.Topic (PartitionCount(..))
import Ambar.Record (Bytes(..))

import Utils.Delay (deadline, seconds)
import Test.Utils.OnDemand (OnDemand)
import Test.Utils.SQL hiding (Connection)
import qualified Test.Utils.SQL as TS

type MySQLCreds = ConnectionInfo

testMySQL :: OnDemand MySQLCreds -> Spec
testMySQL c = do
  describe "MySQL" $ do
    testGenericSQL @(EventsTable MySQL) c withConnection mkMySQL ()

    -- Test that column types are supported/unsupported by
    -- creating database entries with the value and reporting
    -- on the emulator's behaviour when trying to decode them.
    describe "decodes" $ do
      describe "Numeric" $ do
        -- supported "SERIAL" (1 :: Int) -- there can only be one SERIAL and the 'id' is already it.
        supported "TINYINT"               (1 :: Int)
        supported "TINYINT UNSIGNED"      (1 :: Int)
        supported "TINYINT(5)"            (1 :: Int)
        supported "SMALLINT"              (1 :: Int)
        supported "SMALLINT UNSIGNED"     (1 :: Int)
        supported "SMALLINT(5)"           (1 :: Int)
        supported "MEDIUMINT"             (1 :: Int)
        supported "MEDIUMINT UNSIGNED"    (1 :: Int)
        supported "MEDIUMINT(5)"          (1 :: Int)
        supported "INT"                   (1 :: Int)
        supported "INT UNSIGNED"          (1 :: Int)
        supported "INT(5)"                (1 :: Int)
        supported "INTEGER"               (1 :: Int)
        supported "INTEGER UNSIGNED"      (1 :: Int)
        supported "INTEGER(5)"            (1 :: Int)
        supported "BIGINT"                (1 :: Int)
        supported "BIGINT UNSIGNED"       (1 :: Int)
        supported "BIGINT(5)"             (1 :: Int)
        supported "DECIMAL"               (1.0 :: Double)
        supported "DECIMAL(5,2)"          (1.5 :: Double) -- 5 digits, 2 after comma.
        supported "DEC(5,2)"              (1.5 :: Double)
        supported "NUMERIC(5,2)"          (1.5 :: Double)
        supported "FIXED(5,2)"            (1.5 :: Double)
        supported "FLOAT"                 (1.5 :: Double)
        supported "FLOAT(5)"              (1.5 :: Double)  -- 5 bits of precision
        supported "FLOAT(5,2)"            (1.5 :: Double)  -- 5 digits, 2 after comma.
        supported "DOUBLE"                (1.5 :: Double)
        supported "DOUBLE(5,2)"           (1.5 :: Double)
        supported "DOUBLE PRECISION"      (1.5 :: Double)
        supported "DOUBLE PRECISION(5,2)" (1.5 :: Double)
        supported "REAL"                  (1.5 :: Double)
        supported "REAL(5,2)"             (1.5 :: Double)

        -- BOOLEAN is numeric. Very sad.
        supported "BOOL"                  (1 :: Int)
        supported "BOOLEAN"               (0 :: Int)

        -- BIT is numeric. Very sad.
        unsupported "BIT"                        (1 :: Int)
        unsupported "BIT(5)"                     (1 :: Int)

      describe "Time" $ do
        unsupported "DATE"                       ("1999-01-08" :: String)

        -- DATETIME is time from 1000 to 9999
        supported "DATETIME"                   ("1999-01-08 00:00:00" :: String)
        supported "DATETIME(3)"                ("1999-01-08 04:05:06.555" :: String)

        -- TIMESTAMP is time from 1970 to 2038
        supported "TIMESTAMP"                   ("1999-01-08 00:00:00" :: String)
        supported "TIMESTAMP(3)"                ("1999-01-08 04:05:06.555" :: String)

        -- Time
        unsupported "TIME"                       ("04:05:06.789" :: String)
        unsupported "TIME(3)"                    ("04:05:06.789" :: String)

        unsupported "YEAR"                       ("1999" :: String)
        unsupported "YEAR(4)"                    ("1999" :: String)

      describe "Strings" $ do
        supported "TEXT"                         ("tryme" :: String)
        unsupported "CHARACTER VARYING(5)"       ("tryme" :: String)
        unsupported "VARCHAR(5)"                 ("tryme" :: String)
        unsupported "CHARACTER(5)"               ("tryme" :: String)
        unsupported "CHAR(5)"                    ("tryme" :: String)
        unsupported "BPCHAR(5)"                  ("tryme" :: String)
        unsupported "BPCHAR"                     ("tryme" :: String)

      -- describe "Binary" $ do
      --   supported "BYTEA"                        (BytesRow (Bytes "AAAA"))


      -- -- describe "Custom" $ do -- Custom types are not supported.
      -- --   it "unsupported ENUM" $
      -- --     withType "MOOD" "CREATE TYPE MOOD AS ENUM ('sad', 'ok', 'happy')" $
      -- --     roundTrip "MOOD" ("ok" :: String) `shouldThrow` unsupportedType

      -- --   -- composite types are not supported.
      -- --   it "unsupported - composit types" $
      -- --     withType "complex" "CREATE TYPE complex AS ( r DOUBLE PRECISION, i DOUBLE PRECISION )" $
      -- --     roundTrip "COMPLEX" ("( 1.0 , 1.0 )" :: String) `shouldThrow` unsupportedType

      -- --   it "unsupported - domain types" $
      -- --     withType "positive_int" "CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0)" $
      -- --     roundTrip "POSITIVE_INT" (10 :: Int) `shouldThrow` unsupportedType

      -- describe "Geometric" $ do
      --   unsupported "POINT"                      ("( 1,2 )" :: String)
      --   unsupported "LINE"                       ("{ 1,2,3 }" :: String)
      --   unsupported "LSEG"                       ("[ (1,2), (3,4) ]" :: String)
      --   unsupported "BOX"                        ("(1,2), (3,4)" :: String)
      --   unsupported "PATH"                       ("[ (1,2), (3,4) ]" :: String)
      --   unsupported "POLYGON"                    ("( (1,2), (3,4) )" :: String)
      --   unsupported "CIRCLE"                     ("<(1,2), 3>" :: String)

      -- describe "Network" $ do
      --   unsupported "INET"                       ("127.0.0.1" :: String)
      --   unsupported "INET"                       ("2001:0000:130F:0000:0000:09C0:876A:130B" :: String)
      --   unsupported "CIDR"                       ("::ffff:1.2.3.0/120" :: String)
      --   unsupported "MACADDR"                    ("08:00:2b:01:02:03" :: String)
      --   unsupported "MACADDR"                    ("08002b010203" :: String)
      --   unsupported "MACADDR8"                   ("08:00:2b:01:02:03:04:05" :: String)
      --   unsupported "MACADDR8"                   ("08002b0102030405" :: String)

      -- describe "Bit String" $ do
      --   unsupported "BIT"                        ("0" :: String)
      --   unsupported "BIT(3)"                     ("111" :: String)
      --   unsupported "BIT VARYING(5)"             ("111" :: String)

      -- describe "UUID Type" $ do
      --   unsupported "UUID"                       ("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" :: String)

      -- describe "XML Type" $ do
      --   unsupported "XML"                        ("<?xml version=\"1.0\"?><book><title>Manual</title></book>" :: String)

      -- describe "JSON" $ do
      --   -- JSON values are delivered to the client as a string.
      --   supported "JSON"                         ("{\"one\":1}" :: String)
      --   unsupported "JSONB"                      ("{ \"one\": 1 }" :: String)

      -- describe "Arrays" $ do
      --   unsupported "INTEGER[]"                  ("{1,2,3}" :: String)
      --   unsupported "INTEGER ARRAY"              ("{1,2,3}" :: String)
      --   unsupported "INTEGER[3]"                 ("{1,2,3}" :: String)
      --   unsupported "INTEGER ARRAY[3]"           ("{1,2,3}" :: String)
      --   unsupported "INTEGER[][]"                ("{ {1,2,3}, {4,5,6} }" :: String)
      --   unsupported "INTEGER[3][3]"              ("{ {1,2,3}, {4,5,6} }" :: String)

      -- describe "Ranges" $ do
      --   unsupported "INT4RANGE"                  ("[1,3)" :: String)
      --   unsupported "INT4RANGE"                  ("empty" :: String)
      --   unsupported "INT4MULTIRANGE"             ("{ [1,3) }" :: String)
      --   unsupported "INT8RANGE"                  ("[1,3)" :: String)
      --   unsupported "INT8MULTIRANGE"             ("{ [1,3) }" :: String)
      --   unsupported "NUMRANGE"                   ("[ 1.0, 3.0 )" :: String)
      --   unsupported "NUMMULTIRANGE"              ("{ [1.0, 3.0 ) }" :: String)
      --   unsupported "TSRANGE"                    ("[1999-01-08 04:05:06, 2000-01-08 04:05:06)" :: String)
      --   unsupported "TSMULTIRANGE"               ("{ [1999-01-08 04:05:06, 2000-01-08 04:05:06) }" :: String)
      --   unsupported "TSTZRANGE"                  ("[1999-01-08 12:05:06+00, 2000-01-08 12:05:06+00)" :: String)
      --   unsupported "TSTZMULTIRANGE"             ("{ [1999-01-08 12:05:06+00, 2000-01-08 12:05:06+00) }" :: String)
      --   unsupported "DATERANGE"                  ("[1999-01-08, 2000-01-08)" :: String)
      --   unsupported "DATEMULTIRANGE"             ("{ [1999-01-08, 2000-01-08) }" :: String)

      -- describe "Object Identifier" $ do
      --   unsupported "OID"                       (564182 :: Int)
      --   unsupported "REGCLASS"                  ("pg_type" :: String)
      --   unsupported "REGCOLLATION"              ("\"POSIX\"" :: String)
      --   unsupported "REGCONFIG"                 ("english" :: String)
      --   unsupported "REGDICTIONARY"             ("simple" :: String)
      --   unsupported "REGNAMESPACE"              ("pg_catalog" :: String)
      --   unsupported "REGOPERATOR"               ("*(integer,integer)" :: String)
      --   unsupported "REGPROC"                   ("xml" :: String)
      --   unsupported "REGPROCEDURE"              ("sum(int4)" :: String)
      --   -- unsupported "REGROLE"                   ("postgres" :: String)
      --   unsupported "REGTYPE"                   ("integer" :: String)
      --   unsupported "PG_LSN"                    ("AAA/AAA" :: String)
  where
  unsupported :: (FromField a, FromJSON a, ToField a, Show a, Eq a) => String -> a -> Spec
  unsupported ty val =
    it ("unsupported " <> ty) $
      roundTrip ty val `shouldThrow` unsupportedType

  unsupportedType e
    | Just (Connector.UnsupportedType _) <- fromException e = True
    | otherwise = False

  supported :: (FromField a, FromJSON a, ToField a, Show a, Eq a) => String -> a -> Spec
  supported ty val = it ty $ roundTrip ty val

  -- Write a value of a given type to a database table, then read it from the Topic.
  roundTrip :: forall a. (FromField a, FromJSON a, ToField a, Show a, Eq a) => String -> a -> IO ()
  roundTrip ty val =
    withConnector @(TTable MySQL a) c withConnection mkMySQL (MySQLType ty) (PartitionCount 1) $ \conn table topic connected -> do
    let record = TEntry 1 1 1 val
    insert conn table [record]
    connected $ deadline (seconds 1) $ do
      Topic.withConsumer topic group $ \consumer -> do
        (entry, _) <- readEntry consumer
        entry `shouldBe` record

mkMySQL :: Table t => ConnectionInfo -> t -> MySQL
mkMySQL ConnectionInfo{..} table = MySQL
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

-- | Binary data saved in MySQL.
-- Use it to read and write binary data. Does not perform base64 conversion.
newtype BytesRow = BytesRow Bytes
  deriving stock (Eq, Show)
  deriving newtype (FromJSON, ToJSON)

instance Param BytesRow where
  render (BytesRow (Bytes bs)) = render (Binary bs)

instance FromField BytesRow where
  fieldParser = BytesRow . Bytes <$> fieldParser

newtype MySQLType = MySQLType String

instance FromField a => FromRow (TEntry a) where
  rowParser = TEntry <$> parseField <*> parseField <*> parseField <*> parseField

instance (ToField a, FromField a) => Table (TTable MySQL a) where
  type Entry (TTable MySQL a) = TEntry a
  type Config (TTable MySQL a) = MySQLType
  type Connection (TTable MySQL a) = Connection
  tableName (TTable name _) = name
  tableCols _ = ["id", "aggregate_id", "sequence_number", "value"]
  mocks _ = error "no mock for TTable"
  selectAll conn (TTable table _) =
    query_ conn (fromString $ "SELECT * FROM " <> table)

  insert conn (TTable table _) events =
    void $ executeMany conn q [(agg_id, seq_num, val) | TEntry _ agg_id seq_num val <- events ]
    where
    q = fromString $ unwords
      [ "INSERT INTO", table
      ,"(aggregate_id, sequence_number, value)"
      ,"VALUES (?, ?, ?)"
      ]

  withTable (MySQLType ty) conn f =
    withMySQLTable conn schema $ \name -> f (TTable name ty)
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

instance FromRow Event where
  rowParser = Event <$> parseField <*> parseField <*> parseField

instance Table (EventsTable MySQL) where
  type Entry (EventsTable MySQL) = Event
  type Config (EventsTable MySQL) = ()
  type Connection (EventsTable MySQL) = Connection
  tableName (EventsTable name) = name
  tableCols _ = ["id", "aggregate_id", "sequence_number"]
  mocks _ =
    -- the aggregate_id is given when the records are inserted into the database
    [ [ Event err agg_id seq_id | seq_id <- [0..] ]
      | agg_id <- [0..]
    ]
    where err = error "aggregate id is determined by mysql"

  selectAll conn (EventsTable table) =
    query_ conn (fromString $ "SELECT * FROM " <> table)

  insert conn (EventsTable table) events =
    void $ executeMany conn q [(agg_id, seq_num) | Event _ agg_id seq_num <- events ]
    where
    q = fromString $ unwords
      [ "INSERT INTO", table
      ,"(aggregate_id, sequence_number)"
      ,"VALUES ( ?, ? )"
      ]

  withTable _ conn f =
    withMySQLTable conn schema $ \name -> f (EventsTable name)
    where
    schema = unwords
        [ "( id               SERIAL"
        , ", aggregate_id     INTEGER NOT NULL"
        , ", sequence_number  INTEGER NOT NULL"
        , ", PRIMARY KEY (id)"
        , ", UNIQUE (aggregate_id, sequence_number)"
        , ")"
        ]

type Schema = String

-- | Creates a new table on every invocation.
withMySQLTable :: Connection -> Schema -> (String -> IO a) -> IO a
withMySQLTable conn schema f = bracket create destroy f
  where
  execute q = void $ execute_ conn (fromString q)
  create = do
    name <- mkTableName
    execute $ unwords [ "CREATE TABLE IF NOT EXISTS", name, schema ]
    return name

  destroy name =
    execute $ "DROP TABLE " <> name

-- | Create a MySQL database and delete it upon completion.
withMySQL :: (ConnectionInfo -> IO a) -> IO a
withMySQL f = bracket setup teardown f
  where
  setup = do
    let creds@ConnectionInfo{..} = defaultConnectionInfo
          { conn_database = "test_db"
          , conn_username = "test_user"
          , conn_password = "test_pass"
          }

        user = conn_username
    createUser user conn_password
    createDatabase conn_username conn_database
    return creds

  teardown ConnectionInfo{..} = uninterruptibleMask_ $ do
    deleteDatabase conn_database
    dropUser conn_username

  -- run setup commands as root.
  mysql cmd = do
    (code, _, err) <- readProcessWithExitCode "mysql"
      [ "--user", "root"
      , "--execute", cmd
      ] ""
    case code of
      ExitSuccess -> return Nothing
      ExitFailure _ -> return (Just err)

  run cmd = do
    r <- mysql (Text.unpack cmd)
    forM_ r $ \err ->
      throwIO $ ErrorCall $ "MySQL command failed: " <> (Text.unpack cmd) <> "\n" <> err

  createUser user pass =
    run $ Text.unwords ["CREATE USER", user, "IDENTIFIED BY",  "'" <> pass <> "'"]

  createDatabase user db =
    run $ Text.unwords
      ["CREATE DATABASE", db, ";"
      , "GRANT ALL PRIVILEGES ON", db <> ".*", "TO", user, "WITH GRANT OPTION;"
      , "FLUSH PRIVILEGES;"
      ]

  dropUser user =
    run $ Text.unwords [ "DROP USER", user]

  deleteDatabase db =
    run $ Text.unwords [ "DROP DATABASE", db]

