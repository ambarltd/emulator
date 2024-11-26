{-# OPTIONS_GHC -Wno-orphans #-}
module Test.Connector.MySQL
  ( testMySQL
  , MySQLCreds
  , withMySQL
  , mkMySQL
  )
  where

import Control.Exception (bracket, throwIO, ErrorCall(..), uninterruptibleMask_)
import Control.Monad (void, forM_)
import Data.String (fromString)
import qualified Data.Text as Text
import System.Exit (ExitCode(..))
import System.Process (readProcessWithExitCode)
import Test.Hspec
  ( Spec
  , describe
  )

import Ambar.Emulator.Connector.MySQL (MySQL(..))
import Database.MySQL
   ( ConnectionInfo(..)
   , Connection
   , FromRow(..)
   , FromField
   , ToField
   , parseField
   , executeMany
   , execute_
   , query_
   , withConnection
   , defaultConnectionInfo
   )

import Test.Utils.OnDemand (OnDemand)

import Test.Utils.SQL hiding (Connection)
import qualified Test.Utils.SQL as TS

type MySQLCreds = ConnectionInfo

testMySQL :: OnDemand MySQLCreds -> Spec
testMySQL c = do
  describe "MySQL" $ do
    testGenericSQL @(EventsTable MySQL) c withConnection mkMySQL ()

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


newtype MySQLType = MySQLType String

instance FromField a => FromRow (TEntry a) where
  rowParser = TEntry <$> parseField <*> parseField <*> parseField <*> parseField

instance (ToField a, FromField a) => Table (TTable MySQL a) where
  type Entry (TTable MySQL a) = TEntry a
  type Config (TTable MySQL a) = MySQLType
  type Connection (TTable MySQL a) = Connection
  tableName (TTable name _) = name
  tableCols _ = ["id", "aggregate_id", "sequence_number"]
  mocks _ = error "no mock for TTable"
  selectAll conn (TTable table _) =
    query_ conn (fromString $ "SELECT * FROM " <> table)

  insert conn (TTable table _) events =
    void $ executeMany conn q [(agg_id, seq_num, val) | TEntry _ agg_id seq_num val <- events ]
    where
    q = fromString $ unwords
      [ "INSERT INTO", table
      ,"(aggregate_id, sequence_number, value)"
      ,"VALUES ( ?, ?, ?)"
      ]

  withTable (MySQLType ty) conn f =
    withMySQLTable conn schema $ \name -> f (TTable name ty)
    where
    schema = unwords
        [ "( id               SERIAL"
        , ", aggregate_id     INTEGER NOT NULL"
        , ", sequence_number  INTEGER NOT NULL"
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

