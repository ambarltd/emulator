module Test.Connector.MySQL
  ( testMySQL

  , MySQLCreds
  , withMySQL
  , mkMySQL
  )
  where

import Control.Concurrent (MVar, newMVar, modifyMVar)
import Control.Exception (bracket, throwIO, ErrorCall(..))
import Control.Monad (void, forM_)
import qualified Data.Aeson as Aeson
import Data.Default (def)
import Data.List (stripPrefix)
import Data.Maybe (fromMaybe)
import Data.String (fromString)
import qualified Data.Text as Text
import Data.Word (Word16)
import qualified Database.MySQL.Simple as M
import qualified Database.MySQL.Simple.QueryResults as M
import GHC.Generics
import System.Exit (ExitCode(..))
import System.IO.Unsafe (unsafePerformIO)
import System.Process (readProcessWithExitCode)
import Test.Hspec
  ( Spec
  , it
  , describe
  , shouldBe
  )


import qualified Ambar.Emulator.Connector as Connector
import Ambar.Emulator.Connector (partitioner, encoder)
import Ambar.Emulator.Connector.MySQL (MySQL(..))
import Ambar.Emulator.Queue.Topic (Topic, PartitionCount(..))
import qualified Ambar.Emulator.Queue.Topic as Topic

import Test.Queue (withFileTopic)
import Test.Utils.OnDemand (OnDemand)
import Utils.Logger (plainLogger, Severity(..))
import qualified Test.Utils.OnDemand as OnDemand


testMySQL :: OnDemand MySQLCreds -> Spec
testMySQL c = do
  describe "MySQL" $ do
    -- checks that our tests can connect to postgres
    it "connects" $
      with () (PartitionCount 1) $ \conn table _ _ -> do
        insert conn table (take 10 $ head mocks)
        rs <- M.query_ @Event conn (fromString $ "SELECT * FROM " <> tableName table)
        length rs `shouldBe` 10
  where
  with
    :: Table t
    => Config t
    -> PartitionCount
    -> (M.Connection -> t -> Topic -> (IO b -> IO b) -> IO a)
    -> IO a
  with conf partitions f =
    OnDemand.with c $ \creds ->                                    -- load db
    withConnection creds $ \conn ->
    withTable conf conn $ \table ->                                -- create events table
    withFileTopic partitions $ \topic ->                           -- create topic
    let config = mkMySQL creds table in
    Topic.withProducer topic partitioner encoder $ \producer -> do -- create topic producer
    let logger = plainLogger Warn
        connected act = -- setup connector
          Connector.connect config logger def producer (const act)
    f conn table topic connected

-- | Credentials to manipulate the available MySQL database.
data MySQLCreds = MySQLCreds
  { p_database :: String
  , p_username :: String
  , p_password :: String
  , p_host :: String
  , p_port :: Word16
  }

mkMySQL :: Table t => MySQLCreds -> t -> MySQL
mkMySQL MySQLCreds{..} table = MySQL
  { c_host = Text.pack p_host
  , c_port = p_port
  , c_username = Text.pack p_username
  , c_password = Text.pack p_password
  , c_database = Text.pack p_database
  , c_table = Text.pack (tableName table)
  , c_columns = tableCols table
  , c_partitioningColumn = "aggregate_id"
  , c_incrementingColumn = "id"
  }

withConnection :: MySQLCreds -> (M.Connection -> IO a) -> IO a
withConnection MySQLCreds{..} act = do
  conn <- M.connect M.defaultConnectInfo
    { M.connectHost = p_host
    , M.connectPort = p_port
    , M.connectUser = p_username
    , M.connectPassword = p_password
    , M.connectDatabase = p_database
    }
  act conn

class Table a where
  type Entry a = b | b -> a
  type Config a = b | b -> a
  withTable :: Config a -> M.Connection -> (a -> IO b) -> IO b
  tableCols :: a -> [Text.Text]
  tableName :: a -> String
  -- Mock events to be added to the database.
  -- Each sublist is an infinite list of events for the same aggregate.
  mocks :: [[Entry a]]
  insert :: M.Connection -> a -> [Entry a] -> IO ()

data Event = Event
  { e_id :: Int
  , e_aggregate_id :: Int
  , e_sequence_number :: Int
  }
  deriving (Eq, Show, Generic)

instance Aeson.FromJSON Event where
  parseJSON = Aeson.genericParseJSON opt
    where
    opt = Aeson.defaultOptions
      { Aeson.fieldLabelModifier = \label ->
        fromMaybe label (stripPrefix "e_" label)
      }

instance M.QueryResults Event where
  convertResults tys mbs =
    let (e_id, e_agg, e_seq) = M.convertResults tys mbs
    in Event e_id e_agg e_seq

newtype EventsTable = EventsTable String

instance Table EventsTable where
  type (Entry EventsTable) = Event
  type (Config EventsTable) = ()
  tableName (EventsTable name) = name
  tableCols _ = ["id", "aggregate_id", "sequence_number"]
  mocks =
    -- the aggregate_id is given when the records are inserted into the database
    [ [ Event err agg_id seq_id | seq_id <- [0..] ]
      | agg_id <- [0..]
    ]
    where err = error "aggregate id is determined by mysql"

  insert conn (EventsTable table) events =
    void $ M.executeMany conn query [(agg_id, seq_num) | Event _ agg_id seq_num <- events ]
    where
    query = fromString $ unwords
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

{-# NOINLINE tableNumber #-}
tableNumber :: MVar Int
tableNumber = unsafePerformIO (newMVar 0)

-- | Creates a new table on every invocation.
withMySQLTable :: M.Connection -> Schema -> (String -> IO a) -> IO a
withMySQLTable conn schema f = bracket create destroy f
  where
  execute q = void $ M.execute_ conn (fromString q)
  create = do
    name <- takeName
    execute $ unwords [ "CREATE TABLE IF NOT EXISTS", name, schema ]
    return name

  destroy name =
    execute $ "DROP TABLE " <> name

  takeName = do
    number <- modifyMVar tableNumber $ \n -> return (n + 1, n)
    return $ "table_" <> show number

-- | Create a MySQL database and delete it upon completion.
withMySQL :: (MySQLCreds -> IO a) -> IO a
withMySQL f = bracket setup teardown f
  where
  setup = do
    let creds@MySQLCreds{..} = MySQLCreds
          { p_database = "test_db"
          , p_username = "test_user"
          , p_password = "test_pass"
          , p_host =  M.connectHost M.defaultConnectInfo
          , p_port = M.connectPort M.defaultConnectInfo
          }

        user = "'" <> p_username <> "'@'" <> p_host <> "'"

    putStrLn "creating user..."
    createUser user p_password
    putStrLn "creating database..."
    createDatabase p_username p_database
    putStrLn "granting permissions..."
    grantPermissions user p_database
    putStrLn "database ready"
    return creds

  teardown MySQLCreds{..} = do
    deleteDatabase p_database
    dropUser p_username

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
    r <- mysql cmd
    forM_ r $ \err ->
      throwIO $ ErrorCall $ "MySQL command failed: " <> cmd <> "\n" <> err

  createUser user pass =
    run $ unwords ["CREATE USER", user, "IDENTIFIED BY",  "'" <> pass <> "'"]

  createDatabase user db =
    run $ unwords ["CREATE DATABASE", db, "WITH OWNER '" <> user <> "'"]

  grantPermissions user db =
    run $ unwords [
      "GRANT ALL PRIVILEGES ON", db <> ".*", "TO", user, "WITH GRANT OPTION;",
      "FLUSH PRIVILEGES;"
      ]

  dropUser user =
    run $ unwords [ "DROP USER", user]

  deleteDatabase db =
    run $ unwords [ "DROP DATABASE", db]
