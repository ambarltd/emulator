module Test.Connector.MySQL
  ( testMySQL
  , MySQLCreds
  , withMySQL
  , mkMySQL
  )
  where

import Control.Concurrent (MVar, newMVar, modifyMVar)
import Control.Concurrent.Async (mapConcurrently)
import Control.Exception (bracket, throwIO, ErrorCall(..), uninterruptibleMask_, fromException)
import Control.Monad (void, replicateM, forM_)
import qualified Data.Aeson as Aeson
import Data.Aeson (FromJSON)
import qualified Data.ByteString.Lazy as LB
import Data.Default (def)
import Data.List (sort, stripPrefix)
import qualified Data.Map.Strict as Map
import Data.Maybe (fromMaybe)
import Data.String (fromString)
import qualified Data.Text as Text
import GHC.Generics
import System.Exit (ExitCode(..))
import System.IO.Unsafe (unsafePerformIO)
import System.Process (readProcessWithExitCode)
import Test.Hspec
  ( Spec
  , it
  , describe
  , shouldBe
  , shouldThrow
  )
import Test.Hspec.Expectations.Contrib (annotate)


import qualified Ambar.Emulator.Connector as Connector
import Ambar.Emulator.Connector (partitioner, encoder)
import Ambar.Emulator.Connector.MySQL (MySQL(..), UnsupportedType(..))
import Ambar.Emulator.Queue.Topic (Topic, PartitionCount(..))
import qualified Ambar.Emulator.Queue.Topic as Topic
import Database.MySQL
   ( ConnectionInfo(..)
   , Connection
   , FromRow(..)
   , ToField
   , parseField
   , query_
   , executeMany
   , execute_
   , withConnection
   , defaultConnectionInfo
   )

import Test.Queue (withFileTopic)
import Test.Utils.OnDemand (OnDemand)
import Utils.Async (withAsyncThrow)
import Utils.Delay (deadline, seconds)
import Utils.Logger (plainLogger, Severity(..))
import qualified Test.Utils.OnDemand as OnDemand

type MySQLCreds = ConnectionInfo

testMySQL :: OnDemand MySQLCreds -> Spec
testMySQL c = do
  describe "MySQL" $ do
    -- checks that our tests can connect to postgres
    it "connects" $
      with () (PartitionCount 1) $ \conn table _ _ -> do
        insert conn table (take 10 $ head mocks)
        rs <- query_ @Event conn (fromString $ "SELECT * FROM " <> tableName table)
        length rs `shouldBe` 10

    it "retrieves all events already in the db" $
      with () (PartitionCount 1) $ \conn table topic connected -> do
        let count = 10
        insert conn table (take count $ head mocks)
        connected $
          deadline (seconds 2) $
          Topic.withConsumer topic group $ \consumer -> do
          es <- replicateM count $ readEntry @Event consumer
          length es `shouldBe` count

    it "can retrieve a large number of events" $
      with () (PartitionCount 1) $ \conn table topic connected -> do
        let count = 10_000
        insert conn table (take count $ head mocks)
        connected $ deadline (seconds 2) $
          Topic.withConsumer topic group $ \consumer -> do
          es <- replicateM count $ readEntry @Event consumer
          length es `shouldBe` count

    it "retrieves events added after initial snapshot" $
      with () (PartitionCount 1) $ \conn table topic connected -> do
        let count = 10
            write = insert conn table (take count $ head mocks)
        connected $ deadline (seconds 1) $
          Topic.withConsumer topic group $ \consumer ->
          withAsyncThrow write $ do
          es <- replicateM count $ readEntry @Event consumer
          length es `shouldBe` count

    it "maintains ordering through parallel writes" $ do
      let partitions = 5
      with () (PartitionCount partitions) $ \conn table topic connected -> do
        let count = 1_000
            write = mapConcurrently id
              [ insert conn table (take count $ mocks !! partition)
              | partition <- [1..partitions] ]
        connected $ deadline (seconds 1) $
          Topic.withConsumer topic group $ \consumer -> do
          -- write and consume concurrently
          withAsyncThrow write $ do
            es <- replicateM (count * partitions) $ readEntry consumer
            let byAggregateId = Map.toList $ Map.fromListWith (flip (++))
                    [ (e_aggregate_id, [e_sequence_number])
                    | (Event{..}, _) <- es
                    ]
            forM_ byAggregateId $ \(a_id, seqs) ->
              annotate ("ordered (" <> show a_id <> ")") $
                sort seqs `shouldBe` seqs

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
        -- unsupported "DECIMAL"        (1.5 :: Scientific)
        -- unsupported "NUMERIC"        (1.5 :: Scientific)
      describe "Monetary" $ do
        unsupported "MONEY" (1.5 :: Double)
  where
  unsupported :: (FromJSON a, ToField a, Show a, Eq a) => String -> a -> Spec
  unsupported ty val =
    it ("unsupported " <> ty) $
      roundTrip ty val `shouldThrow` unsupportedType

  unsupportedType e
    | Just (UnsupportedType _) <- fromException e = True
    | otherwise = False

  supported :: (FromJSON a, ToField a, Show a, Eq a) => String -> a -> Spec
  supported ty val = it ty $ roundTrip ty val

  -- Write a value of a given type to a database table, then read it from the Topic.
  roundTrip :: (FromJSON a, ToField a, Show a, Eq a) => String -> a -> IO ()
  roundTrip ty val =
    with (MySQLType ty) (PartitionCount 1) $ \conn table topic connected -> do
    let record = TEntry 1 1 1 val
    insert conn table [record]
    connected $ deadline (seconds 1) $ do
      Topic.withConsumer topic group $ \consumer -> do
        (entry, _) <- readEntry consumer
        entry `shouldBe` record

  with
    :: Table t
    => Config t
    -> PartitionCount
    -> (Connection -> t -> Topic -> (IO b -> IO b) -> IO a)
    -> IO a
  with conf partitions f =
    OnDemand.with c $ \cinfo ->                                    -- load db
    withConnection cinfo $ \conn ->
    withTable conf conn $ \table ->                                -- create events table
    withFileTopic partitions $ \topic ->                           -- create topic
    let config = mkMySQL cinfo table in
    Topic.withProducer topic partitioner encoder $ \producer -> do -- create topic producer
    let logger = plainLogger Warn
        connected act = -- setup connector
          Connector.connect config logger def producer (const act)
    f conn table topic connected

group :: Topic.ConsumerGroupName
group = Topic.ConsumerGroupName "test_group"

readEntry :: Aeson.FromJSON a => Topic.Consumer -> IO (a, Topic.Meta)
readEntry consumer = do
  result <- Topic.read consumer
  (bs, meta) <- either (throwIO . ErrorCall . show) return result
  case Aeson.eitherDecode $ LB.fromStrict bs of
    Left err -> throwIO $ ErrorCall $ "decoding error: " <> show err
    Right val -> return (val, meta)

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

class Table a where
  type Entry a = b | b -> a
  type Config a = b | b -> a
  withTable :: Config a -> Connection -> (a -> IO b) -> IO b
  tableCols :: a -> [Text.Text]
  tableName :: a -> String
  -- Mock events to be added to the database.
  -- Each sublist is an infinite list of events for the same aggregate.
  mocks :: [[Entry a]]
  insert :: Connection -> a -> [Entry a] -> IO ()

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

instance FromRow Event where
  rowParser = Event <$> parseField <*> parseField <*> parseField

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

{-# NOINLINE tableNumber #-}
tableNumber :: MVar Int
tableNumber = unsafePerformIO (newMVar 0)

-- | Creates a new table on every invocation.
withMySQLTable :: Connection -> Schema -> (String -> IO a) -> IO a
withMySQLTable conn schema f = bracket create destroy f
  where
  execute q = void $ execute_ conn (fromString q)
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


data TTable a = TTable
  { tt_name :: String
  , _tt_tyName :: String
  }

data TEntry a = TEntry
  { te_id :: Int
  , te_aggregate_id :: Int
  , te_sequence_number :: Int
  , te_value :: a
  }
  deriving (Eq, Show, Generic, Functor)

instance FromJSON a => FromJSON (TEntry a) where
  parseJSON = Aeson.genericParseJSON opt
    where
    opt = Aeson.defaultOptions
      { Aeson.fieldLabelModifier = \label ->
        fromMaybe label (stripPrefix "te_" label)
      }

newtype MySQLType a = MySQLType String

instance ToField a => Table (TTable a) where
  type Entry (TTable a) = TEntry a
  type Config (TTable a) = MySQLType a
  tableName (TTable name _) = name
  tableCols _ = ["id", "aggregate_id", "sequence_number", "value"]
  mocks = error "no mocks for TTable"
  insert conn t entries =
    void $ executeMany conn query [(agg_id, seq_num, val) | TEntry _ agg_id seq_num val <- entries ]
    where
    query = fromString $ unwords
      [ "INSERT INTO", tt_name t
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
