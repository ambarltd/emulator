module Test.Connector
  ( testConnectors
  , withPostgresSQL
  ) where

import Control.Concurrent (MVar, newMVar, modifyMVar)
import Control.Concurrent.Async (race, withAsync, wait)
import qualified Data.Aeson as Aeson
import qualified Data.ByteString.Lazy as LB
import Data.List (isInfixOf)
import Data.String (fromString)
import qualified Data.Text as Text
import Data.Void (Void)
import Data.Word (Word16)
import Control.Exception (bracket, throwIO, ErrorCall(..))
import Control.Monad (void, replicateM)
import System.Exit (ExitCode(..))
import System.Process (readProcessWithExitCode)
import System.Timeout (timeout)
import Test.Hspec
  ( Spec
  , it
  , describe
  , shouldBe
  )
import qualified Database.PostgreSQL.Simple as P
import GHC.Generics
import System.IO.Unsafe (unsafePerformIO)

import Connector.Poll
  ( BoundaryTracker(..)
  , Boundaries(..)
  , rangeTracker
  )
import Connector.Postgres (ConnectorConfig(..), partitioner, encoder)
import qualified Connector.Postgres as ConnectorPostgres
import Queue (PartitionCount(..))
import Queue.Topic (Topic)
import qualified Queue.Topic as Topic
import Test.Queue (withFileTopic)
import Test.Utils.OnDemand (OnDemand)
import qualified Test.Utils.OnDemand as OnDemand

testConnectors :: OnDemand PostgresCreds -> Spec
testConnectors creds = do
  describe "connector" $ do
    testPollingConnector
    testPostgreSQL creds

testPollingConnector :: Spec
testPollingConnector = describe "Poll" $
  describe "rangeTracker" $ do
    BoundaryTracker mark boundaries cleanup <- return (rangeTracker :: BoundaryTracker Int)
    let bs xs = foldr (uncurry mark) mempty $ reverse $ zip [0, 1..] xs
    it "one id" $ do
      boundaries (mark 1 1 mempty) `shouldBe` Boundaries [(1,1)]

    it "disjoint ranges" $ do
      (boundaries . bs) [1, 3] `shouldBe` Boundaries [(1,1), (3,3)]

    it "extend range above" $ do
      (boundaries . bs) [1, 2] `shouldBe` Boundaries [(1,2)]

    it "extend range below" $ do
      (boundaries . bs) [2, 1] `shouldBe` Boundaries [(1,2)]

    it "detects inside range bottom" $ do
      (boundaries . bs) [1,2,3,1] `shouldBe` Boundaries [(1,3)]

    it "detects inside range middle" $ do
      (boundaries . bs) [1,2,3,2] `shouldBe` Boundaries [(1,3)]

    it "detects inside range top" $ do
      (boundaries . bs) [1,2,3,3] `shouldBe` Boundaries [(1,3)]

    it "joins range" $ do
      (boundaries . bs) [1,3,2] `shouldBe` Boundaries [(1,3)]

    it "tracks multiple ranges" $ do
      (boundaries . bs) [10, 1,5,3,2,4] `shouldBe` Boundaries [(1,5), (10,10)]

    it "cleanup removes ranges with ending lower than time given" $ do
      (boundaries . cleanup 1 . bs) [1 ,3, 5, 7] `shouldBe` Boundaries [(5,5), (7,7)]

    it "cleanup doesn't remove ranges ending higher than time given" $ do
      (boundaries . cleanup 1 . bs) [1 ,2, 5, 3] `shouldBe` Boundaries [(1,3), (5,5)]

testPostgreSQL :: OnDemand PostgresCreds -> Spec
testPostgreSQL p = do
  describe "PostgreSQL" $ do
    -- checks that our tests can connect to postgres
    it "connects" $
      with $ \conn table _ _ -> do
        insert conn table (take 10 $ head mocks)
        rs <- P.query_ @Event conn (fromString $ "SELECT * FROM " <> table)
        length rs `shouldBe` 10

    it "retrieves all events already in the db" $
      with $ \conn table topic connect -> do
        let count = 10
        insert conn table (take count $ head mocks)
        withAsync_ connect $
          timeout_ (seconds 2) $
          Topic.withConsumer topic group $ \consumer -> do
          es <- replicateM count $ readEvent consumer
          length es `shouldBe` count

    it "can retrieve a large number of events" $ () `shouldBe` ()
    it "retrieves events added after initial snapshot" $ () `shouldBe` ()
    it "maintains ordering" $ () `shouldBe` ()
  where
  readEvent :: Topic.Consumer -> IO (Event, Topic.Meta)
  readEvent consumer = do
    result <- Topic.read consumer
    (bs, meta) <- either (throwIO . ErrorCall . show) return result
    case Aeson.eitherDecode $ LB.fromStrict bs of
      Left err -> throwIO $ ErrorCall $ "Event decoding error: " <> show err
      Right val -> return (val, meta)

  with :: (P.Connection -> TableName -> Topic -> IO Void -> IO a) -> IO a
  with f =
    OnDemand.with p $ \creds ->                                           -- load db
    withEventsTable creds $ \conn table ->                                -- create events table
    withFileTopic (PartitionCount 1) $ \topic ->                          -- create topic
    let config = mkConfig creds table in
    Topic.withProducer topic partitioner (encoder config) $ \producer ->  -- create topic producer
    let connect = ConnectorPostgres.connect producer config in            -- setup connector
    f conn table topic connect

  seconds n = n * 1_000_000 -- one second in microseconds

-- version of timeout that throws on timeout
timeout_ :: Int -> IO a -> IO a
timeout_ time act = do
  r <- timeout time act
  case r of
    Nothing -> error "timed out"
    Just v -> return v

-- version of withAsync which throws if left throws
withAsync_ :: IO a -> IO b -> IO b
withAsync_ left right =
  withAsync right $ \r -> do
    out <- race (do _ <- left; wait r) (wait r)
    return $ either id id out

mkConfig :: PostgresCreds -> TableName -> ConnectorPostgres.ConnectorConfig
mkConfig PostgresCreds{..} table = ConnectorConfig
  { c_host = Text.pack p_host
  , c_port = p_port
  , c_username = Text.pack p_username
  , c_password = Text.pack p_password
  , c_database = Text.pack p_database
  , c_table = Text.pack table
  , c_columns = ["id", "aggregate_id", "sequence_number"]
  , c_partitioningColumn = "aggregate_id"
  , c_serialColumn = "id"
  }

group :: Topic.ConsumerGroupName
group = Topic.ConsumerGroupName "test_group"

data PostgresCreds = PostgresCreds
  { p_database :: String
  , p_username :: String
  , p_password :: String
  , p_host :: String
  , p_port :: Word16
  }

data Event = Event
  { e_id :: Int
  , e_aggregate_id :: Int
  , e_sequence_number :: Int
  }
  deriving (Generic, P.FromRow, Show)

instance Aeson.FromJSON Event where
  parseJSON = Aeson.withObject "Event" $ \o -> do
    e_id <- o Aeson..: "id"
    e_aggregate_id <- o Aeson..: "aggregate_id"
    e_sequence_number <- o Aeson..: "sequence_number"
    return Event{..}

-- Mock events to be added to the database.
-- Each sublist is an infinite list of events for the same aggregate.
mocks :: [[Event]]
mocks =
  -- the aggregate_id is given when the records are inserted into the database
  [ [ Event (-1) agg_id seq_id | seq_id <- [0..] ]
    | agg_id <- [0..]
  ]

insert :: P.Connection -> TableName -> [Event] -> IO ()
insert conn table events =
  void $ P.executeMany conn query [(agg_id, seq_num) | Event _ agg_id seq_num <- events ]
  where
  query = fromString $ unwords
    [ "INSERT INTO", table
    ,"(aggregate_id, sequence_number)"
    ,"VALUES ( ?, ? )"
    ]

{-# NOINLINE tableNumber #-}
tableNumber :: MVar Int
tableNumber = unsafePerformIO (newMVar 0)

type TableName = String

withEventsTable :: PostgresCreds -> (P.Connection -> TableName -> IO a) -> IO a
withEventsTable creds f = bracket create destroy $ uncurry f
  where
  execute conn q = void $ P.execute_ conn (fromString q)
  create = do
    name <- takeName
    conn <- connect
    execute conn $ unwords
      [ "CREATE TABLE IF NOT EXISTS " <> name <> " "
      , "( id               SERIAL"
      , ", aggregate_id     INTEGER NOT NULL"
      , ", sequence_number  INTEGER NOT NULL"
      , ", PRIMARY KEY (id)"
      , ", UNIQUE (aggregate_id, sequence_number)"
      , ")"
      ]
    return (conn, name)

  destroy (conn, name) = execute conn $ "DROP TABLE " <> name

  takeName = do
    number <- modifyMVar tableNumber $ \n -> return (n + 1, n)
    return $ "table_" <> show number

  connect = P.connect P.ConnectInfo
    { P.connectUser = p_username creds
    , P.connectPassword = p_password creds
    , P.connectDatabase = p_database creds
    , P.connectHost = p_host creds
    , P.connectPort = p_port creds
    }

-- | Create a PostgreSQL database and delete it upon completion.
withPostgresSQL :: (PostgresCreds -> IO a) -> IO a
withPostgresSQL f = bracket setup teardown f
  where
  setup = do
    let creds@PostgresCreds{..} = PostgresCreds
          { p_database = "db_test"
          , p_username = "conn_test"
          , p_password = ""
          , p_host =  P.connectHost P.defaultConnectInfo
          , p_port = P.connectPort P.defaultConnectInfo
          }
    createUser p_username
    createDatabase p_username p_database
    return creds

  teardown PostgresCreds{..} = do
    deleteDatabase p_database
    dropUser p_username

  createUser name = do
    (code, _, err) <- readProcessWithExitCode "createuser" ["--superuser", name] ""
    case code of
      ExitSuccess -> return ()
      ExitFailure 1 | "already exists" `isInfixOf` err -> return ()
      _ -> throwIO $ ErrorCall $ "Unable to create PostgreSQL user: " <> err

  createDatabase user name = do
    (code, _, err) <- readProcessWithExitCode "createdb"
      [ "--username", user
      , "--no-password", name
      ] ""
    case code of
      ExitSuccess -> return ()
      ExitFailure 1 | "already exists" `isInfixOf` err -> return ()
      _ -> throwIO $ ErrorCall $ "Unable to create PostgreSQL database: " <> err

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
