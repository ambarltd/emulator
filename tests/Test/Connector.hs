{-# OPTIONS_GHC -Wno-x-partial #-}
module Test.Connector
  ( testConnectors
  , PostgresCreds
  , withPostgresSQL
  , withEventsTable
  , mkPostgreSQL
  , Event(..)
  , Table(..)

  , BytesRow(..)
  ) where

import Control.Concurrent (MVar, newMVar, modifyMVar)
import Control.Concurrent.Async (mapConcurrently)
import qualified Data.Aeson as Aeson
import Data.Aeson (FromJSON)
import qualified Data.ByteString.Lazy as LB
import Data.Default (def)
import Data.List (isInfixOf, sort, stripPrefix, intersperse, (\\))
import qualified Data.Map.Strict as Map
import Data.Maybe (fromMaybe)
import Data.String (fromString)
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import Data.Word (Word16)
import Control.Exception (bracket, throwIO, ErrorCall(..))
import Control.Monad (void, replicateM, forM_)
import System.Exit (ExitCode(..))
import System.Process (readProcessWithExitCode)
import Test.Hspec
  ( Spec
  , it
  , describe
  , shouldBe
  )
import Test.Hspec.Expectations.Contrib (annotate)
import qualified Database.PostgreSQL.Simple as P
import qualified Database.PostgreSQL.Simple.ToField as P
import GHC.Generics
import System.IO.Unsafe (unsafePerformIO)

import qualified Ambar.Emulator.Connector as Connector
import Ambar.Emulator.Connector (partitioner, encoder)
import Ambar.Emulator.Connector.Poll (Boundaries(..), mark, boundaries, cleanup)
import Ambar.Emulator.Connector.Postgres (PostgreSQL(..))
import Ambar.Emulator.Queue.Topic (Topic, PartitionCount(..))
import qualified Ambar.Emulator.Queue.Topic as Topic
import Ambar.Record (Bytes(..))
import Test.Queue (withFileTopic)
import Test.Utils.OnDemand (OnDemand)
import qualified Test.Utils.OnDemand as OnDemand

import Utils.Async (withAsyncThrow)
import Utils.Delay (deadline, seconds)
import Utils.Logger (plainLogger, Severity(..))

testConnectors :: OnDemand PostgresCreds -> Spec
testConnectors creds = do
  describe "connector" $ do
    testPollingConnector
    testPostgreSQL creds

testPollingConnector :: Spec
testPollingConnector = describe "Poll" $
  describe "rangeTracker" $ do
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
      (boundaries . cleanup 2 . bs) [1 ,3, 5, 7] `shouldBe` Boundaries [(5,5), (7,7)]

    it "cleanup doesn't remove ranges ending higher than time given" $ do
      (boundaries . cleanup 2 . bs) [1 ,2, 5, 3] `shouldBe` Boundaries [(1,3), (5,5)]

testPostgreSQL :: OnDemand PostgresCreds -> Spec
testPostgreSQL p = do
  describe "PostgreSQL" $ do
    -- checks that our tests can connect to postgres
    it "connects" $
      with (PartitionCount 1) $ \conn table _ _ -> do
        insert conn table (take 10 $ head mocks)
        rs <- P.query_ @Event conn (fromString $ "SELECT * FROM " <> tableName table)
        length rs `shouldBe` 10

    it "retrieves all events already in the db" $
      with (PartitionCount 1) $ \conn table topic connected -> do
        let count = 10
        insert conn table (take count $ head mocks)
        connected $
          deadline (seconds 2) $
          Topic.withConsumer topic group $ \consumer -> do
          es <- replicateM count $ readEvent consumer
          length es `shouldBe` count

    it "can retrieve a large number of events" $
      with (PartitionCount 1) $ \conn table topic connected -> do
        let count = 10_000
        insert conn table (take count $ head mocks)
        connected $ deadline (seconds 2) $
          Topic.withConsumer topic group $ \consumer -> do
          es <- replicateM count $ readEvent consumer
          length es `shouldBe` count

    it "retrieves events added after initial snapshot" $
      with (PartitionCount 1) $ \conn table topic connected -> do
        let count = 10
            write = insert conn table (take count $ head mocks)
        connected $ deadline (seconds 1) $
          Topic.withConsumer topic group $ \consumer ->
          withAsyncThrow write $ do
          es <- replicateM count $ readEvent consumer
          length es `shouldBe` count

    it "maintains ordering through parallel writes" $ do
      let partitions = 5
      with (PartitionCount partitions) $ \conn table topic connected -> do
        let count = 1_000
            write = mapConcurrently id
              [ insert conn table (take count $ mocks !! partition)
              | partition <- [1..partitions] ]
        connected $ deadline (seconds 1) $
          Topic.withConsumer topic group $ \consumer -> do
          -- write and consume concurrently
          withAsyncThrow write $ do
            es <- replicateM (count * partitions) $ readEvent consumer
            let byAggregateId = Map.toList $ Map.fromListWith (flip (++))
                    [ (e_aggregate_id, [e_sequence_number])
                    | (Event{..}, _) <- es
                    ]
            forM_ byAggregateId $ \(a_id, seqs) ->
              annotate ("ordered (" <> show a_id <> ")") $
                sort seqs `shouldBe` seqs
    it "decodes types" $
      with_ ((),()) (PartitionCount 1) $ \conn table topic connected -> do
      [record] <- return $ concat $ take 1 <$> take 1 mocks
      insert conn table [record]
      connected $ deadline (seconds 1) $
        Topic.withConsumer topic group $ \consumer -> do
          (entry, _) <- readEntry @TypesRecord consumer
          let filledRecord = record
                { tr_serial = 1
                , tr_bigserial = 1
                , tr_id = 1
                , tr_smallserial = 1
                }
          entry `shouldBe` filledRecord

    describe "decodes" $ do
      roundTrip "BYTEA" (BytesRow (Bytes "AAAA"))

  where
  readEvent = readEntry @Event
  with = with_ ()

  roundTrip :: (FromJSON a, P.ToField a, Show a, Eq a) => String -> a -> Spec
  roundTrip ty val = it ty $
    with_ (TTableConfig ty) (PartitionCount 1) $ \conn table topic connected -> do
    let record = TEntry 1 1 1 val
    insert conn table [record]
    connected $ deadline (seconds 1) $
      Topic.withConsumer topic group $ \consumer -> do
        (entry, _) <- readEntry consumer
        entry `shouldBe` record

  with_
    :: Table t
    => Config t
    -> PartitionCount
    -> (P.Connection -> t -> Topic -> (IO b -> IO b) -> IO a)
    -> IO a
  with_ conf partitions f =
    OnDemand.with p $ \creds ->                                    -- load db
    withTable conf creds $ \conn table ->                          -- create events table
    withFileTopic partitions $ \topic ->                           -- create topic
    let config = mkPostgreSQL creds table in
    Topic.withProducer topic partitioner encoder $ \producer -> do -- create topic producer
    let logger = plainLogger Warn
        connected act = -- setup connector
          Connector.connect config logger def producer (const act)
    f conn table topic connected

readEntry :: Aeson.FromJSON a => Topic.Consumer -> IO (a, Topic.Meta)
readEntry consumer = do
  result <- Topic.read consumer
  (bs, meta) <- either (throwIO . ErrorCall . show) return result
  case Aeson.eitherDecode $ LB.fromStrict bs of
    Left err -> throwIO $ ErrorCall $ "decoding error: " <> show err
    Right val -> return (val, meta)

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
  deriving (Eq, Show, Generic, P.FromRow)

instance Aeson.FromJSON Event where
  parseJSON = Aeson.genericParseJSON opt
    where
    opt = Aeson.defaultOptions
      { Aeson.fieldLabelModifier = \label ->
        fromMaybe label (stripPrefix "e_" label)
      }

class Table a where
  type Entry a = b | b -> a
  type Config a = b | b -> a
  withTable :: Config a -> PostgresCreds -> (P.Connection -> a -> IO b) -> IO b
  tableCols :: a -> [Text.Text]
  tableName :: a -> String
  -- Mock events to be added to the database.
  -- Each sublist is an infinite list of events for the same aggregate.
  mocks :: [[Entry a]]
  insert :: P.Connection -> a -> [Entry a] -> IO ()

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

data TTableConfig a = TTableConfig
  { _ttc_typePostgres :: String
  }

instance P.ToField a => Table (TTable a) where
  type Entry (TTable a) = (TEntry a)
  type Config (TTable a) = TTableConfig a
  tableName (TTable name _) = name
  tableCols _ = ["id", "aggregate_id", "sequence_number", "value"]
  mocks = error "no mocks for TTable"
  insert conn t entries =
    void $ P.executeMany conn query [(agg_id, seq_num, val) | TEntry _ agg_id seq_num val <- entries ]
    where
    query = fromString $ unwords
      [ "INSERT INTO", tt_name t
      ,"(aggregate_id, sequence_number, value)"
      ,"VALUES (?, ?, ?)"
      ]
  withTable (TTableConfig ty) creds f =
    withPgTable schema creds $ \conn name -> f conn (TTable name ty)
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
    where err = error "aggregate id is determined by postgres"

  insert conn (EventsTable table) events =
    void $ P.executeMany conn query [(agg_id, seq_num) | Event _ agg_id seq_num <- events ]
    where
    query = fromString $ unwords
      [ "INSERT INTO", table
      ,"(aggregate_id, sequence_number)"
      ,"VALUES ( ?, ? )"
      ]

  withTable _ creds f =
    withPgTable schema creds $ \conn name -> f conn (EventsTable name)
    where
    schema = unwords
        [ "( id               SERIAL"
        , ", aggregate_id     INTEGER NOT NULL"
        , ", sequence_number  INTEGER NOT NULL"
        , ", PRIMARY KEY (id)"
        , ", UNIQUE (aggregate_id, sequence_number)"
        , ")"
        ]

withEventsTable :: PostgresCreds -> (P.Connection -> EventsTable -> IO a) -> IO a
withEventsTable = withTable ()

{-# NOINLINE tableNumber #-}
tableNumber :: MVar Int
tableNumber = unsafePerformIO (newMVar 0)

newtype TypesTable = TypesTable String

data TypesRecord = TypesRecord
  { tr_id              :: Int
  , tr_aggregate_id    :: Int
  , tr_sequence_number :: Int

  -- Numeric
  , tr_smallint        :: Int
  , tr_integer         :: Int
  , tr_bigint          :: Int
  , tr_real            :: Double
  , tr_double          :: Double
  , tr_smallserial     :: Int
  , tr_serial          :: Int
  , tr_bigserial       :: Int
  -- , tr_decimal      :: Scientific
  -- , tr_numeric      :: Scientific

  -- Strings
  -- , tr_character_varying_5 :: String
  -- , tr_varchar_5           :: String
  -- , tr_character_5         :: String
  -- , tr_char_5              :: String
  -- , tr_bpchar_5            :: String
  -- , tr_bpchar              :: String
  , tr_text            :: String

  -- Binary
  , tr_bytea           :: BytesRow
  }
  deriving (Eq, Show, Generic)

-- | Binary data saved in PostgreSQL.
-- Use it to read and write binary data. Does not perform base64 conversion.
newtype BytesRow = BytesRow Bytes
  deriving stock (Eq, Show)
  deriving newtype (Aeson.FromJSON, Aeson.ToJSON)

instance P.ToField BytesRow where
  toField (BytesRow (Bytes bs)) = P.toField (P.Binary bs)

trAesonOptions :: Aeson.Options
trAesonOptions = Aeson.defaultOptions
  { Aeson.fieldLabelModifier = \label ->
    fromMaybe label (stripPrefix "tr_" label)
  }

instance Aeson.FromJSON TypesRecord where
  parseJSON = Aeson.genericParseJSON trAesonOptions

instance Table TypesTable where
  type (Entry TypesTable) = TypesRecord
  type (Config TypesTable) = ((),())
  tableName (TypesTable name) = name
  tableCols _ =
    [ "id"
    , "aggregate_id"
    , "sequence_number"
    , "smallint"
    , "integer"
    , "bigint"
    , "real"
    , "double"
    , "smallserial"
    , "serial"
    , "bigserial"
    , "text"
    , "bytea"
    ]

  mocks =
    -- the aggregate_id is given when the records are inserted into the database
    [ [ TypesRecord
        { tr_id = err "id"
        , tr_aggregate_id = agg_id
        , tr_sequence_number = seq_id
        , tr_smallint    = seq_id
        , tr_integer     = seq_id
        , tr_bigint      = seq_id
        , tr_real        = fromIntegral seq_id
        , tr_double      = fromIntegral seq_id
        , tr_smallserial = err "smallserial"
        , tr_serial      = err "serial"
        , tr_bigserial   = err "bigserial"
        , tr_text        = take 5 $ str seq_id
        , tr_bytea       =
            BytesRow $ Bytes $ Text.encodeUtf8 $ Text.pack $ take 5 $ str seq_id
        }
      | seq_id <- [0..]
      ]
    | agg_id <- [0..]
    ]
    where
      err fname = error $ "field determined by postgres (" <> fname <> ")"
      chars = toEnum <$> [65..122] :: String
      str seed =
        [ head $ drop (seed * n) $ cycle chars
        | n <- [1..]
        ]

  insert conn t@(TypesTable table) events =
    void $ P.executeMany conn query
    [ ( tr_aggregate_id
      , tr_sequence_number
      , tr_smallint
      , tr_integer
      , tr_bigint
      , tr_real
      , tr_double
      , tr_text
      , tr_bytea
      )
    | TypesRecord{..} <- events
    ]
    where
    query = fromString $ unwords $ concat $
      [ [ "INSERT INTO", table ]
      , tupled cols
      , [ "VALUES"]
      , tupled $ replicate (length cols) "?"
      ]
      where
      tupled xs = ["("] ++ intersperse ", " xs ++ [")"]
      cols = fmap Text.unpack $ (tableCols t) \\ autoAssigned
      autoAssigned = ["id", "smallserial", "serial", "bigserial" ]


  withTable _ creds f =
    withPgTable schema creds $ \conn name -> f conn (TypesTable name)
    where
    schema = unwords
        [ "( id                   SERIAL"
        , ", aggregate_id         INTEGER NOT NULL"
        , ", sequence_number      INTEGER NOT NULL"
        , ", smallint             SMALLINT"
        , ", integer              INTEGER"
        , ", bigint               BIGINT"
        , ", real                 REAL"
        , ", double               DOUBLE PRECISION"
        , ", smallserial          SMALLSERIAL"
        , ", serial               SERIAL"
        , ", bigserial            BIGSERIAL"
        -- , ", character_varying_5  CHAR VARYING(5)"
        -- , ", varchar_5            VARCHAR(5)"
        -- , ", character_5          CHARACTER(5)"
        -- , ", char_5               CHAR(5)"
        -- , ", bpchar_5             BPCHAR(5)"
        , ", text                 TEXT"
        , ", bytea                BYTEA"
        , ", PRIMARY KEY (id)"
        , ", UNIQUE (aggregate_id, sequence_number)"
        , ")"
        ]

type Schema = String

-- | Creates a new table on every invocation.
withPgTable :: Schema -> PostgresCreds -> (P.Connection -> String -> IO a) -> IO a
withPgTable schema creds f = bracket create destroy $ uncurry f
  where
  execute conn q = void $ P.execute_ conn (fromString q)
  create = do
    name <- takeName
    conn <- connect
    execute conn $ unwords [ "CREATE TABLE IF NOT EXISTS", name, schema ]
    return (conn, name)

  destroy (conn, name) =
    execute conn $ "DROP TABLE " <> name

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
