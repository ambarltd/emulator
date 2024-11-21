{-# OPTIONS_GHC -Wno-x-partial #-}
module Test.Connector
  ( testConnectors
  , PostgresCreds
  , withPostgreSQL
  , withEventsTable
  , mkPostgreSQL
  , Event(..)
  , Table(..)

  , BytesRow(..)
  , Debugging(..)
  ) where

import Control.Concurrent (MVar, newMVar, modifyMVar)
import Control.Concurrent.Async (mapConcurrently)
import Control.Exception (bracket, throwIO, ErrorCall(..), fromException)
import Control.Monad (void, replicateM, forM_)
import qualified Data.Aeson as Aeson
import Data.Aeson (FromJSON)
import qualified Data.ByteString.Lazy as LB
import Data.Default (def)
import Data.List (isInfixOf, sort, stripPrefix)
import qualified Data.Map.Strict as Map
import Data.Maybe (fromMaybe)
import Data.Scientific (Scientific)
import Data.String (fromString)
import qualified Data.Text as Text
import Data.Word (Word16)
import System.Exit (ExitCode(..))
import System.IO.Temp (withSystemTempFile)
import System.IO
  ( Handle
  , IOMode(..)
  , BufferMode(..)
  , hClose
  , hGetContents
  , hGetLine
  , hPutStrLn
  , hSetBuffering
  , stdout
  , withFile
  , hSetBuffering
  , hGetBuffering
  , hGetEncoding
  , hSetEncoding
  )
import System.Process
  ( CreateProcess(..)
  , StdStream(..)
  , proc
  , withCreateProcess
  , waitForProcess
  , createPipe
  )
import Test.Hspec
  ( Spec
  , it
  , describe
  , shouldBe
  , shouldThrow
  )
import Test.Hspec.Expectations.Contrib (annotate)
import qualified Database.PostgreSQL.Simple as P
import qualified Database.PostgreSQL.Simple.ToField as P
import GHC.Generics
import System.IO.Unsafe (unsafePerformIO)

import qualified Ambar.Emulator.Connector as Connector
import Ambar.Emulator.Connector (partitioner, encoder)
import Ambar.Emulator.Connector.Poll (Boundaries(..), mark, boundaries, cleanup)
import Ambar.Emulator.Connector.Postgres (PostgreSQL(..), UnsupportedType(..))
import Ambar.Emulator.Queue.Topic (Topic, PartitionCount(..))
import qualified Ambar.Emulator.Queue.Topic as Topic
import Ambar.Record (Bytes(..))
import Test.Queue (withFileTopic)
import Test.Utils.OnDemand (OnDemand)
import qualified Test.Utils.OnDemand as OnDemand

import Utils.Async (withAsyncThrow)
import Utils.Delay (deadline, seconds, delay)
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
          es <- replicateM count $ readEntry @Event consumer
          length es `shouldBe` count

    it "can retrieve a large number of events" $
      with (PartitionCount 1) $ \conn table topic connected -> do
        let count = 10_000
        insert conn table (take count $ head mocks)
        connected $ deadline (seconds 2) $
          Topic.withConsumer topic group $ \consumer -> do
          es <- replicateM count $ readEntry @Event consumer
          length es `shouldBe` count

    it "retrieves events added after initial snapshot" $
      with (PartitionCount 1) $ \conn table topic connected -> do
        let count = 10
            write = insert conn table (take count $ head mocks)
        connected $ deadline (seconds 1) $
          Topic.withConsumer topic group $ \consumer ->
          withAsyncThrow write $ do
          es <- replicateM count $ readEntry @Event consumer
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
  with = with_ ()

  withType ty definition act =
    OnDemand.with p $ \creds ->
    withConnection creds $ \conn -> do
      let create = P.execute_ conn definition
          destroy _ = P.execute_ conn $ "DROP TYPE " <> ty
      bracket create destroy (const act)

  unsupported :: (FromJSON a, P.ToField a, Show a, Eq a) => String -> a -> Spec
  unsupported ty val =
    it ("unsupported " <> ty) $
      roundTrip ty val `shouldThrow` unsupportedType

  unsupportedType e
    | Just (UnsupportedType _) <- fromException e = True
    | otherwise = False

  supported :: (FromJSON a, P.ToField a, Show a, Eq a) => String -> a -> Spec
  supported ty val = it ty $ roundTrip ty val

  -- Write a value of a given type to a database table, then read it from the Topic.
  roundTrip :: (FromJSON a, P.ToField a, Show a, Eq a) => String -> a -> IO ()
  roundTrip ty val =
    with_ (PostgresType ty) (PartitionCount 1) $ \conn table topic connected -> do
    let record = TEntry 1 1 1 val
    insert conn table [record]
    connected $ deadline (seconds 1) $ do
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
    withConnection creds $ \conn ->
    withTable conf conn $ \table ->                                -- create events table
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
  withTable :: Config a -> P.Connection -> (a -> IO b) -> IO b
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

newtype PostgresType a = PostgresType String

instance P.ToField a => Table (TTable a) where
  type Entry (TTable a) = TEntry a
  type Config (TTable a) = PostgresType a
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

withEventsTable :: PostgresCreds -> (P.Connection -> EventsTable -> IO a) -> IO a
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

newtype Debugging = Debugging Bool

withPostgreSQL :: Debugging -> (PostgresCreds -> IO a) -> IO a
withPostgreSQL (Debugging debug) f = do
  let cmd = DockerRun
        { run_image = "postgres:14.10"
        , run_args =
          [ "--env", "POSTGRES_PASSWORD=" <> p_password
          , "--env", "POSTGRES_USER=" <> p_username
          , "--env", "POSTGRES_DB=" <> p_database
          , "--publish",  show p_port <> ":5432"
          ]
        }
  r <- withDocker "PostgreSQL" cmd $ \h ->
    debugging h $ \h' -> do
    waitTillReady h'
    f creds
  return r
  where
  debugging h act =
    if debug
    then tracing "PostgreSQL" h act
    else act h

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

data DockerCommand
  = DockerRun
    { run_image :: String
    , run_args :: [String]
    }

{-# NOINLINE dockerImageNumber #-}
dockerImageNumber :: MVar Int
dockerImageNumber = unsafePerformIO (newMVar 0)

-- | Run a command with a docker image running in the background.
-- Automatically assigns a container name and removes the container
-- on exit.
withDocker :: String -> DockerCommand -> (Handle -> IO a) -> IO a
withDocker tag cmd act =
  withPipe $ \hread hwrite -> do
  name <- mkName
  let create = (proc "docker" (args name))
        { std_out = UseHandle hwrite
        , std_err = UseHandle hwrite
        , create_group = True
        }
  withCreateProcess create $ \_ _ _ p ->
    withAsyncThrow (wait name p) $
    act hread
  where
  withPipe f = do
    (hread, hwrite) <- createPipe
    hSetBuffering hread LineBuffering
    hSetBuffering hwrite LineBuffering
    f hread hwrite

  wait name p = do
    exit <- waitForProcess p
    throwIO $ ErrorCall $ case exit of
      ExitSuccess ->  "unexpected successful termination of container " <> name
      ExitFailure code ->
        "docker failed with exit code" <> show code <> " for container " <> name

  mkName = do
    number <- modifyMVar dockerImageNumber $ \n -> return (n + 1, n)
    return $ tag <> "_" <> show number

  args :: String -> [String]
  args name =
    case cmd of
      DockerRun img opts -> ["run", "--init", "--rm", "--name", name] ++ opts ++ [img]

  _withHandle name f = do
    let debug = False -- set to true to print Docker output to sdtdout
        mhandle =
          if debug
          then Just stdout
          else Nothing
    case mhandle of
      Just handle -> f handle
      Nothing -> withSystemTempFile (name <> "-output") (\path h0 -> do
        hClose h0
        withFile path ReadWriteMode f
        )

-- | Log a handle's content to stdout.
tracing :: String -> Handle -> (Handle -> IO a) -> IO a
tracing name h f = censoring h logIt f
  where
  logIt str = do
    putStrLn $ name <> ": " <> str
    return (Just str)

-- | Perform an action before any line of content goes from one thread to the other
censoring :: Handle -> (String -> IO (Maybe String)) -> (Handle -> IO a) -> IO a
censoring h censor f = do
  buffering <- hGetBuffering h
  mencoding <- hGetEncoding h
  (hread, hwrite) <- createPipe

  forM_ [hread, hwrite] $ \h' -> do
    hSetBuffering h' buffering
    forM_ mencoding (hSetEncoding h')

  let worker = do
        str <- hGetContents h
        forM_ (lines str) $ \line -> do
          r <- censor line
          forM_ r (hPutStrLn hwrite)

  withAsyncThrow worker (f hread)

