module Ambar.Emulator.Connector.MySQL
  ( MySQL(..)
  , MySQLState
  , MySQLRow
  ) where

import Control.Concurrent.STM (STM, TVar, newTVarIO, readTVar)
import Control.Exception (Exception(..), throwIO)
import Data.Aeson (FromJSON, ToJSON)
import qualified Data.Aeson as Aeson
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as LB
import Data.Default (Default(..))
import Data.List ((\\))
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.String (fromString)
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import Data.Text (Text)
import Data.Word (Word16)
import Data.Void (Void)
import GHC.Generics (Generic)
import qualified Prettyprinter as Pretty
import Prettyprinter (pretty, (<+>))

import qualified Ambar.Emulator.Connector as C
import Ambar.Emulator.Connector.Poll (BoundaryTracker, Boundaries(..), EntryId(..), Stream)
import qualified Ambar.Emulator.Connector.Poll as Poll
import Ambar.Emulator.Queue.Topic (Producer, hashPartitioner)
import Ambar.Record (Record(..), Value(..), Bytes(..), TimeStamp(..), Type(..))
import qualified Ambar.Record.Encoding as Encoding

import Database.MySQL
  ( FromField(..)
  , Connection
  , ConnectionInfo(..)
  , RowParser
  , FieldParser

  , fieldInfo
  , fieldParseError
  , parseFailure
  , parseFieldWith
  , withConnection
  )
import qualified Database.MySQL as M
import Util.Delay (Duration, millis, seconds)
import Util.Async (withAsyncThrow)
import Util.Logger (SimpleLogger, logDebug, logInfo)
import Util.Prettyprinter (renderPretty, sepBy, commaSeparated, prettyJSON)

_POLLING_INTERVAL :: Duration
_POLLING_INTERVAL = millis 50

_MAX_TRANSACTION_TIME :: Duration
_MAX_TRANSACTION_TIME = seconds 120

data MySQL = MySQL
  { c_host :: Text
  , c_port :: Word16
  , c_username :: Text
  , c_password :: Text
  , c_database :: Text
  , c_table :: Text
  , c_columns :: [Text]
  , c_partitioningColumn :: Text
  , c_incrementingColumn :: Text
  }

newtype MySQLState = MySQLState BoundaryTracker
  deriving (Generic)
  deriving newtype (Default)
  deriving anyclass (FromJSON, ToJSON)

newtype MySQLRow = MySQLRow { unMySQLRow :: Record }

-- | May be thrown if we didn't expect to read that type
data UnexpectedType = UnexpectedType Type M.Type
  deriving (Show)

instance Exception UnexpectedType where
  displayException (UnexpectedType expected found) =
    unwords
      [ "Unexpected type."
      , "Expected:", show expected
      , "Found:", show found
      ]

instance C.Connector MySQL where
  type ConnectorState MySQL = MySQLState
  type ConnectorRecord MySQL = MySQLRow
  -- | A rows gets saved in the database as a JSON object with
  -- the columns specified in the config file as keys.
  encoder = LB.toStrict . Aeson.encode . Encoding.encode @Aeson.Value . unMySQLRow
  partitioner = hashPartitioner partitioningValue
  connect = connect

partitioningValue :: MySQLRow -> Value
partitioningValue (MySQLRow (Record row)) = snd $ row !! 1

newtype Schema = Schema (Map Text Type)
  deriving Show

-- | All MySQL types
newtype MySQLType = MySQLType { unMySQLType :: Text }
  deriving (Show, Eq)
  deriving newtype (FromField)

connect
  :: MySQL
  -> SimpleLogger
  -> MySQLState
  -> Producer MySQLRow
  -> (STM MySQLState -> IO a)
  -> IO a
connect config@MySQL{..} logger (MySQLState tracker) producer f =
  withConnection cinfo $ \conn -> do
  schema <- fetchSchema c_table conn
  validate config schema
  trackerVar <- newTVarIO tracker
  let readState = MySQLState <$> readTVar trackerVar
  withAsyncThrow (consume conn schema trackerVar) (f readState)
  where
  cinfo = ConnectionInfo
    { conn_host = c_host
    , conn_port = c_port
    , conn_username = c_username
    , conn_password = c_password
    , conn_database = c_database
    }

  fetchSchema :: Text -> Connection -> IO Schema
  fetchSchema table conn = do
    cols <- M.query_ conn $ fromString $ "DESCRIBE " <> Text.unpack c_database <> "." <> Text.unpack table
    Schema . Map.fromList <$> traverse fromCol cols
    where
    fromCol :: (Text, MySQLType, Maybe Bool, Maybe Text, Maybe Text, Maybe Text) -> IO (Text, Type)
    fromCol (field, ty, _, _, _, _) =
      case toExpectedType ty of
        Nothing -> throwIO $ C.UnsupportedType (Text.unpack $ unMySQLType ty)
        Just t -> return (field, t)

  consume
     :: Connection
     -> Schema
     -> TVar BoundaryTracker
     -> IO Void
  consume conn schema trackerVar = Poll.connect trackerVar pc
    where
    pc = Poll.PollingConnector
       { Poll.c_getId = entryId
       , Poll.c_poll = run
       , Poll.c_pollingInterval = _POLLING_INTERVAL
       , Poll.c_maxTransactionTime = _MAX_TRANSACTION_TIME
       , Poll.c_producer = producer
       }

    parser :: RowParser MySQLRow
    parser = mkParser (columns config) schema

    run :: Boundaries -> Stream MySQLRow
    run (Boundaries bs) acc0 emit = do
      logDebug logger query
      (acc, count) <- M.foldWith_ conn parser (fromString query) (acc0, 0 :: Int) $
        \(acc, !count) row -> do
          logResult row
          acc' <- emit acc row
          return (acc', succ count)
      logDebug logger $ "results: " <> show count
      return acc
      where
      query = fromString $ Text.unpack $ renderPretty $ Pretty.fillSep
         [ "SELECT" , commaSeparated $ map pretty $ columns config
         , "FROM" , pretty c_table
         , if null bs then "" else "WHERE" <> constraints
         , "ORDER BY" , pretty c_incrementingColumn
         ]

      constraints = sepBy "AND"
         [ Pretty.fillSep
            [ "("
            , pretty c_incrementingColumn, "<", pretty low
            , "OR"
            ,  pretty high, "<", pretty c_incrementingColumn
            , ")"]
         | (EntryId low, EntryId high) <- bs
         ]

      logResult row =
       logInfo logger $ renderPretty $
         "ingested." <+> commaSeparated
           [ "serial_value:" <+> prettyJSON (serialValue row)
           , "partitioning_value:" <+> prettyJSON (partitioningValue row)
           ]

-- | Columns in the order they will be queried.
columns :: MySQL -> [Text]
columns MySQL{..} =
  [c_incrementingColumn, c_partitioningColumn] <>
    ((c_columns \\ [c_incrementingColumn]) \\ [c_partitioningColumn])

entryId :: MySQLRow -> EntryId
entryId row =
  case serialValue row of
    UInt n -> EntryId $ fromIntegral n
    Int n -> EntryId $ fromIntegral n
    val -> error "Invalid serial column value:" (show val)

serialValue :: MySQLRow -> Value
serialValue (MySQLRow (Record row)) =
  case row of
    [] -> error "serialValue: empty row"
    (_,x):_ -> x

validate :: MySQL -> Schema -> IO ()
validate _ _ = do
  -- TODO: Validate
  return ()

mkParser :: [Text] -> Schema -> RowParser MySQLRow
mkParser cols (Schema schema) = do
  values <- mapM parse cols
  return $ MySQLRow $ Record $ zip cols values
  where
  parse :: Text -> RowParser Value
  parse cname = do
    ty <- case Map.lookup cname schema of
      Nothing -> fail $ "unknown column: " <> Text.unpack cname
      Just v -> return v
    parseFieldWith $ do
      (field, mbs) <- fieldInfo
      case mbs of
        Nothing -> return Null
        Just bs -> parseWithType field bs ty

parseWithType :: M.Field -> ByteString -> Type -> FieldParser Value
parseWithType field content expected = case expected of
  TBoolean   -> error "non-existent MySQL base type"
  TUInteger  -> UInt `ifType` [M.Tiny, M.Short, M.Long, M.Int24, M.LongLong]
  TInteger   -> Int `ifType` [M.Tiny, M.Short, M.Long, M.Int24, M.LongLong]
  TReal      -> Real `ifType` [M.Decimal, M.Long, M.Float, M.Double, M.NewDecimal]
  TString    -> String `ifType` stringTypes
  TBytes     -> (Binary . Bytes) `ifType` stringTypes
  TJSON      -> json `ifType_` [M.Json]
  TDateTime  -> datetime  `ifType_` [M.DateTime, M.Timestamp]
  where
  stringTypes = [M.VarChar,M.VarString,M.String,M.Set,M.Enum,M.Json,M.TinyBlob,M.MediumBlob,M.LongBlob,M.Blob]

  datetime = do
    utc <- fieldParser
    return $ DateTime $ TimeStamp (Text.decodeUtf8 content) utc

  json =
    case Aeson.eitherDecode' $ LB.fromStrict content of
      Left err -> fieldParseError $ M.ConversionFailed
        { M.errSQLType = show sqlType
        , M.errHaskellType = "Aeson.Value"
        , M.errFieldName = Text.unpack $ Text.decodeUtf8 $ M.fieldName field
        , M.errMessage = "Unable to decode JSON input: " <> err
        }
      Right v -> return $ Json v

  ifType_ :: FieldParser Value -> [M.Type] -> FieldParser Value
  ifType_ parser xs =
    if sqlType `elem` xs
    then parser
    else unexpected

  ifType :: forall a. FromField a => (a -> Value) -> [M.Type] -> FieldParser Value
  ifType f xs = (f <$> fieldParser) `ifType_` xs

  sqlType = M.fieldType field

  unsupported = parseFailure $ C.UnsupportedType $ show sqlType

  mismatch = parseFailure $ UnexpectedType expected sqlType

  unexpected = case sqlType of
    M.Null       -> pure Null
    M.Date       -> unsupported
    M.Time       -> unsupported
    M.Geometry   -> unsupported
    M.Year       -> unsupported
    M.NewDate    -> unsupported
    M.Bit        -> unsupported
    M.Enum       -> unsupported
    M.Set        -> unsupported
    M.Decimal    -> mismatch
    M.Long       -> mismatch
    M.Float      -> mismatch
    M.Double     -> mismatch
    M.NewDecimal -> mismatch
    M.Timestamp  -> mismatch
    M.Tiny       -> mismatch
    M.Short      -> mismatch
    M.LongLong   -> mismatch
    M.Int24      -> mismatch
    M.DateTime   -> mismatch
    M.TinyBlob   -> mismatch
    M.MediumBlob -> mismatch
    M.LongBlob   -> mismatch
    M.Blob       -> mismatch
    M.VarChar    -> mismatch
    M.VarString  -> mismatch
    M.String     -> mismatch
    M.Json       -> mismatch

toExpectedType :: MySQLType -> Maybe Type
toExpectedType (MySQLType sqlType)
  | oneOf integers && contains "UNSIGNED" = Just TUInteger
  | oneOf integers = Just TInteger
  | oneOf reals = Just TReal
  | oneOf ["DATETIME", "TIMESTAMP"] = Just TDateTime
  | oneOf text = Just TString
  | oneOf binary = Just TBytes
  | oneOf ["JSON"] = Just TJSON
  | oneOf unsupported = Nothing
  | otherwise = Nothing
  where
  ty = Text.toUpper sqlType
  oneOf xs = any hasPrefix xs
  hasPrefix v = v `Text.isPrefixOf` ty
  contains v = v `Text.isInfixOf` ty
  integers = ["BIGINT", "INT", "INTEGER", "MEDIUMINT", "SMALLINT", "TINYINT"]
  reals = ["DECIMAL", "DEC", "NUMERIC", "FIXED", "FLOAT", "DOUBLE"]
  text = ["CHAR", "NATIONAL CHAR", "TEXT", "CHARACTER", "TINYTEXT", "MEDIUMTEXT","LONGTEXT", "ENUM", "SET"]
  binary = ["BINARY", "VARBINARY", "BLOB","TINYBLOB", "MEDIUMBLOB", "LONGBLOB"]
  unsupported = ["BIT"]
