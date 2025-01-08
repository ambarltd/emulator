module Ambar.Emulator.Connector.MicrosoftSQLServer
  ( SQLServer(..)
  , SQLServerState
  , SQLServerRow
  ) where

import Control.Concurrent.STM (STM, TVar, newTVarIO, readTVar)
import Control.Exception (throwIO)
import Control.Monad (forM)
import Data.Aeson (FromJSON, ToJSON)
import qualified Data.Aeson as Aeson
import qualified Data.ByteString.Lazy as LB
import Data.Default (Default(..))
import Data.Fixed (Fixed(MkFixed), E0)
import Data.List ((\\))
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Scientific (Scientific, toBoundedRealFloat)
import Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import Data.Time.Format.ISO8601 (iso8601Show)
import Data.Void (Void)
import Data.Word (Word8, Word16)
import GHC.Generics (Generic)
import qualified Prettyprinter as Pretty
import Prettyprinter (pretty, (<+>))

import Database.MicrosoftSQLServer
  ( Connection
  , ConnectionInfo(..)
  , withConnection
  , RowParser
  , FieldParser
  , FromField
  , fieldParser
  , mkQuery_
  , rawBytes
  )
import qualified Database.Tds.Message as Tds
import qualified Database.MicrosoftSQLServer as M

import qualified Ambar.Emulator.Connector as C
import Ambar.Emulator.Connector.Poll (BoundaryTracker, Boundaries(..), EntryId(..))
import qualified Ambar.Emulator.Connector.Poll as Poll
import Ambar.Emulator.Queue.Topic (Producer, hashPartitioner)
import Ambar.Record (Record(..), Value(..), Type(..), Bytes(..), TimeStamp(..))
import qualified Ambar.Record.Encoding as Encoding

import Util.Delay (Duration, millis, seconds)
import Util.Async (withAsyncThrow)
import Util.Logger (SimpleLogger, logDebug, logInfo)
import Util.Prettyprinter (renderPretty, sepBy, commaSeparated, prettyJSON)

_POLLING_INTERVAL :: Duration
_POLLING_INTERVAL = millis 50

_MAX_TRANSACTION_TIME :: Duration
_MAX_TRANSACTION_TIME = seconds 120

data SQLServer = SQLServer
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

newtype SQLServerState = SQLServerState BoundaryTracker
  deriving (Generic)
  deriving newtype (Default)
  deriving anyclass (FromJSON, ToJSON)

data SQLServerRow = SQLServerRow { unSQLServerRow :: Record }

instance C.Connector SQLServer where
  type ConnectorState SQLServer = SQLServerState
  type ConnectorRecord SQLServer = SQLServerRow
  -- | A rows gets saved in the database as a JSON object with
  -- the columns specified in the config file as keys.
  encoder = LB.toStrict . Aeson.encode . Encoding.encode @Aeson.Value . unSQLServerRow
  partitioner = hashPartitioner partitioningValue
  connect = connect

connect
  :: SQLServer
  -> SimpleLogger
  -> SQLServerState
  -> Producer SQLServerRow
  -> (STM SQLServerState -> IO a)
  -> IO a
connect config@SQLServer{..} logger (SQLServerState tracker) producer f =
  withConnection cinfo $ \conn -> do
  schema <- fetchSchema c_table conn
  validate config schema
  trackerVar <- newTVarIO tracker
  let readState = SQLServerState <$> readTVar trackerVar
  withAsyncThrow (consume conn schema trackerVar) (f readState)
  where
  cinfo = ConnectionInfo
    { conn_host = c_host
    , conn_port = c_port
    , conn_username = c_username
    , conn_password = c_password
    , conn_database = c_database
    }

  consume
     :: Connection
     -> Schema
     -> TVar BoundaryTracker
     -> IO Void
  consume conn schema trackerVar = Poll.connect trackerVar pc
    where
    pc = Poll.PollingConnector
       { Poll.c_getId = entryId
       , Poll.c_poll = \bs -> Poll.streamed (run bs)
       , Poll.c_pollingInterval = _POLLING_INTERVAL
       , Poll.c_maxTransactionTime = _MAX_TRANSACTION_TIME
       , Poll.c_producer = producer
       }

    parser :: RowParser SQLServerRow
    parser = mkParser (columns config) schema

    run :: Boundaries -> IO [SQLServerRow]
    run (Boundaries bs) = do
      results <- M.queryWith parser conn query
      mapM_ logResult results
      logDebug logger $ "results: " <> show (length results)
      return results
      where
      query = mkQuery_ $ Text.unpack $ renderPretty $ Pretty.fillSep
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

newtype Schema = Schema (Map Text Type)
  deriving Show

newtype SQLServerType = SQLServerType Text
  deriving newtype (FromField)

fetchSchema :: Text -> Connection -> IO Schema
fetchSchema table conn = do
  cols <- M.query conn query
  cols' <- forM cols $ \(cname, SQLServerType cty) ->
    case toExpectedType (SQLServerType cty) of
      Just ty -> return (cname, ty)
      Nothing -> throwIO $ C.UnsupportedType $ Text.unpack cty
  return $ Schema $ Map.fromList cols'
  where
  query = mkQuery_ $ unwords
    [ "select COLUMN_NAME, DATA_TYPE"
    , "from INFORMATION_SCHEMA.COLUMNS"
    , "where TABLE_NAME='" <> Text.unpack table <> "'"
    ]

toExpectedType :: SQLServerType -> Maybe Type
toExpectedType (SQLServerType ty) = case Text.toUpper ty of
  "TEXT"       -> Just TString
  "INT"        -> Just TInteger
  "TINYINT"    -> Just TInteger
  "SMALLINT"   -> Just TInteger
  "BIGINT"     -> Just TInteger
  "DECIMAL"    -> Just TReal
  "NUMERIC"    -> Just TReal
  "REAL"       -> Just TReal
  "MONEY"      -> Just TReal
  "FLOAT"      -> Just TReal
  "SMALLMONEY" -> Just TReal
  "BIT"              -> Just TBytes
  "UNIQUEIDENTIFIER" -> Just TBytes
  "CHAR"             -> Just TString
  "VARCHAR"          -> Just TString
  "BINARY"           -> Just TBytes
  "VARBINARY"        -> Just TBytes
  "CLR UDT"          -> Just TString
  "NTEXT"            -> Just TString
  "NVARCHAR"         -> Just TString
  "NCHAR"            -> Just TString
  "XML"              -> Just TString

  "DATETIME"          -> Just TDateTime
  "SMALLDATETIME"     -> Just TDateTime
  "DATE"              -> Just TString
  "TIME"              -> Just TString
  "DATETIME2"         -> Just TString
  "DATETIMEOFFSET"    -> Just TString

  _ -> Nothing

validate :: SQLServer -> Schema -> IO ()
validate _ _ = do
  -- TODO: Validate
  return ()

entryId :: SQLServerRow -> EntryId
entryId row =
  case serialValue row of
    UInt n -> EntryId $ fromIntegral n
    Int n -> EntryId $ fromIntegral n
    val -> error "Invalid serial column value:" (show val)

partitioningValue :: SQLServerRow -> Value
partitioningValue (SQLServerRow (Record row)) = snd $ row !! 1

serialValue :: SQLServerRow -> Value
serialValue (SQLServerRow (Record row)) =
  case row of
    [] -> error "serialValue: empty row"
    (_,x):_ -> x

-- | Columns in the order they will be queried.
columns :: SQLServer -> [Text]
columns SQLServer{..} =
  [c_incrementingColumn, c_partitioningColumn] <>
    ((c_columns \\ [c_incrementingColumn]) \\ [c_partitioningColumn])

mkParser :: [Text] -> Schema -> RowParser SQLServerRow
mkParser cols (Schema schema) = do
  values <- mapM (M.parseFieldWith . parse) cols
  return $ SQLServerRow $ Record $ zip cols values
  where
  parse :: Text -> FieldParser Value
  parse cname = do
    ty <- case Map.lookup cname schema of
      Nothing -> fail $ "unknown column: " <> Text.unpack cname
      Just v -> return v
    M.Field (Tds.MetaColumnData _ _ tinfo _ _) mbytes <- M.fieldInfo
    case mbytes of
      Nothing -> return Null
      Just _ -> convertTo tinfo ty =<< parseAsValue tinfo

  convertTo :: Tds.TypeInfo -> Type -> Value -> FieldParser Value
  convertTo tinfo ty val = case val of
    Boolean _ -> case ty of
      TBoolean -> return val
      _ -> unsupported
    UInt _ ->  case ty of
      TUInteger -> return val
      _ -> unsupported
    Int _ -> case ty of
      TInteger -> return val
      _ -> unsupported
    Real _ -> case ty of
      TReal -> return val
      _ -> unsupported
    String _ -> case ty of
      TString -> return val
      _ -> unsupported
    Binary _ -> case ty of
      TBytes -> return val
      _ -> unsupported
    Json _ _ -> case ty of
      TJSON -> return val
      _ -> unsupported
    DateTime _ -> case ty of
      TDateTime -> return val
      _ -> unsupported
    Null -> return val
    where
    unsupported = fail $
      Text.unpack $ renderPretty $
      "Cannot convert "
        <> pretty (show val)
        <> " with type "
        <> pretty (show tinfo)
        <> " to "
        <> pretty (show ty)

  parseAsValue :: Tds.TypeInfo -> FieldParser Value
  parseAsValue tinfo = case tinfo of
    Tds.TINull -> return Null
    Tds.TIInt1 -> parseInt
    Tds.TIInt2 -> parseInt
    Tds.TIInt4 -> parseInt
    Tds.TIInt8 -> parseInt
    Tds.TIIntN1 -> parseInt
    Tds.TIIntN2 -> parseInt
    Tds.TIIntN4 -> parseInt
    Tds.TIIntN8 -> parseInt
    Tds.TIFlt4 -> Real <$> fieldParser
    Tds.TIFlt8 -> Real <$> fieldParser
    Tds.TIMoney4 -> parseMoney
    Tds.TIMoney8 -> parseMoney
    Tds.TIMoneyN4 -> parseMoney
    Tds.TIMoneyN8 -> parseMoney
    Tds.TIFltN4 -> Real <$> fieldParser
    Tds.TIFltN8 -> Real <$> fieldParser
    Tds.TIGUID -> Binary . Bytes . LB.toStrict <$> rawBytes
    Tds.TIDecimalN _ scale -> parseScientific scale
    Tds.TINumericN _ scale -> parseScientific scale
    Tds.TIChar _ -> String . Text.decodeUtf8 . LB.toStrict <$> rawBytes
    Tds.TIVarChar _ -> String . Text.decodeUtf8 . LB.toStrict <$> rawBytes
    Tds.TIBigChar _ _collation -> String . Text.decodeUtf8 . LB.toStrict <$> rawBytes
    Tds.TIBigVarChar _ _collation -> String . Text.decodeUtf8 . LB.toStrict <$> rawBytes
    Tds.TIText _ _collation -> String . Text.decodeUtf8 . LB.toStrict <$> rawBytes
    Tds.TINChar _ _collation -> String <$> fieldParser
    Tds.TINVarChar _ _collation -> String <$> fieldParser
    Tds.TINText _ _collation -> String <$> fieldParser
    Tds.TIBit -> Binary . Bytes . LB.toStrict <$> rawBytes
    Tds.TIBitN -> Binary . Bytes . LB.toStrict <$> rawBytes
    Tds.TIBinary _ -> Binary . Bytes . LB.toStrict <$> rawBytes
    Tds.TIVarBinary _ -> Binary . Bytes . LB.toStrict <$> rawBytes
    Tds.TIBigBinary _ -> Binary . Bytes . LB.toStrict <$> rawBytes
    Tds.TIBigVarBinary _ -> Binary . Bytes . LB.toStrict <$> rawBytes
    Tds.TIImage _ -> unsupported

    Tds.TIDateTime4 -> do
      utc <- fieldParser
      return $ DateTime $ TimeStamp (Text.pack $ iso8601Show utc) utc
    Tds.TIDateTime8 -> do
      utc <- fieldParser
      return $ DateTime $ TimeStamp (Text.pack $ iso8601Show utc) utc
    Tds.TIDateTimeN4 -> parseDateTime
    Tds.TIDateTimeN8 -> parseDateTime
    where
    unsupported = fail $ "type '" <> show tinfo <> "' not supported"

  parseInt :: FieldParser Value
  parseInt = Int . fromIntegral @Int <$> fieldParser

  parseScientific :: Word8 -> FieldParser Value
  parseScientific scale = do
    MkFixed n <- fieldParser @(Fixed E0)
    let num = fromIntegral n / (10 ^ scale) :: Scientific
    case toBoundedRealFloat num of
      Left _ -> fail $ "Number outside of valid Double scale: " <> show num
      Right x -> return $ Real x

  parseMoney :: FieldParser Value
  parseMoney = do
    Tds.Money n <- fieldParser
    return $ Real $ realToFrac n

  parseDateTime :: FieldParser Value
  parseDateTime = do
    utc <- fieldParser
    txt <- fieldParser
    return $ DateTime $ TimeStamp txt utc

