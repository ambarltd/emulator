module Ambar.Emulator.Connector.Postgres
  ( PostgreSQL (..),
    PostgreSQLState (..),
  )
where

import Ambar.Emulator.Connector qualified as C
import Ambar.Emulator.Connector.Poll (Boundaries (..), BoundaryTracker, EntryId (..), PollingInterval, Stream)
import Ambar.Emulator.Connector.Poll qualified as Poll
import Ambar.Emulator.Queue.Topic (Producer, hashPartitioner)
import Ambar.Record (Bytes (..), Record (..), TimeStamp (..), Value (..))
import Ambar.Record.Encoding qualified as Encoding
import Control.Concurrent.STM (STM, TVar, newTVarIO, readTVar)
import Control.Exception (ErrorCall (..), bracket, throwIO)
import Control.Monad (foldM)
import Data.Aeson (FromJSON, ToJSON)
import Data.Aeson qualified as Aeson
import Data.ByteString (ByteString)
import Data.ByteString.Lazy qualified as LB
import Data.Default (Default (..))
import Data.List ((\\))
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe)
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text (decodeUtf8)
import Data.Time.LocalTime (localTimeToUTC, utc)
import Data.Void (Void)
import Data.Word (Word16)
import Database.PostgreSQL.Simple qualified as P
import Database.PostgreSQL.Simple.FromField qualified as P
import Database.PostgreSQL.Simple.FromRow qualified as P
import Database.PostgreSQL.Simple.Transaction qualified as P
import GHC.Generics (Generic)
import Prettyprinter (pretty, (<+>))
import Prettyprinter qualified as Pretty
import Util.Async (withAsyncThrow)
import Util.Delay (Duration, millis, seconds)
import Util.Logger (SimpleLogger, logDebug, logInfo)
import Util.Prettyprinter (commaSeparated, prettyJSON, renderPretty, sepBy)

_POLLING_INTERVAL :: Duration
_POLLING_INTERVAL = millis 50

_MAX_TRANSACTION_TIME :: Duration
_MAX_TRANSACTION_TIME = seconds 120

_MAX_BOUNDARY_BATCH_SIZE :: Int
_MAX_BOUNDARY_BATCH_SIZE = 500

_USE_FIX :: Bool
_USE_FIX = True

data PostgreSQL = PostgreSQL
  { c_host :: Text,
    c_port :: Word16,
    c_username :: Text,
    c_password :: Text,
    c_database :: Text,
    c_table :: Text,
    c_columns :: [Text],
    c_partitioningColumn :: Text,
    c_serialColumn :: Text,
    c_pollingInterval :: PollingInterval
  }

instance C.Connector PostgreSQL where
  type ConnectorState PostgreSQL = PostgreSQLState
  type ConnectorRecord PostgreSQL = Record

  partitioner = hashPartitioner partitioningValue

  -- \| A rows gets saved in the database as a JSON object with
  -- the columns specified in the config file as keys.
  encoder = LB.toStrict . Aeson.encode . Encoding.encode @Aeson.Value

  connect = connect

newtype TableSchema = TableSchema {unTableSchema :: Map Text PgType}
  deriving (Show)

-- | Supported PostgreSQL types
data PgType
  = PgInt8
  | PgInt2
  | PgInt4
  | PgFloat4
  | PgFloat8
  | PgBool
  | PgJson
  | PgBytea
  | PgTimestamp
  | PgTimestamptz
  | PgText
  deriving (Show, Eq)

instance P.FromField PgType where
  fromField t d = do
    str <- P.fromField t d
    case str of
      "int8" -> return PgInt8
      "int2" -> return PgInt2
      "int4" -> return PgInt4
      "float4" -> return PgFloat4
      "float8" -> return PgFloat8
      "bool" -> return PgBool
      "json" -> return PgJson
      "bytea" -> return PgBytea
      "timestamp" -> return PgTimestamp
      "timestamptz" -> return PgTimestamptz
      "text" -> return PgText
      _ -> P.conversionError $ C.UnsupportedType str

-- | Opaque serializable connector state
newtype PostgreSQLState = PostgreSQLState BoundaryTracker
  deriving (Generic)
  deriving newtype (Default)
  deriving anyclass (FromJSON, ToJSON)

connect ::
  PostgreSQL ->
  SimpleLogger ->
  PostgreSQLState ->
  Producer Record ->
  (STM PostgreSQLState -> IO a) ->
  IO a
connect config@PostgreSQL {..} logger (PostgreSQLState tracker) producer f =
  bracket open P.close $ \conn -> do
    schema <- fetchSchema c_table conn
    validate config schema
    trackerVar <- newTVarIO tracker
    let readState = PostgreSQLState <$> readTVar trackerVar
    withAsyncThrow (consume conn c_pollingInterval schema trackerVar) (f readState)
  where
    open =
      P.connect
        P.ConnectInfo
          { P.connectHost = Text.unpack c_host,
            P.connectPort = c_port,
            P.connectUser = Text.unpack c_username,
            P.connectPassword = Text.unpack c_password,
            P.connectDatabase = Text.unpack c_database
          }

    consume ::
      P.Connection ->
      PollingInterval ->
      TableSchema ->
      TVar BoundaryTracker ->
      IO Void
    consume conn interval schema trackerVar = Poll.connect trackerVar pc
      where
        pc =
          Poll.PollingConnector
            { Poll.c_getId = entryId,
              Poll.c_poll = run,
              Poll.c_pollingInterval = interval,
              Poll.c_maxTransactionTime = _MAX_TRANSACTION_TIME,
              Poll.c_producer = producer
            }

        parser = mkParser (columns config) schema

        opts =
          P.FoldOptions
            { P.fetchQuantity = P.Automatic,
              P.transactionMode =
                P.TransactionMode
                  { P.isolationLevel = P.ReadCommitted,
                    P.readWriteMode = P.ReadOnly
                  }
            }

        run :: Boundaries -> Stream Record
        run (Boundaries bs) acc0 emit =
          if _USE_FIX
            then run_new (Boundaries bs) acc0 emit
            else run_old (Boundaries bs) acc0 emit

        run_old :: Boundaries -> Stream Record
        run_old (Boundaries bs) acc0 emit = do
          logDebug logger query
          (acc, count) <- P.foldWithOptionsAndParser opts parser conn (fromString query) () (acc0, 0) $
            \(acc, !count) record -> do
              logResult record
              acc' <- emit acc record
              return (acc', succ count)
          logDebug logger $ "results: " <> show @Int count
          return acc
          where
            query =
              fromString $
                Text.unpack $
                  renderPretty $
                    Pretty.fillSep
                      [ "SELECT",
                        commaSeparated $ map pretty $ columns config,
                        "FROM",
                        pretty c_table,
                        if null bs then "" else "WHERE" <> constraints,
                        "ORDER BY",
                        pretty c_serialColumn
                      ]

            constraints =
              sepBy
                "AND"
                [ Pretty.fillSep
                    [ "(",
                      pretty c_serialColumn,
                      "<",
                      pretty low,
                      "OR",
                      pretty high,
                      "<",
                      pretty c_serialColumn,
                      ")"
                    ]
                | (EntryId low, EntryId high) <- bs
                ]

            logResult row =
              logInfo logger $
                renderPretty $
                  "ingested."
                    <+> commaSeparated
                      [ "serial_value:" <+> prettyJSON (serialValue row),
                        "partitioning_value:" <+> prettyJSON (partitioningValue row)
                      ]

        run_new :: Boundaries -> Stream Record
        run_new (Boundaries bs) acc0 emit = do
          let batches = chunksOf _MAX_BOUNDARY_BATCH_SIZE bs
          (acc, mLastUpper) <-
            foldM
              ( \(acc, mLower) batch -> do
                  let upper = snd (lastElem batch)
                  acc' <- runBatch mLower (Just upper) batch acc
                  return (acc', Just upper)
              )
              (acc0, Nothing)
              batches
          runBatch mLastUpper Nothing [] acc
          where
            runBatch mLower mUpper batchBs acc = do
              logDebug logger batchQuery
              (acc', count) <- P.foldWithOptionsAndParser opts parser conn (fromString batchQuery) () (acc, 0) $
                \(a, !count) record -> do
                  logResult record
                  a' <- emit a record
                  return (a', succ count)
              logDebug logger $ "results: " <> show @Int count
              return acc'
              where
                batchQuery =
                  fromString $
                    Text.unpack $
                      renderPretty $
                        Pretty.fillSep
                          [ "SELECT",
                            commaSeparated $ map pretty $ columns config,
                            "FROM",
                            pretty c_table,
                            if null allConstraints then "" else "WHERE" <+> sepBy "AND" allConstraints,
                            "ORDER BY",
                            pretty c_serialColumn
                          ]

                allConstraints = lowerBound ++ upperBound ++ exclusions

                lowerBound = case mLower of
                  Nothing -> []
                  Just (EntryId low) ->
                    [Pretty.fillSep [pretty low, "<", pretty c_serialColumn]]

                upperBound = case mUpper of
                  Nothing -> []
                  Just (EntryId high) ->
                    [Pretty.fillSep [pretty c_serialColumn, "<=", pretty high]]

                exclusions =
                  [ Pretty.fillSep
                      [ "(",
                        pretty c_serialColumn,
                        "<",
                        pretty low,
                        "OR",
                        pretty high,
                        "<",
                        pretty c_serialColumn,
                        ")"
                      ]
                  | (EntryId low, EntryId high) <- batchBs
                  ]

            logResult row =
              logInfo logger $
                renderPretty $
                  "ingested."
                    <+> commaSeparated
                      [ "serial_value:" <+> prettyJSON (serialValue row),
                        "partitioning_value:" <+> prettyJSON (partitioningValue row)
                      ]

            chunksOf :: Int -> [a] -> [[a]]
            chunksOf _ [] = []
            chunksOf n xs = let (chunk, rest) = splitAt n xs in chunk : chunksOf n rest

            lastElem :: [a] -> a
            lastElem [] = error "lastElem: empty list"
            lastElem [x] = x
            lastElem (_ : xs) = lastElem xs

-- | Columns in the order they will be queried.
columns :: PostgreSQL -> [Text]
columns PostgreSQL {..} =
  [c_serialColumn, c_partitioningColumn]
    <> ((c_columns \\ [c_serialColumn]) \\ [c_partitioningColumn])

serialValue :: Record -> Value
serialValue (Record row) =
  case row of
    [] -> error "serialValue: empty row"
    (_, x) : _ -> x

partitioningValue :: Record -> Value
partitioningValue (Record row) = snd $ row !! 1

mkParser :: [Text] -> TableSchema -> P.RowParser Record
mkParser cols (TableSchema schema) = Record . zip cols <$> traverse (parser . getType) cols
  where
    getType col = schema Map.! col

    parser :: PgType -> P.RowParser Value
    parser ty = fmap (fromMaybe Null) $ case ty of
      PgInt8 -> fmap Int <$> P.field
      PgInt2 -> fmap Int <$> P.field
      PgInt4 -> fmap Int <$> P.field
      PgFloat4 -> fmap Real <$> P.field
      PgFloat8 -> fmap Real <$> P.field
      PgBool -> fmap Boolean <$> P.field
      PgJson -> do
        -- this JSON type is untyped and therefore is conveyed as string
        val <- P.field @Aeson.Value
        let txt = Text.decodeUtf8 $ LB.toStrict $ Aeson.encode val
        return (Just $ String txt)
      PgBytea -> fmap (Binary . Bytes . P.fromBinary) <$> P.field
      PgTimestamp -> fmap DateTime <$> P.fieldWith (P.optionalField parserTimeStamp)
      PgTimestamptz -> fmap DateTime <$> P.fieldWith (P.optionalField parserTimeStampTZ)
      PgText -> fmap String <$> P.field

parserTimeStampTZ :: P.FieldParser TimeStamp
parserTimeStampTZ = parser
  where
    parser :: P.Field -> Maybe ByteString -> P.Conversion TimeStamp
    parser field mbs =
      case mbs of
        Nothing -> P.returnError P.UnexpectedNull field ""
        Just bs -> do
          let txt = Text.decodeUtf8 bs
          utcTime <- P.fromField field mbs
          return $ TimeStamp txt utcTime

parserTimeStamp :: P.FieldParser TimeStamp
parserTimeStamp = parser
  where
    parser :: P.Field -> Maybe ByteString -> P.Conversion TimeStamp
    parser field mbs =
      case mbs of
        Nothing -> P.returnError P.UnexpectedNull field ""
        Just bs -> do
          let txt = Text.decodeUtf8 bs
          localTime <- P.fromField field mbs
          return $ TimeStamp txt $ Just $ localTimeToUTC utc localTime

entryId :: Record -> EntryId
entryId record = case serialValue record of
  Int n -> EntryId $ fromIntegral n
  val -> error "Invalid serial column value:" (show val)

validate :: PostgreSQL -> TableSchema -> IO ()
validate config (TableSchema schema) = do
  missingCol
  invalidSerial
  where
    missing = filter (not . (`Map.member` schema)) (columns config)
    missingCol =
      case missing of
        [] -> return ()
        xs -> throwIO $ ErrorCall $ "Missing columns in target table: " <> Text.unpack (Text.unwords xs)

    invalidSerial =
      if serialTy `elem` allowedSerialTypes
        then return ()
        else throwIO $ ErrorCall $ "Invalid serial column type: " <> show serialTy
    serialTy = schema Map.! c_serialColumn config
    allowedSerialTypes = [PgInt8, PgInt2, PgInt4]

fetchSchema :: Text -> P.Connection -> IO TableSchema
fetchSchema table conn = do
  cols <- P.query conn query [table]
  return $ TableSchema $ Map.fromList cols
  where
    -- In PostgreSQL internals 'columns' are called 'attributes' for hysterical raisins.
    -- oid is the table identifier in the pg_class table.
    query =
      fromString $
        unwords
          [ "SELECT a.attname, t.typname",
            "  FROM pg_class c",
            "  JOIN pg_attribute a ON relname = ? AND c.oid = a.attrelid AND a.attnum > 0",
            "  JOIN pg_type t ON t.oid = a.atttypid",
            "  ORDER BY a.attnum ASC;"
          ]
