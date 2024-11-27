{-# LANGUAGE UndecidableInstances #-}

-- | A thread-safe wrapper around the mysql and mysql-simple libraries.
module Database.MySQL
  ( Connection
  , ConnectionInfo(..)
  , defaultConnectionInfo
  , withConnection
  , query
  , query_
  , executeMany
  , execute_
  , fold
  , fold_

  -- saner parsing primitives
  , ToRow
  , ToField
  , FromRow(..)
  , FromField(..)
  , FieldParser
  , RowParser
  , isEndOfRow
  , asList
  , fieldInfo
  , fieldParseError
  , fromFieldParser
  , parseFailure
  , parseField

  -- useful re-exports
  , M.Field(..)
  , M.Type(..)
  , M.ResultError(..)
  , Binary(..)
  , M.Param(..)
  )
  where

import Control.Concurrent (MVar, newMVar, newEmptyMVar, putMVar, takeMVar, tryPutMVar, modifyMVar_)
import Control.Exception (Exception, SomeException, ErrorCall(..), toException, bracket, throwIO, try, evaluate)
import Control.Applicative (Alternative(..))
import Control.Monad (unless, forever)
import Data.ByteString (ByteString)
import Data.Int (Int64)
import qualified Data.Text as Text
import Data.Text (Text)
import Data.Word (Word16)
import System.IO.Unsafe (unsafePerformIO)

import qualified Database.MySQL.Base as M (MySQLError(..), initLibrary, initThread, endThread)
import qualified Database.MySQL.Base.Types as M (Field(..), Type(..))
import qualified Database.MySQL.Simple as M
import qualified Database.MySQL.Simple.Param as M (Param(..))
import Database.MySQL.Simple.Types (Binary(..))
import qualified Database.MySQL.Simple.Result as M (Result(..), ResultError(..))
import qualified Database.MySQL.Simple.QueryResults as M
import qualified Database.MySQL.Simple.QueryParams as M

import Utils.Exception (annotateWith, tryAll)
import Utils.Async (withAsyncBoundThrow)

query :: (ToRow q, FromRow r) => Connection -> M.Query -> q -> IO [r]
query (Connection run) q params =
  run $ \conn ->
    annotateQ q $ do
      rows <- M.query conn q params
      traverse parseRow rows

query_ :: FromRow r => Connection -> M.Query -> IO [r]
query_ (Connection run) q =
  run $ \conn ->
    annotateQ q $ do
      rows <- M.query_ conn q
      traverse parseRow rows

fold
  :: (ToRow q, FromRow r)
  => Connection
  -> M.Query
  -> q
  -> a
  -> (a -> r -> IO a)
  -> IO a
fold (Connection run) q params acc f =
  run $ \conn ->
  annotateQ q $
  M.fold conn q params acc $ \acc' row -> do
    r <- parseRow row
    f acc' r

fold_ :: FromRow r => Connection -> M.Query -> a -> (a -> r -> IO a) -> IO a
fold_ (Connection run) q acc f =
  run $ \conn ->
  annotateQ q $
  M.fold_ conn q acc $ \acc' row -> do
    r <- parseRow row
    f acc' r

execute_ :: Connection -> M.Query -> IO Int64
execute_ (Connection run) q = run $ \conn -> annotateQ q $ M.execute_ conn q

executeMany :: ToRow q => Connection -> M.Query -> [q] -> IO Int64
executeMany (Connection run) q ps =
  run $ \conn -> annotateQ q $ M.executeMany conn q ps

-- | Include the query that was executed together with the error.
annotateQ :: M.Query -> IO a -> IO a
annotateQ q act = annotateWith f act
  where
  f :: M.MySQLError -> String
  f _ = "On query " <> show q

-- Run an action in the connection thread.
newtype Connection = Connection (forall a. (M.Connection -> IO a) -> IO a)

data ConnectionInfo = ConnectionInfo
  { conn_host :: Text
  , conn_port :: Word16
  , conn_username :: Text
  , conn_password :: Text
  , conn_database :: Text
  }

defaultConnectionInfo :: ConnectionInfo
defaultConnectionInfo = ConnectionInfo
  { conn_host = Text.pack $ M.connectHost M.defaultConnectInfo
  , conn_port = M.connectPort M.defaultConnectInfo
  , conn_username = Text.pack $ M.connectUser M.defaultConnectInfo
  , conn_password = Text.pack $ M.connectPassword M.defaultConnectInfo
  , conn_database = Text.pack $ M.connectDatabase M.defaultConnectInfo
  }


{-# NOINLINE initialised #-}
initialised :: MVar Bool
initialised = unsafePerformIO (newMVar False)

-- | Initialise the C library only once.
initialise :: IO ()
initialise = modifyMVar_ initialised $ \done -> do
  unless done M.initLibrary
  return True

-- | The underlying MySQL C library uses thread-local variables which means it isn't
-- really thread-safe. There is a way to use a connection in multiple threads but it's
-- too much hassle.
--
-- We do the easiest thing: use a sigle bound thread per connection to the db and
-- perform all db communication in that thread.
withConnection :: ConnectionInfo -> (Connection -> IO a) -> IO a
withConnection ConnectionInfo{..} f = do
  initialise
  varAction <- newEmptyMVar
  r <- withAsyncBoundThrow (worker varAction) $ f (Connection (run varAction))
  _ <- tryPutMVar varAction (\_ -> return ())
  return r
  where
  run :: MVar (M.Connection -> IO ()) -> (forall a. (M.Connection -> IO a) -> IO a)
  run varAction act = do
    varResult :: MVar (Either SomeException a) <- newEmptyMVar
    putMVar varAction $ \conn -> do
      r <- tryAll (act conn)
      putMVar varResult r
    r <- takeMVar varResult
    case r of
      Right v -> return v
      Left err -> throwIO err

  worker :: MVar (M.Connection -> IO ()) -> IO ()
  worker varAction =
    bracket open close $ \conn -> do
    forever $ do
      act <- takeMVar varAction
      act conn

  open = do
    M.initThread
    M.connect M.defaultConnectInfo
     { M.connectHost = Text.unpack conn_host
     , M.connectPort = conn_port
     , M.connectUser = Text.unpack conn_username
     , M.connectPassword = Text.unpack conn_password
     , M.connectDatabase = Text.unpack conn_database
     }

  close conn = do
    M.endThread
    M.close conn

-- | A value that can be converted into query parameters
-- Use ToRow instead of QueryResults
type ToRow = M.QueryParams

-- Use ToField instead of Param
type ToField = M.Param

newtype Row = Row [(M.Field, Maybe ByteString)]

data ParseFailure
  = ParseError M.ResultError
  | Unexpected SomeException
  deriving (Show)
  deriving anyclass (Exception)

class FromRow r where
  rowParser :: RowParser r

instance (FromField a, FromField b) =>
  FromRow (a,b) where
  rowParser = (,) <$> parseField <*> parseField

instance (FromField a , FromField b, FromField c) =>
    FromRow (a,b,c) where
  rowParser = (,,) <$> parseField <*> parseField <*> parseField

instance (FromField a , FromField b, FromField c, FromField d) =>
    FromRow (a,b,c,d) where
  rowParser = (,,,) <$> parseField <*> parseField <*> parseField <*> parseField

instance (FromField a , FromField b, FromField c, FromField d, FromField e) =>
    FromRow (a,b,c,d,e) where
  rowParser = (,,,,) <$> parseField <*> parseField <*> parseField <*> parseField <*> parseField

instance (FromField a , FromField b, FromField c, FromField d, FromField e, FromField f) =>
    FromRow (a,b,c,d,e,f) where
  rowParser = (,,,,,) <$> parseField <*> parseField <*> parseField <*> parseField <*> parseField <*> parseField

parseRow :: FromRow a => Row -> IO a
parseRow row =
  case fmap snd $ parser row of
    Left err -> throwIO err
    Right v -> return v
  where
  RowParser parser = rowParser

newtype RowParser a = RowParser (Row -> Either ParseFailure (Row, a))

instance Functor RowParser where
  fmap f (RowParser g) = RowParser $ fmap (fmap $ fmap f) g

instance Applicative RowParser where
  pure v = RowParser $ \row -> Right (row, v)
  (RowParser fs) <*> (RowParser vs) = RowParser $ \row -> do
    (row', f) <- fs row
    (row'', v) <- vs row'
    return (row'', f v)

instance Alternative RowParser where
  empty = fail "no parse"
  (RowParser l) <|> (RowParser r) = RowParser $ \row ->
    case l row of
      Right v -> return v
      Left _ -> r row

instance Monad RowParser where
  (RowParser vs) >>= f = RowParser $ \row -> do
    (row', v) <- vs row
    let RowParser g = f v
    x <- g row'
    return x

instance MonadFail RowParser where
  fail str = fromFieldParser (fail str)

class FromField r where
  fieldParser :: FieldParser r

-- | Parse a row as a list of values.
asList :: FromField a => RowParser [a]
asList = do
  isEnd <- isEndOfRow
  if isEnd
     then return []
     else (:) <$> parseField <*> asList

isEndOfRow :: RowParser Bool
isEndOfRow  = RowParser $ \(Row xs) -> Right (Row xs, null xs)

fieldInfo :: FieldParser (M.Field, Maybe ByteString)
fieldInfo = FieldParser $ \x mbs -> Right (x, mbs)

fieldParseError :: M.ResultError -> FieldParser a
fieldParseError err = FieldParser $ \_ _ -> Left (ParseError err)

parseFailure :: Exception e => e -> FieldParser a
parseFailure e = FieldParser $ \_ _ -> Left (Unexpected $ toException e)

newtype FieldParser a = FieldParser
  { unFieldParser :: M.Field -> Maybe ByteString -> Either ParseFailure a
  }

instance Functor FieldParser where
  fmap fun (FieldParser g) = FieldParser $ \x mbs -> fun <$> g x mbs

instance Applicative FieldParser where
  pure v = FieldParser $ \_ _ -> Right v
  FieldParser pf <*> FieldParser pv =
    FieldParser $ \x mbs -> pf x mbs <*> pv x mbs

instance Monad FieldParser where
  return = pure
  FieldParser pv >>= f = FieldParser $ \x mbs ->
    case pv x mbs of
      Left err -> Left err
      Right v ->
        let FieldParser g = f v
         in g x mbs

instance MonadFail FieldParser where
  fail str = parseFailure (ErrorCall str)

parseField :: FromField r => RowParser r
parseField = fromFieldParser fieldParser

fromFieldParser :: FieldParser a -> RowParser a
fromFieldParser parser = RowParser $ \(Row row) ->
  case row of
    [] -> Left $ Unexpected $ toException $ ErrorCall "insufficient values"
    (f,mbs) : row' -> fmap (Row row',) (unFieldParser parser f mbs)

instance {-# OVERLAPPABLE #-} M.Result r => FromField r where
  fieldParser  = do
    (f, mbs) <- fieldInfo
    case unsafePerformIO $ try $ evaluate $ M.convert f mbs of
      Left (err :: M.ResultError) ->
        FieldParser $ \_ _ -> Left (ParseError err)
      Right v -> return v

instance M.QueryResults Row where
  convertResults fs mbs = Row $ zip fs mbs

