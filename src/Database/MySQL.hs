{-# LANGUAGE UndecidableInstances #-}

-- | A thread-safe wrapper around the mysql and mysql-simple libraries.
module Database.MySQL
  ( Connection
  , ConnectionInfo(..)
  , withConnection
  , query
  , query_
  , executeMany
  , execute_

  -- saner parsing primitives
  , FromRow(..)
  , FromField(..)
  , FieldParser
  , RowParser
  , fieldInfo
  , field
  , failure
  , ParseFailure(..)
  )
  where

import Control.Concurrent (MVar, newMVar, newEmptyMVar, putMVar, takeMVar, tryPutMVar, modifyMVar_)
import Control.Exception (Exception, SomeException, bracket, throwIO, try, evaluate)
import Control.Monad (forM, unless)
import Control.Applicative (Alternative(..))
import Data.ByteString (ByteString)
import Data.Int (Int64)
import Control.Monad (forever)
import qualified Data.Text as Text
import Data.Text (Text)
import Data.Word (Word16)
import System.IO.Unsafe (unsafePerformIO)

import qualified Database.MySQL.Base as M (MySQLError(..), initLibrary, initThread, endThread)
import qualified Database.MySQL.Base.Types as M (Field(..))
import qualified Database.MySQL.Simple as M
import qualified Database.MySQL.Simple.Result as M (Result(..))
import qualified Database.MySQL.Simple.QueryResults as M
import qualified Database.MySQL.Simple.QueryParams as M

import Utils.Exception (annotateWith, tryAll)
import Utils.Async (withAsyncBoundThrow)

query :: forall q r. (M.QueryParams q, FromRow r) => Connection -> M.Query -> q -> IO [r]
query (Connection run) q params =
  run $ \conn ->
    annotateQ q $ do
      rows <- M.query conn q params
      parseRows rows

query_ :: FromRow r => Connection -> M.Query -> IO [r]
query_ (Connection run) q =
  run $ \conn ->
    annotateQ q $ do
      rows <- M.query_ conn q
      parseRows rows

execute_ :: Connection -> M.Query -> IO Int64
execute_ (Connection run) q = run $ \conn -> annotateQ q $ M.execute_ conn q

executeMany :: M.QueryParams q => Connection -> M.Query -> [q] -> IO Int64
executeMany (Connection run) q ps = run $ \conn -> annotateQ q $ M.executeMany conn q ps

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

newtype Row = Row [(M.Field, Maybe ByteString)]

data ParseFailure
  = ParseError M.ResultError
  | Unexpected String
  deriving (Show)
  deriving anyclass (Exception)


class FromRow r where
  rowParser :: RowParser r

instance (FromField a, FromField b) =>
  FromRow (a,b) where
  rowParser = (,) <$> field <*> field

instance (FromField a , FromField b, FromField c) =>
    FromRow (a,b,c) where
  rowParser = (,,) <$> field <*> field <*> field

instance (FromField a , FromField b, FromField c, FromField d) =>
    FromRow (a,b,c,d) where
  rowParser = (,,,) <$> field <*> field <*> field <*> field

instance (FromField a , FromField b, FromField c, FromField d, FromField e) =>
    FromRow (a,b,c,d,e) where
  rowParser = (,,,,) <$> field <*> field <*> field <*> field <*> field

instance (FromField a , FromField b, FromField c, FromField d, FromField e, FromField f) =>
    FromRow (a,b,c,d,e,f) where
  rowParser = (,,,,,) <$> field <*> field <*> field <*> field <*> field <*> field

parseRow :: FromRow a => Row -> Either ParseFailure a
parseRow row = fmap snd $ parser row
  where
  RowParser parser = rowParser

parseRows :: FromRow a => [Row] -> IO [a]
parseRows rows =
  case forM rows parseRow of
    Left err -> throwIO err
    Right vs -> return vs

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
  fail str = RowParser $ \_ -> Left $ failure str

class FromField r where
  parseField :: M.Field -> Maybe ByteString -> Either ParseFailure r

fieldInfo :: FieldParser (M.Field, Maybe ByteString)
fieldInfo = FieldParser $ \x mbs -> Right (x, mbs)

newtype FieldParser a =
  FieldParser (M.Field -> Maybe ByteString -> Either ParseFailure a)

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

field :: FromField r => RowParser r
field = RowParser $ \(Row row) ->
  case row of
    [] -> Left $ failure "insufficient values"
    (f,mbs) : row' -> fmap (Row row',) (parseField f mbs)

instance {-# OVERLAPPABLE #-} M.Result r => FromField r where
  parseField f mbs =
    case unsafePerformIO $ try $ evaluate $ M.convert f mbs of
      Left (err :: M.ResultError) -> Left $ ParseError err
      Right v -> Right v

failure :: String -> ParseFailure
failure = Unexpected

instance M.QueryResults Row where
  convertResults fs mbs = Row $ zip fs mbs
