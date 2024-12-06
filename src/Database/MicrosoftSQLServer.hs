
module Database.MicrosoftSQLServer
  ( Connection
  , ConnectionInfo(..)
  , withConnection

  , query
  , queryWith
  , FromRow(..)
  , Row(..)
  , RowParser
  , FromField(..)
  , Field(..)
  , FieldParser
  , parseFieldWith

  , asList
  , isEndOfRow
  , fieldInfo
  )
  where

import Control.Exception (SomeException, Exception, bracket, throwIO, toException, ErrorCall(..))
import Control.Applicative (Alternative(..))
import qualified Data.Text as Text
import Data.String (IsString)
import Data.Text (Text)
import Data.Word (Word16)

import Database.MSSQLServer.Connection (Connection)
import qualified Database.MSSQLServer.Connection as M
import qualified Database.MSSQLServer.Query as M
import qualified Database.Tds.Message as M

data ConnectionInfo = ConnectionInfo
  { conn_host :: Text
  , conn_port :: Word16
  , conn_username :: Text
  , conn_password :: Text
  , conn_database :: Text
  }


withConnection :: ConnectionInfo -> (Connection -> IO a) -> IO a
withConnection ConnectionInfo{..} f =
  bracket connect disconnect f
  where
  connect =
    M.connect M.defaultConnectInfo
      { M.connectHost = Text.unpack conn_host
      , M.connectPort = show conn_port
      , M.connectDatabase = Text.unpack conn_database
      , M.connectUser = Text.unpack conn_username
      , M.connectPassword = Text.unpack conn_password
      }

  disconnect = M.close

newtype Query = Query Text
  deriving (Show)
  deriving newtype IsString

query :: FromRow a => Connection -> Query -> IO [a]
query conn q = queryWith rowParser conn q

queryWith :: RowParser a -> Connection -> Query -> IO [a]
queryWith parser conn (Query q) = do
  rows <- M.sql conn q
  traverse (parseRow parser) rows

parseRow :: RowParser a -> Row -> IO a
parseRow (RowParser parser) row =
  case snd <$> parser row of
    Left (Unexpected err) -> throwIO err
    Right v -> return v

data Field = Field M.MetaColumnData M.RawBytes
newtype Row = Row [Field]

instance M.Row Row where
  fromListOfRawBytes rawdata bytes = Row (zipWith Field rawdata bytes)

data ParseFailure = Unexpected SomeException
  deriving (Show)
  deriving anyclass (Exception)

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
  fail str = parseFieldWith (fail str)

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

newtype FieldParser a = FieldParser
  { unFieldParser :: Field -> Either ParseFailure a
  }

instance Functor FieldParser where
  fmap fun (FieldParser g) = FieldParser $ \x -> fun <$> g x

instance Applicative FieldParser where
  pure v = FieldParser $ \_ -> Right v
  FieldParser pf <*> FieldParser pv =
    FieldParser $ \x -> pf x <*> pv x

instance Monad FieldParser where
  return = pure
  FieldParser pv >>= f = FieldParser $ \x ->
    case pv x of
      Left err -> Left err
      Right v ->
        let FieldParser g = f v
        in g x

instance MonadFail FieldParser where
  fail str = parseFailure (ErrorCall str)

class FromField r where
  fieldParser :: FieldParser r

parseField :: FromField r => RowParser r
parseField = parseFieldWith fieldParser

parseFieldWith :: FieldParser a -> RowParser a
parseFieldWith parser = RowParser $ \(Row row) ->
  case row of
    [] -> Left $ Unexpected $ toException $ ErrorCall "insufficient values"
    x : row' -> fmap (Row row',) (unFieldParser parser x)

-- | Parse a row as a list of values.
asList :: FromField a => RowParser [a]
asList = do
  isEnd <- isEndOfRow
  if isEnd
     then return []
     else (:) <$> parseField <*> asList

isEndOfRow :: RowParser Bool
isEndOfRow  = RowParser $ \(Row xs) -> Right (Row xs, null xs)

fieldInfo :: FieldParser Field
fieldInfo = FieldParser $ \x -> Right x

parseFailure :: Exception e => e -> FieldParser a
parseFailure e = FieldParser $ \_ -> Left (Unexpected $ toException e)
