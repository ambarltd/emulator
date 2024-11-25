
-- | A thread-safe wrapper around the mysql and mysql-simple libraries.
module Database.MySQL
  ( query
  , query_
  , executeMany
  , execute_
  , Connection
  , ConnectionInfo(..)
  , withConnection
  )
  where

import Control.Concurrent (MVar, newMVar, newEmptyMVar, putMVar, takeMVar, tryPutMVar, modifyMVar_)
import Control.Exception (SomeException, bracket, throwIO)
import Control.Monad (unless)
import Data.Int (Int64)
import Control.Monad (forever)
import qualified Data.Text as Text
import Data.Text (Text)
import Data.Word (Word16)
import System.IO.Unsafe (unsafePerformIO)

import qualified Database.MySQL.Base as M (MySQLError(..), initLibrary, initThread, endThread)
import qualified Database.MySQL.Simple as M
import qualified Database.MySQL.Simple.QueryResults as M
import qualified Database.MySQL.Simple.QueryParams as M

import Utils.Exception (annotateWith, tryAll)
import Utils.Async (withAsyncBoundThrow)

query :: (M.QueryParams q, M.QueryResults r) => Connection -> M.Query -> q -> IO [r]
query (Connection run) q params = run $ \conn -> annotateQ q $ M.query conn q params

query_ :: (M.QueryResults r) => Connection -> M.Query -> IO [r]
query_ (Connection run) q = run $ \conn -> annotateQ q $ M.query_ conn q
  where

execute_ :: Connection -> M.Query -> IO Int64
execute_ (Connection run) q = run $ \conn -> annotateQ q $ M.execute_ conn q

executeMany :: M.QueryParams q => Connection -> M.Query -> [q] -> IO Int64
executeMany (Connection run) q ps = run $ \conn -> annotateQ q $ M.executeMany conn q ps

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
