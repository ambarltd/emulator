module Test.Connector
  ( testConnectors
  , withPostgresSQL
  ) where

import Data.List (isInfixOf)
import Data.Text (Text)
import qualified Data.Text as Text
import Control.Exception (bracket, throwIO, ErrorCall(..))
import System.Process (readProcessWithExitCode)
import System.Exit (ExitCode(..))
import Test.Hspec
  ( Spec
  , it
  , describe
  , shouldBe
  )

import Connector.Poll
  ( BoundaryTracker(..)
  , Boundaries(..)
  , rangeTracker
  )
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
testPostgreSQL lcreds = do
  describe "postgreSQL" $ do
    it "works" $
      OnDemand.with lcreds $ \creds ->
      p_username creds `shouldBe` "conn_test"

data PostgresCreds = PostgresCreds
  { p_database :: Text
  , p_username :: Text
  , p_password :: Text
  }

withPostgresSQL :: (PostgresCreds -> IO a) -> IO a
withPostgresSQL f = bracket setup teardown f
  where
  setup = do
    let creds@PostgresCreds{..} = PostgresCreds
          { p_database = "db_test"
          , p_username = "conn_test"
          , p_password = ""
          }
    createUser p_username
    createDatabase p_username p_database
    return creds

  teardown PostgresCreds{..} = do
    deleteDatabase p_database
    dropUser p_username

  createUser name = do
    (code, _, err) <- readProcessWithExitCode "createuser" ["--superuser", Text.unpack name] ""
    case code of
      ExitSuccess -> return ()
      ExitFailure 1 | "already exists" `isInfixOf` err -> return ()
      _ -> throwIO $ ErrorCall $ "Unable to create PostgreSQL user: " <> err

  createDatabase user name = do
    (code, _, err) <- readProcessWithExitCode "createdb"
      [ "--username", Text.unpack user
      , "--no-password"
      , Text.unpack name
      ] ""
    case code of
      ExitSuccess -> return ()
      ExitFailure 1 | "already exists" `isInfixOf` err -> return ()
      _ -> throwIO $ ErrorCall $ "Unable to create PostgreSQL database: " <> err

  dropUser name = do
    (code, _, err) <- readProcessWithExitCode "dropuser" [Text.unpack name] ""
    case code of
      ExitSuccess -> return ()
      _ -> throwIO $ ErrorCall $ "Unable to delete PostgreSQL user: " <> err

  deleteDatabase name = do
    (code, _, err) <- readProcessWithExitCode "dropdb" [Text.unpack name] ""
    case code of
      ExitSuccess -> return ()
      _ -> throwIO $ ErrorCall $ "Unable to delete PostgreSQL database: " <> err
