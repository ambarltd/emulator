module Test.Connector
  ( testConnectors
  , withDatabases
  , Databases(..)
  ) where

import Test.Hspec
  ( Spec
  , it
  , describe
  , shouldBe
  )
import Ambar.Emulator.Connector.Poll (Boundaries(..), mark, boundaries, compact)
import Test.Connector.File (testFileConnector)
import Test.Connector.PostgreSQL (PostgresCreds, testPostgreSQL, withPostgreSQL)
import Test.Connector.MySQL (MySQLCreds, testMySQL, withMySQL)
import Test.Connector.MicrosoftSQLServer (MicrosoftSQLServerCreds, testMicrosoftSQLServer, withMicrosoftSQLServer)
import Util.OnDemand (OnDemand)
import qualified Util.OnDemand as OnDemand
import Data.Default

-- | Info to connect to known databases.
data Databases = Databases
  (OnDemand PostgresCreds)
  (OnDemand MySQLCreds)
  (OnDemand MicrosoftSQLServerCreds)

withDatabases :: (Databases -> IO a) -> IO a
withDatabases f =
  OnDemand.withLazy withPostgreSQL $ \pcreds ->
  OnDemand.withLazy withMySQL $ \mcreds ->
  OnDemand.withLazy withMicrosoftSQLServer $ \screds ->
    f (Databases pcreds mcreds screds)

testConnectors :: Databases -> Spec
testConnectors (Databases pcreds mcreds screds) = do
  describe "connector" $ do
    testPollingConnector
    testFileConnector
    testPostgreSQL pcreds
    testMySQL mcreds
    testMicrosoftSQLServer screds

testPollingConnector :: Spec
testPollingConnector = describe "Poll" $
  describe "rangeTracker" $ do
    let bs xs = foldr (uncurry mark) def $ reverse $ zip [0, 1..] xs
    it "one id" $ do
      boundaries (mark 1 1 def) `shouldBe` Boundaries [(1,1)]

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

    it "compact removes ranges with ending lower than time given" $ do
      (boundaries . compact 2 . bs) [1, 5, 3, 7] `shouldBe` Boundaries [(1,5), (7,7)]

    it "compact doesn't remove ranges ending higher than time given" $ do
      (boundaries . compact 2 . bs) [1 ,2, 5, 3] `shouldBe` Boundaries [(1,3), (5,5)]

    it "can compact empty boundaries" $ do
      (boundaries . compact 2 . bs) [] `shouldBe` Boundaries []
