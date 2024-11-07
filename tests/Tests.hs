module Main where

import Test.Hspec (hspec, parallel)

import Test.Config (testConfig)
import Test.Emulator (testEmulator)
import Test.Queue (testQueues)
import Test.Connector (testConnectors, withPostgresSQL)
import Test.OnDemand (testOnDemand)
import Test.Warden (testWarden)
import qualified Test.Utils.OnDemand as OnDemand


{- | Note [How tests work]

# Running a subset of tests

Example: match on test description

  ./util.sh test -- --match "typecheck"

 -}
main :: IO ()
main = do
  pcreds <- OnDemand.lazy withPostgresSQL
  hspec $ parallel $ do
    -- unit tests use the projector library
    testOnDemand
    testWarden
    testConfig
    testQueues
    testEmulator pcreds
    testConnectors pcreds
