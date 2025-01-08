module Main where

import Test.Hspec (hspec, parallel)

import Test.Config (testConfig)
import Test.Emulator (testEmulator)
import Test.Transport (testTransport)
import Test.Queue (testQueues)
import Test.Connector (testConnectors, withDatabases, Databases(..))

{- | Note [How tests work]

# Running a subset of tests

Example: match on test description

  ./util.sh test -- --match "typecheck"

 -}
main :: IO ()
main =
  withDatabases $ \dbs@(Databases pcreds _ _ ) ->
  hspec $ parallel $ do
    -- unit tests use the projector library
    testConfig
    testQueues
    testTransport
    testEmulator pcreds
    testConnectors dbs
