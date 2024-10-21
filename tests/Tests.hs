module Main where

import Test.Hspec (hspec, parallel)

import Test.Queue (testQueues)
import Test.Connector (testConnectors)


{- | Note [How tests work]

# Running a subset of tests

Example: match on test description

  ./util.sh test -- --match "typecheck"

 -}
main :: IO ()
main =
  hspec $ parallel $ do
    -- unit tests use the projector library
    testQueues
    testConnectors
