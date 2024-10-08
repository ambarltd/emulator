module Main where

import Test.Hspec (hspec, parallel, describe)

import Test.Queue (testQueues)


{- | Note [How tests work]

# Running a subset of tests

Example: match on test description

  ./util.sh test -- --match "typecheck"

 -}
main :: IO ()
main =
  hspec $ parallel $ do
    -- unit tests use the projector library
    describe "unit" $ do
      testQueues
