module Test.Warden (testWarden) where

import Test.Hspec
  ( Spec
  , it
  , describe
  , shouldBe
  )

testWarden :: Spec
testWarden = describe "Warden" $ do
  it "works" $ () `shouldBe` ()
