module Test.Projector (testProjector) where

import qualified Data.Aeson as Json
import Data.Aeson ((.=))
import qualified Data.Set as Set
import Data.Text (Text)
import Test.Hspec (Spec, it, describe, shouldBe)

import Ambar.Emulator.Config (DestinationFilter(..))
import Ambar.Emulator.Projector (matchesFilter, Payload(..))

testProjector :: Spec
testProjector =
  describe "projector filter" $ do
    it "no filter delivers everything" $
      matchesFilter Nothing (row "CustomerCreated") `shouldBe` True

    it "delivers records whose filter-column value is allowed" $
      matchesFilter (Just orderFilter) (row "OrderCreated") `shouldBe` True

    it "skips records whose filter-column value is not allowed" $
      matchesFilter (Just orderFilter) (row "CustomerCreated") `shouldBe` False

    it "fails open when the filter column is missing from the record" $
      matchesFilter (Just (filterOn "no_such_column")) (row "OrderCreated") `shouldBe` True

    it "fails open when the filter-column value is not a string" $
      matchesFilter (Just (filterOn "amount")) (row "CustomerCreated") `shouldBe` True

    it "fails open when the record is not an object" $
      matchesFilter (Just orderFilter) (Payload (Json.String "just text")) `shouldBe` True
  where
  orderFilter = filterOn "event_name"

  filterOn column = DestinationFilter
    { f_column = column
    , f_values = Set.fromList ["OrderCreated", "OrderUpdated"]
    }

  row eventName = Payload $ Json.object
    [ "id" .= (1 :: Int)
    , "event_name" .= (eventName :: Text)
    , "amount" .= (42 :: Int)
    ]
