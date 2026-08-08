module Test.Projector (testProjector) where

import qualified Data.Aeson as Json
import Data.Aeson ((.=))
import qualified Data.Set as Set
import Data.Text (Text)
import Test.Hspec (Spec, it, describe, shouldBe)

import Ambar.Emulator.Config (DestinationFilter(..))
import Ambar.Emulator.Projector (filterVerdict, FilterVerdict(..), Payload(..))

testProjector :: Spec
testProjector =
  describe "projector filter" $ do
    it "no filter delivers everything" $
      filterVerdict Nothing (row "CustomerCreated") `shouldBe` Matched

    it "delivers records whose filter-column value is allowed" $
      filterVerdict (Just orderFilter) (row "OrderCreated") `shouldBe` Matched

    it "skips records whose filter-column value is not allowed" $
      filterVerdict (Just orderFilter) (row "CustomerCreated") `shouldBe` Skipped

    it "fails open (with reason) when the filter column is missing from the record" $
      filterVerdict (Just (filterOn "no_such_column")) (row "OrderCreated")
        `shouldBe` FailedOpen "filter column 'no_such_column' is missing from the record"

    it "fails open (with reason) when the filter-column value is not a string" $
      filterVerdict (Just (filterOn "amount")) (row "CustomerCreated")
        `shouldBe` FailedOpen "filter column 'amount' has a non-string value"

    it "fails open (with reason) when the filter-column value is null" $
      filterVerdict (Just (filterOn "maybe_null")) (row "CustomerCreated")
        `shouldBe` FailedOpen "filter column 'maybe_null' is null"

    it "fails open (with reason) when the record is not an object" $
      filterVerdict (Just orderFilter) (Payload (Json.String "just text"))
        `shouldBe` FailedOpen "record is not a JSON object"
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
    , "maybe_null" .= Json.Null
    ]
