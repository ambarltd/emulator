{-| Record encoding is about the format in which records are kept in queue.
   The goals are to enable fast filtering and efficient storage.
-}
module Ambar.Record.Encoding
  ( Encode(..)
  , Decode(..)
  , TaggedJson
  ) where

import qualified Data.Aeson as Json
import Data.Aeson (ToJSON(..), FromJSON)
import Data.Base64.Types (extractBase64)
import Data.ByteString.Base64 (encodeBase64)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Text (Text)

import Ambar.Record (Record(..), Schema, Value(..), Bytes(..))

class Encode a where
  encode :: Record -> a

class Decode a where
  decode :: Schema -> a -> Either Text Record

newtype TaggedJson = TaggedJson (Map Text Value)
  deriving newtype (ToJSON, FromJSON)

instance Encode TaggedJson where
  encode (Record fields) = TaggedJson $ Map.fromList fields

instance Decode TaggedJson where
  decode _ (TaggedJson bs) = Right $ Record $ Map.toList bs

instance Encode Record where
  encode = id

instance Decode Record where
  decode _ = Right

instance Encode Json.Value where
  encode (Record fields) =
    Json.toJSON $ Map.fromList
      [ (name, untagged value)
      | (name, value) <- fields
      ]
    where
    untagged = \case
      Boolean x -> toJSON x
      Int x -> toJSON x
      UInt x -> toJSON x
      Real x -> toJSON x
      String x -> toJSON x
      Binary (Bytes x) -> Json.String $ extractBase64 $ encodeBase64 x
      Json txt _ -> toJSON txt
      DateTime x -> toJSON x
      Null -> Json.Null
