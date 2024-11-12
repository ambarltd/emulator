module Utils.Prettyprinter
  ( renderPretty
  , sepBy
  , commaSeparated
  , prettyJSON
  ) where

import qualified Data.Aeson as Aeson
import Data.Text (Text)
import Data.Text.Lazy.Encoding as Text
import Prettyprinter.Render.Text (renderStrict)
import Prettyprinter (Doc, pretty, layoutSmart, defaultLayoutOptions, concatWith, (<+>))

renderPretty :: Doc ann -> Text
renderPretty = renderStrict . layoutSmart defaultLayoutOptions

sepBy :: Doc ann -> [Doc ann] -> Doc ann
sepBy s = concatWith (\x y -> x <+> s <+> y)

commaSeparated :: [Doc ann] -> Doc ann
commaSeparated = concatWith (\x y -> x <> "," <+> y)

prettyJSON :: Aeson.ToJSON a => a -> Doc ann
prettyJSON = pretty . Text.decodeUtf8 . Aeson.encode
