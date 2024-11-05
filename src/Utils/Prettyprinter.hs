module Utils.Prettyprinter
  ( renderPretty
  , sepBy
  , commaSeparated
  ) where

import Data.Text (Text)
import Prettyprinter.Render.Text (renderStrict)
import Prettyprinter (Doc, layoutSmart, defaultLayoutOptions, concatWith, (<+>))

renderPretty :: Doc ann -> Text
renderPretty = renderStrict . layoutSmart defaultLayoutOptions

sepBy :: Doc ann -> [Doc ann] -> Doc ann
sepBy s = concatWith (\x y -> x <+> s <+> y)

commaSeparated :: [Doc ann] -> Doc ann
commaSeparated = sepBy ","
