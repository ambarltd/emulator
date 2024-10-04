module Main where

import qualified Options.Applicative as O
import qualified Options.Applicative.Help.Pretty as OP

main :: IO ()
main = do
  Options <- O.execParser cliOptions
  putStrLn "Ambar Lite"

data Options = Options

cliOptions :: O.ParserInfo Options
cliOptions = O.info (O.helper <*> parser) $ mconcat
  [ O.fullDesc
  , O.headerDoc $ Just $ OP.vcat
    [ "Ambar Lite"
    , ""
    , OP.indent 2 $ OP.vcat
      [ "A local version of Ambar <https://ambar.cloud>"
      , "Connect your databases to multiple consumers with minimal configuration and no libraries needed."
      ]
    ]
  , O.footerDoc $ Just $
      "More info at <https://github.com/ambarltd/lite>"
      <> OP.line
  ]
  where
    parser = pure Options


