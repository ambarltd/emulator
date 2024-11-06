{-# LANGUAGE ApplicativeDo #-}
module Main where

import Control.Applicative (optional)
import Data.Maybe (fromMaybe)
import qualified Options.Applicative as O
import qualified Options.Applicative.Help.Pretty as OP
import System.Directory (createDirectoryIfMissing, getXdgDirectory, XdgDirectory(..))
import Prettyprinter (pretty)

import Ambar.Emulator (emulate)
import Ambar.Emulator.Config (parseEnvConfigFile, EmulatorConfig(..))
import Utils.Logger (plainLogger, Severity(..))

_DEFAULT_PARTITIONS_PER_TOPIC :: Int
_DEFAULT_PARTITIONS_PER_TOPIC = 10

-- | Version of the binary file
_VERSION :: String
_VERSION = "v0.0.1 - alpha release"

main :: IO ()
main = do
  cmd <- O.execParser cliOptions
  case cmd of
    CmdRun{..} -> do
      env <- parseEnvConfigFile o_configPath
      queue <- maybe defaultStatePath return o_statePath
      let config = EmulatorConfig
            { c_partitionsPerTopic = fromMaybe _DEFAULT_PARTITIONS_PER_TOPIC o_partitionsPerTopic
            , c_dataPath = queue
            }
          severity = if o_verbose then Debug else Info
      emulate (plainLogger severity) config env
    CmdVersion ->
      print _VERSION

defaultStatePath :: IO FilePath
defaultStatePath = do
  -- here's some discussion why history files belong in XDG_DATA_HOME:
  --   https://github.com/fish-shell/fish-shell/issues/744
  dir <- getXdgDirectory XdgData "ambar-emulator"
  createDirectoryIfMissing True dir
  return dir

data Command
  = CmdRun
    { o_partitionsPerTopic :: Maybe Int
    , o_statePath :: Maybe FilePath
    , o_configPath :: FilePath
    , o_verbose :: Bool
    }
  | CmdVersion

cliOptions :: O.ParserInfo Command
cliOptions = O.info (O.simpleVersioner _VERSION <*> O.helper <*> parser) $ mconcat
  [ O.fullDesc
  , O.headerDoc $ Just $ OP.vcat
    [ "Ambar Emulator " <> pretty _VERSION
    , ""
    , OP.indent 2 $ OP.vcat
      [ "A local version of Ambar <https://ambar.cloud>"
      , "Connect your databases to multiple consumers with minimal configuration and no libraries needed."
      ]
    ]
  , O.footerDoc $ Just $
      "More info at <https://github.com/ambarltd/emulator>"
      <> OP.line
  ]
  where
    parser = O.subparser $ mconcat
      [ O.command "run"
        $ O.info (O.helper <*> parserRun)
        $ O.progDesc "run the emulator"
      ]

    parserRun = do
      o_partitionsPerTopic <- optional $ O.option O.auto $ mconcat
          [ O.long "partitions-per-topic"
          , O.metavar "INT"
          , O.help "How many partitions should newly created topics have."
          ]
      o_statePath <- optional $ O.strOption $ mconcat
          [ O.long "data-path"
          , O.metavar "PATH"
          , O.help "Where to put emulation data including file queues. Defaults to $XDG_DATA_HOME/ambar-emulator."
          ]
      o_configPath <- O.strOption $ mconcat
          [ O.long "config"
          , O.metavar "FILE"
          , O.help "Yaml file with environment configuration. Spec at at <https://github.com/ambarltd/emulator>."
          ]
      o_verbose <- O.switch $ mconcat
          [ O.long "verbose"
          , O.help "Enable verbose logging."
          ]
      return CmdRun{..}


