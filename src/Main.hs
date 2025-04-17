{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE CPP #-}
module Main where

import Control.Applicative (optional)
import Data.Maybe (fromMaybe)
import qualified Options.Applicative as O
import qualified Options.Applicative.Help.Pretty as OP
import System.Directory (createDirectoryIfMissing, getXdgDirectory, XdgDirectory(..))
import Prettyprinter (pretty)

#if !defined(mingw32_HOST_OS)
import Control.Concurrent (myThreadId)
import Control.Exception (throwTo, AsyncException(UserInterrupt))
import System.Posix.Signals (installHandler, sigINT, sigTERM, Handler(Catch))
#endif

import Ambar.Emulator (emulate)
import Ambar.Emulator.Config (parseEnvConfigFile, EmulatorConfig(..), Port(..))
import Ambar.Emulator.Connector.Poll (PollingInterval(..))
import Util.Logger (plainLogger, Severity(..), logInfo)
import Util.Delay (fromDiffTime)


_DEFAULT_PARTITIONS_PER_TOPIC :: Int
_DEFAULT_PARTITIONS_PER_TOPIC = 10

_DEFAULT_PORT :: Port
_DEFAULT_PORT = Port 8080

-- | Version of the binary file
_VERSION :: String
_VERSION = "v0.0.1 - alpha release"

main :: IO ()
main = do
  handleInterrupts
  cmd <- O.execParser cliOptions
  case cmd of
    CmdRun{..} -> do
      let logger = plainLogger severity
          severity = if o_verbose then Debug else Info

      env <- parseEnvConfigFile o_configPath
      logInfo @String logger "configuration loaded"

      queue <- maybe defaultStatePath return o_statePath
      let config = EmulatorConfig
            { c_partitionsPerTopic = fromMaybe _DEFAULT_PARTITIONS_PER_TOPIC o_partitionsPerTopic
            , c_port = o_port
            , c_dataPath = queue
            }

      emulate logger config env
    CmdVersion ->
      print _VERSION
  where
  handleInterrupts = do
#if !defined(mingw32_HOST_OS)
    tid <- myThreadId
    let interrupt = Catch (throwTo tid UserInterrupt)
    _ <- installHandler sigINT interrupt Nothing
    _ <- installHandler sigTERM interrupt Nothing
#endif
    return ()


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
    , o_port :: Port
    , o_overridePollingInterval :: Maybe PollingInterval
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
      o_port <- fmap Port $ O.option O.auto $ mconcat
          [ O.long "port"
          , O.metavar "INT"
          , O.help "Port to attach projections info server to."
          , O.value 8080
          , O.showDefault
          ]
      o_overridePollingInterval <-
        fmap (fmap $ PollingInterval . fromDiffTime . realToFrac @Double) $ optional $ O.option O.auto $ mconcat
          [ O.long "override-polling-interval"
          , O.metavar "SECONDS"
          , O.help "Override the polling interval for all polled data sources."
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


