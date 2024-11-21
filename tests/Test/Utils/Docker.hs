module Test.Utils.Docker
  ( DockerCommand(..)
  , withDocker
  ) where

import Control.Concurrent (MVar, newMVar, modifyMVar)
import Control.Exception (throwIO, ErrorCall(..))
import System.Exit (ExitCode(..))
import System.IO.Temp (withSystemTempFile)
import System.IO
  ( Handle
  , IOMode(..)
  , BufferMode(..)
  , hClose
  , hSetBuffering
  , stdout
  , withFile
  )
import System.Process
  ( CreateProcess(..)
  , StdStream(..)
  , proc
  , withCreateProcess
  , waitForProcess
  , createPipe
  )
import System.IO.Unsafe (unsafePerformIO)
import Utils.Async (withAsyncThrow)

data DockerCommand
  = DockerRun
    { run_image :: String
    , run_args :: [String]
    }

{-# NOINLINE dockerImageNumber #-}
dockerImageNumber :: MVar Int
dockerImageNumber = unsafePerformIO (newMVar 0)

-- | Run a command with a docker image running in the background.
-- Automatically assigns a container name and removes the container
-- on exit.
--
-- The handle provided contains both stdout and stderr
withDocker :: String -> DockerCommand -> (Handle -> IO a) -> IO a
withDocker tag cmd act =
  withPipe $ \hread hwrite -> do
  name <- mkName
  let create = (proc "docker" (args name))
        { std_out = UseHandle hwrite
        , std_err = UseHandle hwrite
        , create_group = True
        }
  withCreateProcess create $ \_ _ _ p ->
    withAsyncThrow (wait name p) $
    act hread
  where
  withPipe f = do
    (hread, hwrite) <- createPipe
    hSetBuffering hread LineBuffering
    hSetBuffering hwrite LineBuffering
    f hread hwrite

  wait name p = do
    exit <- waitForProcess p
    throwIO $ ErrorCall $ case exit of
      ExitSuccess ->  "unexpected successful termination of container " <> name
      ExitFailure code ->
        "docker failed with exit code" <> show code <> " for container " <> name

  mkName = do
    number <- modifyMVar dockerImageNumber $ \n -> return (n + 1, n)
    return $ tag <> "_" <> show number

  args :: String -> [String]
  args name =
    case cmd of
      DockerRun img opts ->
        [ "run"
        , "--init" -- ensure SIGTERM from `withCreateProcess` kills the container
        , "--rm"   -- remove container on exit
        , "--name", name -- name this run
        ] ++ opts ++ [img]

  _withHandle name f = do
    let debug = False -- set to true to print Docker output to sdtdout
        mhandle =
          if debug
          then Just stdout
          else Nothing
    case mhandle of
      Just handle -> f handle
      Nothing -> withSystemTempFile (name <> "-output") (\path h0 -> do
        hClose h0
        withFile path ReadWriteMode f
        )
