module Test.Utils.Docker
  ( DockerCommand(..)
  , withDocker
  ) where

import Control.Concurrent (MVar, newMVar, modifyMVar)
import Control.Exception (ErrorCall(..), throwIO)
import Control.Monad (forM_)
import System.Exit (ExitCode(..))
import System.IO
  ( Handle
  , BufferMode(..)
  , hGetContents
  , hPutStrLn
  , hSetBuffering
  , hSetBuffering
  , hGetBuffering
  , hGetEncoding
  , hSetEncoding
  )
import System.Process
  ( CreateProcess(..)
  , StdStream(..)
  , proc
  , getProcessExitCode
  , createPipe
  , withCreateProcess
  )
import System.IO.Unsafe (unsafePerformIO)
import Utils.Async (withAsyncThrow)
import Utils.Delay (seconds, every)

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
withDocker
  :: Bool  -- whether to print docker output to stdout.
  -> String
  -> DockerCommand
  -> (Handle -> IO a) -> IO a
withDocker debug tag cmd act =
  withPipe $ \hread hwrite -> do
  name <- mkName
  let create = (proc "docker" (args name))
        { std_out = UseHandle hwrite
        , std_err = UseHandle hwrite
        , create_group = True
        }
  withCreateProcess create $ \stdin stdout stderr p -> do
    let pinfo = (stdin, stdout, stderr, p)
    withAsyncThrow (waitFor name pinfo) $ do
      if debug
      then tracing name hread act
      else act hread
  where
  withPipe f = do
    (hread, hwrite) <- createPipe
    hSetBuffering hread LineBuffering
    hSetBuffering hwrite LineBuffering
    f hread hwrite

  waitFor name (_,_,_,p) = every (seconds 1) $ do
    mexit <- getProcessExitCode p
    forM_ mexit $ \exit ->
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

-- | Log a handle's content to stdout.
tracing :: String -> Handle -> (Handle -> IO a) -> IO a
tracing name h f = censoring h logIt f
  where
  logIt str = do
    putStrLn $ name <> ": " <> str
    return (Just str)

-- | Perform an action before any line of content goes from one thread to the other
censoring :: Handle -> (String -> IO (Maybe String)) -> (Handle -> IO a) -> IO a
censoring h censor f = do
  buffering <- hGetBuffering h
  mencoding <- hGetEncoding h
  (hread, hwrite) <- createPipe

  forM_ [hread, hwrite] $ \h' -> do
    hSetBuffering h' buffering
    forM_ mencoding (hSetEncoding h')

  let worker = do
        str <- hGetContents h
        forM_ (lines str) $ \line -> do
          r <- censor line
          forM_ r (hPutStrLn hwrite)

  withAsyncThrow worker (f hread)

