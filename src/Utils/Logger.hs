module Utils.Logger where

-- | A simple logging module based on co-log, but without the dependencies.

import Control.Applicative ((<|>))
import Control.Concurrent.MVar (MVar, newMVar, withMVar)
import Control.Monad ((<=<), when)
import Prettyprinter (Doc, Pretty(..), (<+>), defaultLayoutOptions, layoutSmart)
import Prettyprinter.Render.Text (renderStrict)
import qualified Data.Text.IO as Text
import Data.String (fromString)
import Data.Text (Text)
import Data.Maybe (listToMaybe)
import Data.Time.Clock (getCurrentTime, UTCTime)
import qualified Data.Time.Format as Time
import GHC.Exception (SrcLoc(..), getCallStack)
import GHC.Stack (HasCallStack, callStack)
import System.Exit (exitFailure)
import System.IO (stderr)
import System.IO.Unsafe (unsafePerformIO)
import System.FilePath (takeFileName)

-- | How to log a message
data Logger m message = Monad m => Logger { logMsg :: HasCallStack => message -> m () }

-- | Use to add an add-on to a logger
enhance :: (b -> a) -> Logger m a -> Logger m b
enhance f (Logger g) = Logger $ g . f

enhanceM :: Monad m => (b -> m a) -> Logger m a -> Logger m b
enhanceM f (Logger g) = Logger $ g <=< f

filterM
  :: (a -> Bool)  -- ^ filtering predicate
  -> Logger m a   -- ^ logger to use if filter is passed
  -> Logger m a
filterM check (Logger g) = Logger $ \msg ->
  when (check msg) $ g msg

-- ============================================================================
-- Add-ons
-- ============================================================================

data WithTimeStamp a = WithTimeStamp UTCTime a

instance Pretty a => Pretty (WithTimeStamp a) where
  pretty (WithTimeStamp time a) =
    "[" <> fromString str <> "]" <+> pretty a
    where
    str = Time.formatTime Time.defaultTimeLocale format time
    format = "%Y-%m-%d %H:%M:%S %Z"

withTimeStamp :: a -> IO (WithTimeStamp a)
withTimeStamp msg = do
  now <- getCurrentTime
  return (WithTimeStamp now msg)

data WithLocation a = WithLocation SrcLoc a

instance Pretty a => Pretty (WithLocation a) where
  pretty (WithLocation SrcLoc{..} a) =
    "[" <> fromString (takeFileName srcLocFile) <> ":"
    <> pretty srcLocStartLine <> ":"
    <> pretty srcLocStartCol <> "]"
    <+> pretty a

getLocation :: HasCallStack => Int -> IO SrcLoc
getLocation callsAgo =
  case target <|> lastEntry of
    Just (_,callerLoc) -> return callerLoc
    _ -> error "Insufficient call stack"
  where
  stack = getCallStack callStack
  target = listToMaybe $ drop callsAgo stack
  lastEntry = listToMaybe $ reverse stack


data Severity
  = Fatal
  | Warn
  | Info
  | Debug
  deriving (Eq, Show, Ord)

data WithSeverity a = WithSeverity Severity a
  deriving (Functor, Traversable, Foldable)

instance Pretty a => Pretty (WithSeverity a) where
  pretty (WithSeverity severity a) =
    case severity of
      Fatal -> "[FATAL]" <+> pretty a
      Warn -> "[WARN]" <+> pretty a
      Info -> pretty a
      Debug -> "[DEBUG]" <+> pretty a

-- ============================================================================
-- Loggers
-- ============================================================================

-- | A logger that ignores logs
noopLogger :: Logger IO a
noopLogger = Logger $ \_ -> return ()

type Mutex = MVar ()

{-# NOINLINE mutex #-}
mutex :: Mutex
mutex = unsafePerformIO $ newMVar ()

serialised :: Logger IO a -> Logger IO a
serialised (Logger f) = Logger $ \msg ->
  withMVar mutex $ \() ->
    f msg

-- | Logs everything to stderr.
standardLogger :: Logger IO Text
standardLogger = Logger $ Text.hPutStrLn stderr

-- | Log up to a set log level, including location for errors and debugging.
-- Serializes all logs, making it not suitable for high performance logging.
plainLogger :: Severity -> SimpleLogger
plainLogger maxSeverity =
  filterM (\(WithSeverity s _) -> s <= maxSeverity) $
  enhance prettify $
  serialised standardLogger

prettify :: Pretty a => a -> Text
prettify = renderStrict . layoutSmart defaultLayoutOptions . pretty

-- ============================================================================
-- Simple logging
-- ============================================================================

-- Add a note before the logged msg.
annotate :: Text -> SimpleLogger -> SimpleLogger
annotate txt = enhance f
  where
  f (WithSeverity s (PrettyT p)) =
    WithSeverity s $ PrettyT $ pretty txt <> " | " <> p

newtype PrettyT = PrettyT (forall a. Doc a)

instance Pretty PrettyT where
  pretty (PrettyT v) = v

type SimpleLogger = Logger IO (WithSeverity PrettyT)

logInfo :: (HasCallStack, Pretty a) => SimpleLogger -> a -> IO ()
logInfo logger msg = logMsg logger (WithSeverity Info $ PrettyT $ pretty msg)

logWarn :: (HasCallStack, Pretty a) => SimpleLogger -> a -> IO ()
logWarn logger msg = logMsg logger (WithSeverity Warn $ PrettyT $ pretty msg)

logDebugAction :: (HasCallStack) => SimpleLogger -> Text -> IO b -> IO b
logDebugAction logger msg act = do
  logDebug logger (msg <> ": start")
  r <- act
  logDebug logger (msg <> ": end")
  return r

logDebug :: (HasCallStack, Pretty a) => SimpleLogger -> a -> IO ()
logDebug logger msg = do
  loc <- getLocation 2
  logMsg logger (WithSeverity Debug $ PrettyT $ pretty $ WithLocation loc msg)

logFatal :: (HasCallStack, Pretty a) => SimpleLogger -> a -> IO ()
logFatal logger msg = logMsg logger (WithSeverity Fatal $ PrettyT $ pretty msg)

fatal :: (HasCallStack, Pretty a) => SimpleLogger -> a -> b
fatal logger msg = unsafePerformIO $ do
  logFatal logger msg
  exitFailure

