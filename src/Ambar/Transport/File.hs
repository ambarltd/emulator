module Ambar.Transport.File
  ( File
  , withFileTransport
  )
  where

import Control.Concurrent.MVar (MVar, newMVar, withMVar)
import qualified Data.Text.IO as Text
import qualified Data.Text.Encoding as Text
import Data.Base64.Types (extractBase64)
import Data.ByteString.Base64 (encodeBase64)
import System.IO (Handle, IOMode(..), BufferMode(..), withFile, hSetBuffering)

import Ambar.Transport (Transport(..))

-- | Project data into a file
newtype File = File (MVar Handle)

withFileTransport :: FilePath -> (File -> IO a) -> IO a
withFileTransport path act =
  withFile path WriteMode $ \handle -> do
    hSetBuffering handle LineBuffering
    var <- newMVar handle
    act (File var)

instance Transport File where
  send (File var) bs =
    withMVar var $ \handle -> do
    Text.hPutStrLn handle (extractBase64 $ encodeBase64 bs)
    return Nothing

  sendJSON (File var) msg =
    withMVar var $ \handle -> do
    Text.hPutStrLn handle $ Text.decodeUtf8 msg
    return Nothing
