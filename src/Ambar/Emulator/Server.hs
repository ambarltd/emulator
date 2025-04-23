module Ambar.Emulator.Server
  ( run
  , Port(..)
  ) where

import Data.Aeson (toJSON, encode)
import qualified Data.HashMap.Strict as HashMap
import Data.Void (Void)
import qualified Network.Wai.Handler.Warp as Warp
import Network.Wai (Application, Response, responseLBS, rawPathInfo)
import Network.HTTP.Types (status200, status404, hContentType)
import System.IO (hPutStrLn, stderr)

import Ambar.Emulator.Queue (Queue, getInfo)

newtype Port = Port Int
    deriving (Show, Eq, Read)

run :: Port -> Queue -> IO Void
run (Port port) queue = do
  hPutStrLn stderr $ "Running server on port " <> show port
  Warp.run port $ app queue
  error "server terminated unexpectedly"

app :: Queue -> Application
app queue request respond = do
  response <- case rawPathInfo request of
    "/projections" -> projections queue
    _   -> return pageNotFound
  respond response

pageNotFound :: Response
pageNotFound = responseLBS
    status404
    [("Content-Type", "text/plain")]
    "404 - Not Found"

projections :: Queue -> IO Response
projections queue = do
  info <- getInfo queue
  let response = fmap (\(partitions, consumers) -> HashMap.fromList
        [ ("partitions" :: String, toJSON partitions)
        , ("projections", toJSON consumers)
        ]) info
  return $ responseLBS status200 [(hContentType, "application/json")] (encode response)
