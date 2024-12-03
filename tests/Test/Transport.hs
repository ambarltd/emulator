module Test.Transport
  ( testTransport
  ) where

import Control.Concurrent (MVar, Chan, newMVar, modifyMVar, newChan, writeChan, readChan)
import Control.Monad (replicateM, forM_)
import Control.Exception (throwIO, ErrorCall(..))
import Data.Aeson (ToJSON, FromJSON)
import qualified Data.Aeson as Json
import Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as LB
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import Data.Time.Clock.POSIX (getPOSIXTime)
import Network.Wai (Request)
import GHC.Stack (HasCallStack)
import qualified Network.Wai as Wai
import Network.Wai.Handler.Warp (Port)
import Network.HTTP.Types (Status(..))
import qualified Network.Wai.Handler.Warp as Warp
import qualified Network.Wai.Middleware.HttpAuth as Wai
import System.IO.Unsafe (unsafePerformIO)
import Test.Hspec
  ( Spec
  , it
  , describe
  , shouldBe
  )

import qualified Ambar.Transport.Http as HttpTransport
import Ambar.Transport (Transport(..))
import Ambar.Transport.Http (Endpoint(..), User(..), Password(..))

import Utils.Async (withAsyncThrow)
import Utils.Some (Some(..))

testTransport :: Spec
testTransport = describe "transport" $ do
  testHttpTransport

testHttpTransport :: HasCallStack => Spec
testHttpTransport = describe "Http" $
  it "sends authenticated requests" $
    withHttpTransport $ \transport getResult -> do
    let submit :: ToJSON a => a -> IO ()
        submit val = do
          r <- sendJSON transport (LB.toStrict $ Json.encode val)
          forM_ r $ \err -> throwIO $ ErrorCall $ show err

        receive :: FromJSON a => IO a
        receive = do
          r <- getResult
          case Json.eitherDecode' r of
            Left err -> throwIO $ ErrorCall $
              "Error decoding received message: " <> err <> "\n" <> show r
            Right v -> return v

        entries = [("A", 1), ("B", 2), ("C", 3)] :: [(String, Int)]

    forM_ entries submit
    received <- replicateM (length entries) receive
    received `shouldBe` entries

data Creds = Creds User Password

withHttpTransport :: (Some Transport -> IO ByteString -> IO a) -> IO a
withHttpTransport act =
  withHttpServer $ \(Creds user pass) endpoint getResponseBody -> do
  transport <- HttpTransport.new endpoint user pass
  act (Some transport) getResponseBody

{-# NOINLINE lastPort #-}
lastPort :: MVar Port
lastPort = unsafePerformIO (newMVar 49152)

nextPort :: IO Port
nextPort = modifyMVar lastPort $ \n -> return (n + 1, n)

withHttpServer :: (Creds -> Endpoint -> IO ByteString -> IO a) -> IO a
withHttpServer act = do
  t <- getPOSIXTime
  port <- nextPort
  chan <- newChan
  let user = User $ "user_" <> Text.pack (show t)
      pass = Password $ "pass_" <> Text.pack (show t)
      creds = Creds user pass
      readResponse = readChan chan
      endpoint = Endpoint $ Text.pack $ "http://localhost:" <> show port <> "/endpoint"
  withAsyncThrow (server chan creds port) $
    act creds endpoint readResponse
  where
  server :: Chan ByteString -> Creds -> Port -> IO ()
  server chan creds port = Warp.run port $ withAuth creds $ requestHandler chan

  withAuth :: Creds -> Wai.Application -> Wai.Application
  withAuth creds f = Wai.basicAuth checkCreds authSettings f
    where
    authSettings = "realm" :: Wai.AuthSettings
    Creds (User user) (Password pass) = creds
    checkCreds bsuser bspass = return $
      Text.decodeUtf8 bsuser == user &&
      Text.decodeUtf8 bspass == pass

  requestHandler
    :: Chan ByteString
    -> Request
    -> (Wai.Response -> IO Wai.ResponseReceived)
    -> IO Wai.ResponseReceived
  requestHandler chan req respond = do
    response <- case toResponse req of
      Left (status, msg) ->
        return $ Wai.responseLBS status [] msg
      Right (status, msg) -> do
        body <- Wai.consumeRequestBodyStrict req
        writeChan chan body
        return $ Wai.responseLBS status [] msg
    respond response

  toResponse :: Request -> Either (Status, ByteString) (Status, ByteString)
  toResponse req = do
    methodIs "POST"
    pathIs ["endpoint"]
    return (Status 200 "Success", "{ \"result\": { \"success\" : {} } }")
    where
    asBS = LB.fromStrict . Text.encodeUtf8
    methodIs m = do
      let method = Text.decodeUtf8 $ Wai.requestMethod req
      if method == m
        then return ()
        else Left (Status 403 "Invalid Method", asBS $ "Expected " <> m <> " but got " <> method)

    pathIs p = do
      let path = Wai.pathInfo req
      if path == p
         then return ()
         else Left
          (Status 404 "Unknown Endpoint", asBS $ "Unknown endpoint. Only valid path is '/" <> Text.intercalate "/" p <> "'")




