module Ambar.Transport.Http
  ( HttpTransport
  , new
  , Endpoint(..)
  , User(..)
  , Password(..)
  , ClientResponse(..)
  , ClientOutcome(..)
  , ClientErr(..)
  , ErrorPolicy(..)
  ) where

import Control.Applicative ((<|>))
import Control.Exception (try, fromException)
import Data.Aeson (ToJSON, FromJSON, (.:), withObject)
import qualified Data.Aeson as Json
import qualified Data.ByteString.Lazy as LB
import Data.ByteString.Lazy (ByteString)
import Data.List (stripPrefix)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Text.Casing as Casing
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import GHC.Generics (Generic)
import qualified Network.HTTP.Client as Http
import qualified Network.HTTP.Client.TLS as Http

import Ambar.Transport (Transport(..), SubmissionError(..))

data HttpTransport = HttpTransport
  { _base_req :: Http.Request
  , _manager :: Http.Manager
  }

newtype Endpoint = Endpoint Text
  deriving newtype (ToJSON, FromJSON, Eq, Ord)
newtype User = User Text
  deriving newtype (ToJSON, FromJSON, Eq, Ord)
newtype Password = Password Text
  deriving newtype (ToJSON, FromJSON, Eq, Ord)

new :: Endpoint -> User -> Password -> IO HttpTransport
new (Endpoint url) (User user) (Password pass) = do
  case Http.parseUrlThrow (Text.unpack url) of
    Left ex -> error $ show $
      case fromException ex of
        Just err -> prettyHttpError err
        Nothing -> Text.pack $ show ex
    Right req -> do
      manager <- Http.newTlsManager
      let base = Http.applyBasicAuth (Text.encodeUtf8 user) (Text.encodeUtf8 pass) req
      return $ HttpTransport base manager

instance Transport HttpTransport where
  send (HttpTransport base manager) bs = do
    r <- try $ Http.httpLbs req manager
    case r of
      Left err -> return $ Just $ UnableToSend (prettyHttpError err)
      Right response -> return $ decode $ Http.responseBody response
    where
      req = base
        { Http.requestBody = Http.RequestBodyBS bs
        , Http.method = "POST"
        , Http.requestHeaders =
            [("Content-Type", "application/json")] <> Http.requestHeaders base
        }

      decode :: ByteString -> Maybe SubmissionError
      decode body =
        case Json.eitherDecode body of
          Right (ClientResponse Success) -> Nothing
          Right (ClientResponse (Error err)) ->
            case err of
              ClientErr KeepGoing _ _ -> Nothing
              ClientErr MustRetry _ _ -> Just $ ClientError $
                Text.decodeUtf8 $ LB.toStrict $ Json.encode err
          Left err -> Just $ ClientError $
            "Unable to decode client response: " <> Text.pack err
            <> "\nFull response: " <> Text.decodeUtf8 (LB.toStrict body)

newtype ClientResponse = ClientResponse { result :: ClientOutcome }
  deriving (Generic, Show, Eq)

instance FromJSON ClientResponse

data ClientOutcome
  = Success
  | Error ClientErr
  deriving (Generic, Show, Eq)

instance FromJSON ClientOutcome where
  parseJSON = withObject "ClientOutcome" $ \obj ->
    success obj <|> failure obj
    where
    success obj = do
      r <- obj .: "success"
      Json.withObject "Success" (const $ return Success) r

    failure obj = do
      r <- obj .: "error"
      return $ Error r

data ClientErr = ClientErr
  { e_policy :: ErrorPolicy
  , e_class :: Text
  , e_description :: Text
  }
  deriving (Generic, Show, Eq)

clientErrOptions :: Json.Options
clientErrOptions = Json.defaultOptions
  { Json.fieldLabelModifier = \label ->
    fromMaybe label (stripPrefix "e_" label)
  }
instance FromJSON ClientErr where
  parseJSON = Json.genericParseJSON clientErrOptions
instance ToJSON ClientErr where
  toJSON = Json.genericToJSON clientErrOptions

data ErrorPolicy
  = MustRetry
  | KeepGoing
  deriving (Generic, Show, Eq)


errorPolicyOptions :: Json.Options
errorPolicyOptions = Json.defaultOptions
  { Json.constructorTagModifier = Casing.quietSnake }
instance FromJSON ErrorPolicy where
  parseJSON = Json.genericParseJSON errorPolicyOptions
instance ToJSON ErrorPolicy where
  toJSON = Json.genericToJSON errorPolicyOptions

prettyHttpError :: Http.HttpException -> Text
prettyHttpError httpErr = "*** HTTP Error: " <> case httpErr of
  Http.InvalidUrlException _ msg ->
     "invalid URL: " <> Text.pack msg
  Http.HttpExceptionRequest _ err -> case err of
    Http.StatusCodeException res _ ->
      "invalid response status: " <> Text.pack (show $ Http.responseStatus res)
    Http.TooManyRedirects _ -> "too many redirects"
    Http.OverlongHeaders -> "overlong headers"
    Http.ResponseTimeout -> "response timeout"
    Http.ConnectionTimeout -> "connection timeout"
    Http.ConnectionFailure _ ->
      "connection failure. Check your internet connection"
    Http.InvalidStatusLine _ -> "invalid status line"
    Http.InvalidHeader header -> "invalid header: " <> Text.decodeUtf8 header
    Http.InvalidRequestHeader _ -> "invalid request header"
    Http.InternalException e -> "internal exception: " <> Text.pack (show e)
    Http.ProxyConnectException _ _ status ->
      "unable to connect to proxy: " <> Text.pack (show status)
    Http.NoResponseDataReceived -> "no response data received"
    Http.TlsNotSupported -> "tls not supported"
    Http.WrongRequestBodyStreamSize _ _ -> "wrong request stream size"
    Http.ResponseBodyTooShort _ _ -> "reponse body too short"
    Http.InvalidChunkHeaders -> "invalid chunk headers"
    Http.IncompleteHeaders -> "incomplete headers"
    Http.InvalidDestinationHost _ -> "invalid destination host"
    Http.HttpZlibException e -> "zlib exception: " <> Text.pack (show e)
    Http.InvalidProxyEnvironmentVariable var val ->
      "invalid proxy environment var: " <> var <> ": " <> val
    Http.ConnectionClosed -> "connection closed"
    Http.InvalidProxySettings _ -> "invalid proxy settings"




