module Ambar.Transport where

import Data.Text (Text)
import Data.ByteString (ByteString)

import Utils.Some (Some(..))

-- | A transport is a way to send data to some destination.
class Transport a where
  send :: a -> ByteString -> IO (Maybe SubmissionError)

  -- | Optional specialised function to allow json manipulation.
  -- The default implementation just sends it as a bytes string.
  sendJSON :: a -> ByteString -> IO (Maybe SubmissionError)
  sendJSON = send

data SubmissionError
  = UnableToSend Text
  | ClientError Text
  deriving Show

instance Transport (Some Transport) where
  send (Some a) = send a
  sendJSON (Some a) = sendJSON a

instance Transport (ByteString -> IO (Maybe SubmissionError)) where
  send f = f
  sendJSON f = f
