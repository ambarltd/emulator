module Ambar.Emulator.Connector where

import Control.Concurrent.STM (STM)
import Control.Exception (Exception)

import Ambar.Emulator.Queue.Topic (Producer, Partitioner, Encoder)
import Utils.Logger (SimpleLogger)

class Connector a where
  type ConnectorState a = b | b -> a
  type ConnectorRecord a = b | b -> a

  partitioner :: Partitioner (ConnectorRecord a)
  encoder :: Encoder (ConnectorRecord a)
  connect
    :: a
    -> SimpleLogger
    -> ConnectorState a
    -> Producer (ConnectorRecord a)
    -> (STM (ConnectorState a) -> IO b)
    -> IO b

-- | May be thrown if an unsupported type is read from the database.
newtype UnsupportedType = UnsupportedType String
  deriving (Show)
  deriving anyclass (Exception)
