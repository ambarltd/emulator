module Ambar.Emulator.Connector.MySQL
  ( MySQL(..)
  , MySQLState
  , MySQLRow
  ) where

import Control.Concurrent.STM (STM)
import qualified Data.Aeson as Aeson
import qualified Data.ByteString.Lazy as LB

import qualified Ambar.Emulator.Connector as C
import Ambar.Emulator.Queue.Topic (Producer, hashPartitioner)
import Ambar.Record (Record(..), Value(..))
import qualified Ambar.Record.Encoding as Encoding

import Utils.Logger (SimpleLogger)

data MySQL = MySQL

data MySQLState = MySQLState

newtype MySQLRow = MySQLRow { unMySQLRow :: Record }

instance C.Connector MySQL where
  type ConnectorState MySQL = MySQLState
  type ConnectorRecord MySQL = MySQLRow

  partitioner = hashPartitioner partitioningValue

  -- | A rows gets saved in the database as a JSON object with
  -- the columns specified in the config file as keys.
  encoder = LB.toStrict . Aeson.encode . Encoding.encode @Aeson.Value . unMySQLRow

  connect = connect

partitioningValue :: MySQLRow -> Value
partitioningValue (MySQLRow (Record row)) = snd $ row !! 1

connect
  :: MySQL
  -> SimpleLogger
  -> MySQLState
  -> Producer MySQLRow
  -> (STM MySQLState -> IO a)
  -> IO a
connect _ _ MySQLState _ _ = undefined
