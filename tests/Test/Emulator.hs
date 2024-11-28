{-# OPTIONS_GHC -Wno-x-partial #-}
module Test.Emulator
  ( testEmulator
  ) where

import Control.Exception (ErrorCall(..), throwIO)
import Control.Concurrent.STM (newTVarIO, modifyTVar, atomically, retry, readTVar, writeTVar)
import Control.Monad (forM, unless, void)
import qualified Data.Aeson as Json
import qualified Data.ByteString.Lazy as LB
import qualified Data.Map.Strict as Map
import Test.Hspec
  ( Spec
  , it
  , describe
  , shouldBe
  )
import System.IO.Temp (withSystemTempDirectory)

import Ambar.Emulator (emulate)
import Ambar.Emulator.Config
  ( EnvironmentConfig(..)
  , EmulatorConfig(..)
  , DataSource(..)
  , DataDestination(..)
  , Id(..)
  , Destination(..)
  , Source(..)
  )
import Ambar.Emulator.Projector (Message(..), Payload(..))

import Test.Connector.PostgreSQL (PostgresCreds, Event(..), mocks)
import qualified Test.Connector.PostgreSQL as C
import Test.Utils.OnDemand (OnDemand)
import qualified Test.Utils.OnDemand as OnDemand
import Utils.Async (withAsyncThrow)
import Utils.Delay (deadline, seconds)
import Utils.Logger (plainLogger, Severity(..))

testEmulator :: OnDemand PostgresCreds -> Spec
testEmulator p = describe "emulator" $ do
    it "retrieves data in a PostgreSQL db" $
      withConfig $ \config ->
      withPostgresSource $ \insert source -> do
      (out, dest) <- funDestination [source]
      let env = mkEnv [source] [dest]
          events = addIds $ take 10 $ head mocks
      insert events
      withAsyncThrow (emulate logger config env) $
        deadline (seconds 5) $ do
        consumed <- consume out (length events)
        consumed `shouldBe` events

    it "resumes from last index" $
      withConfig $ \config ->
      withPostgresSource $ \insert source -> do
      (out, dest) <- funDestination [source]
      let env = mkEnv [source] [dest]
          events = addIds $ take 10 $ head mocks
          (before, after) = splitAt 5 events

      -- insert and consume 'before'
      insert before
      withAsyncThrow (emulate logger config env) $
        void $ consume out (length before)

      -- clear destination
      atomically $ writeTVar out []

      insert after
      withAsyncThrow (emulate logger config env) $ do
        consumed <- consume out (length after)
        consumed `shouldBe` after
    where
    msgToEvent (Message _ _ _ _ (Payload r)) =
      case Json.fromJSON @Event r of
        Json.Error str -> Left str
        Json.Success e -> Right e

    logger = plainLogger Warn

    mkEnv sources dests = EnvironmentConfig
      { c_sources = Map.fromList [ (s_id source, source) | source <- sources ]
      , c_destinations = Map.fromList [ (d_id dest, dest) | dest <- dests ]
      }

    -- read from projected output
    consume out n =
      deadline (seconds 3) $ do
      xs <- atomically $ do
        xs <- readTVar out
        unless (length xs >= n) retry
        return xs
      let emsgs = forM xs $ \x -> do
            msg <- Json.eitherDecode @Message $ LB.fromStrict x
            msgToEvent msg

      case emsgs of
        Left err -> throwIO $ ErrorCall $ "Event decoding error: " <> err
        Right msgs -> return (reverse msgs)

    funDestination sources =  do
      out <- newTVarIO []
      let dest = DataDestination
            { d_id = Id "fun"
            , d_sources = sources
            , d_description = "Function destination"
            , d_destination = DestinationFun $ \e -> do
                atomically $ modifyTVar out (e:)
                return Nothing
            }
      return (out, dest)

    withConfig f =
      withSystemTempDirectory "emulator-XXXXX" $ \path ->
      f EmulatorConfig
        { c_partitionsPerTopic = 5
        , c_dataPath = path
        }

    withPostgresSource :: (([Event] -> IO ()) -> DataSource -> IO a) -> IO a
    withPostgresSource f =
      OnDemand.with p $ \creds ->
      C.withEventsTable creds $ \conn table -> do
      let config = C.mkPostgreSQL creds table
          source = DataSource
            { s_id = Id "postgres_source"
            , s_description = "PostgreSQL source"
            , s_source = SourcePostgreSQL config
            }
          insert events = C.insert conn table events
      f insert source

    -- Add ids to the events we chose to use
    addIds :: [Event] -> [Event]
    addIds es = [Event n a s | (n, Event _ a s) <- zip [1..] es]



