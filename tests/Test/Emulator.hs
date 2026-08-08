{-# OPTIONS_GHC -Wno-x-partial #-}
module Test.Emulator
  ( testEmulator
  ) where

import Control.Exception (ErrorCall(..), throwIO)
import Control.Concurrent.STM (newTVarIO, modifyTVar, atomically, retry, readTVar, writeTVar)
import Control.Monad (forM, unless, void)
import qualified Data.Aeson as Json
import qualified Data.Aeson.KeyMap as KeyMap
import qualified Data.ByteString.Lazy as LB
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import Data.Text (Text)
import System.IO (hClose)
import Test.Hspec
  ( Spec
  , it
  , describe
  , shouldBe
  )
import System.IO.Temp (withSystemTempDirectory, withSystemTempFile)

import Ambar.Emulator (emulate)
import Ambar.Emulator.Config
  ( EnvironmentConfig(..)
  , EmulatorConfig(..)
  , DataSource(..)
  , DataDestination(..)
  , DestinationFilter(..)
  , Id(..)
  , Destination(..)
  , Source(..)
  )
import Ambar.Emulator.Projector (Message(..), Payload(..))
import Ambar.Emulator.Connector.Postgres (PostgreSQL)

import Test.Connector.PostgreSQL (PostgresCreds, Event(..), mocks)
import qualified Test.Connector.PostgreSQL as C
import Test.Util.SQL (EventsTable)
import Util.OnDemand (OnDemand)
import qualified Util.OnDemand as OnDemand
import Util.Async (withAsyncThrow)
import Util.Delay (deadline, seconds)
import Util.Logger (plainLogger, Severity(..))

testEmulator :: OnDemand PostgresCreds -> Spec
testEmulator p = describe "emulator" $ do
    it "retrieves data in a PostgreSQL db" $
      withConfig $ \config ->
      withPostgresSource $ \table insert source -> do
      (out, dest) <- funDestination [source]
      let env = mkEnv [source] [dest]
          events = addIds $ take 10 $ head (mocks table)
      insert events
      withAsyncThrow (emulate logger config env) $
        deadline (seconds 5) $ do
        consumed <- consume out (length events)
        consumed `shouldBe` events

    it "applies destination filters and continues past skipped records" $
      withConfig $ \config ->
      withSystemTempFile "file-source-XXXXX" $ \path h -> do
      hClose h
      let source = DataSource
            { s_id = Id "file_src"
            , s_description = "file source"
            , s_source = SourceFile
                { sf_path = path
                , sf_partitioningField = "aggregate_id"
                , sf_incrementingField = "id"
                }
            }
          -- same aggregate_id → one partition → a filtered record that
          -- failed to commit would block every record behind it.
          entry :: Int -> Text -> Json.Value
          entry n name = Json.object
            [ "id" Json..= n, "aggregate_id" Json..= (1 :: Int), "event_name" Json..= name ]
          rows = [ entry 1 "Skipped", entry 2 "Wanted", entry 3 "Skipped", entry 4 "Wanted" ]
      LB.writeFile path $ LB.intercalate "\n" (map Json.encode rows) <> "\n"
      out <- newTVarIO []
      let dest = DataDestination
            { d_id = Id "filtered_fun"
            , d_sources = [source]
            , d_description = "filtered destination"
            , d_destination = DestinationFun $ \e -> do
                atomically $ modifyTVar out (e:)
                return Nothing
            , d_filter = Just DestinationFilter
                { f_column = "event_name"
                , f_values = Set.fromList ["Wanted"]
                }
            }
          env = mkEnv [source] [dest]
      withAsyncThrow (emulate logger config env) $
        deadline (seconds 5) $ do
        xs <- atomically $ do
          xs <- readTVar out
          unless (length xs >= 2) retry
          return xs
        names <- forM (reverse xs) $ \x ->
          case Json.eitherDecode @Message (LB.fromStrict x) of
            Left err -> throwIO $ ErrorCall $ "Message decoding error: " <> err
            Right (Message _ _ _ _ (Payload v)) -> return (eventName v)
        names `shouldBe` [Just "Wanted", Just "Wanted"]

    it "resumes from last index" $
      withConfig $ \config ->
      withPostgresSource $ \table insert source -> do
      (out, dest) <- funDestination [source]
      let env = mkEnv [source] [dest]
          events = addIds $ take 10 $ head (mocks table)
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

    eventName v = case v of
      Json.Object o -> case KeyMap.lookup "event_name" o of
        Just (Json.String name) -> Just name
        _ -> Nothing
      _ -> Nothing

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
            , d_filter = Nothing
            }
      return (out, dest)

    withConfig f =
      withSystemTempDirectory "emulator-XXXXX" $ \path ->
      f EmulatorConfig
        { c_partitionsPerTopic = 5
        , c_dataPath = path
        , c_port = Nothing
        }

    withPostgresSource ::
      (    EventsTable PostgreSQL
        -> ([Event] -> IO ())
        -> DataSource
        -> IO a )
      -> IO a
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
      f table insert source

    -- Add ids to the events we chose to use
    addIds :: [Event] -> [Event]
    addIds es = [Event n a s | (n, Event _ a s) <- zip [1..] es]



