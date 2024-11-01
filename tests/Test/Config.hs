{-# LANGUAGE QuasiQuotes #-}
module Test.Config (testConfig) where

import qualified Data.Map.Strict as Map
import Data.String.Interpolate (i)
import System.IO.Temp (withSystemTempFile)
import System.IO (hClose)
import Test.Hspec
  ( Spec
  , it
  , describe
  , shouldBe
  , shouldThrow
  , anyErrorCall
  )
import Test.Hspec.Expectations.Contrib (annotate)

import Ambar.Emulator.Config

testConfig :: Spec
testConfig = do
  describe "config" $ do
    it "parses full file" $ do
      config <- parseConfig [i|
        data_sources:
          - id: postgres_source
            description: Main events store
            type: postgres
            host: localhost
            port: 5432
            username: my_user
            password: my_pass
            database: my_db
            table: events_table
            columns:
              - id
              - aggregate_id
              - sequence_number
              - payload
            serialColumn: id
            partitioningColumn: aggregate_id

          - id: file_source
            description: The file source
            type: file
            path: ./source.txt

        data_destinations:
          - id: file_destination
            description: my projection 1
            type: file
            path: ./temp.file

            sources:
              - postgres_source
              - file_source

          - id: HTTP_destination
            description: my projection 2
            type: http-push
            endpoint: http://some.url.com:8080/my_projection
            username: name-of-user
            password: password123

            sources:
              - postgres source
              - file source
        |]
      annotate "source count" $ Map.size (c_sources  config) `shouldBe` 2
      annotate "source count" $ Map.size (c_destinations  config) `shouldBe` 2

    it "detects duplicate sources" $ do
      parseConfig [i|
        data_sources:
          - id: source_1
            description: The file source
            type: file
            path: ./source.txt

          - id: source_1
            description: The file source
            type: file
            path: ./source.txt

        data_destinations:
          - id: file_destination
            description: my projection 1
            type: file
            path: ./temp.file
            sources:
              - postgres_source
              - file_source
        |] `shouldThrow` anyErrorCall

    it "detects duplicate destinations" $ do
      parseConfig [i|
        data_sources:
          - id: source_1
            description: The file source
            type: file
            path: ./source.txt

        data_destinations:
          - id: dest_1
            description: my projection 1
            type: file
            path: ./temp.file
            sources:
              - postgres_source
              - file_source

          - id: dest_1
            description: my projection 1
            type: file
            path: ./temp.file
            sources:
              - postgres_source
              - file_source
        |] `shouldThrow` anyErrorCall

  where
  parseConfig str =
    withSystemTempFile "config-xxxx" $ \path handle -> do
    hClose handle
    writeFile path str
    parseEnvConfigFile path





