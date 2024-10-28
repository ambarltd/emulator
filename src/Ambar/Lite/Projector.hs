module Ambar.Lite.Projector where

{-| A projector reads messages from multiple queues, applies a filter to the
stream and submits passing messages to a single data destination.

We do not implement filters for now.
-}
import Ambar.Lite.Queue (Queue)
import Ambar.Transport (Transport)
import Utils.Some (Some)

newtype Parallelism = Parallelism Int

project :: Queue -> Some Transport -> Parallelism -> IO ()
project = undefined

