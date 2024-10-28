module Lite.Projector where

{-| A projector reads messages from multiple queues, applies a filter to the
stream and submits passing messages to a single data destination.

We do not implement filters for now.
-}

project :: IO ()
project = return ()

