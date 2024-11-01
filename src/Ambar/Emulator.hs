module Ambar.Emulator where

import Ambar.Emulator.Config (EmulatorConfig(..), EnvironmentConfig(..))

emulate :: EmulatorConfig -> EnvironmentConfig -> IO ()
emulate _ _ = return ()
