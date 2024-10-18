module Utils.Some where

-- | An existential wrapper.
-- Allows us to say that we hold a value of any type that
-- implements a class.
-- This allows us to call the class methods on the content of
-- Some while removing the need to parameterise the container
-- of Some on the concrete type implementing the class.
data Some tag = forall a. tag a => Some a
