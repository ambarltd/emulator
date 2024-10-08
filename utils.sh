#! /bin/bash

function build {
  cabal build lite-lib -j "${@}"
}

function run {
  cabal run lite -- "${@}"
}

function test {
  cabal run lite-tests -- "${@}"
}

function bench {
  cabal run lite-bench -- "${@}"
}

# Save results of benchmarking
function bench-save {
  cabal build lite-bench
  cabal run lite-bench -- "${@}" | tee ./benchmarks/benchmarks.log
}

function typecheck {
  # Start fast type-checking of the library. (Everything but Main.hs)
  # Watches your files and type-checks on save
  ghcid -c 'cabal v2-repl' lite-lib "${@}"
}

function typecheck-executable {
  # Start fast type-checking of the executable. (Just Main.hs)
  # Watches your files and type-checks on save
  ghcid -c 'cabal v2-repl' lite "${@}"
}

function typecheck-tests {
  # Start fast type-checking of the executable. (Just Main.hs)
  # Watches your files and type-checks on save
  ghcid -c 'cabal v2-repl' lite-tests "${@}"
}

function typecheck-bench {
  # Start fast type-checking of the executable. (Just Main.hs)
  # Watches your files and type-checks on save
  ghcid -c 'cabal v2-repl' lite-bench "${@}"
}

# If the first argument is a function run it.
if [[ $(type -t $1) == function ]];
  then
   $1 "${@:2}";
  else
    echo "Development utilities"
    echo ""
    echo "  ./util.sh COMMAND"
    echo ""
fi
