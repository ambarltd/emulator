#! /bin/bash

function build {
  cabal build emulator-lib -j "${@}"
}

function run {
  cabal run emulator -- "${@}"
}

function test {
  cabal run emulator-tests -- "${@}"
}

function bench {
  cabal run emulator-bench -- "${@}"
}

function build-docker {
  docker build --file "./build/Dockerfile.aarch64.linux" -t emulator .
}

# Save results of benchmarking
function bench-save {
  cabal build emulator-bench
  cabal run emulator-bench -- "${@}" | tee ./benchmarks/benchmarks.log
}

function typecheck {
  # Start fast type-checking of the library. (Everything but Main.hs)
  # Watches your files and type-checks on save
  ghcid -c 'cabal v2-repl' emulator-lib "${@}"
}

function typecheck-executable {
  # Start fast type-checking of the executable. (Just Main.hs)
  # Watches your files and type-checks on save
  ghcid -c 'cabal v2-repl' emulator "${@}"
}

function typecheck-tests {
  # Start fast type-checking of the executable. (Just Main.hs)
  # Watches your files and type-checks on save
  ghcid -c 'cabal v2-repl' emulator-tests "${@}"
}

function typecheck-bench {
  # Start fast type-checking of the executable. (Just Main.hs)
  # Watches your files and type-checks on save
  ghcid -c 'cabal v2-repl' emulator-bench "${@}"
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
