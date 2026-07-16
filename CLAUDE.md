# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

The Ambar event-bus **emulator**: a local stand-in for the hosted Ambar platform. It polls rows out of source databases (PostgreSQL / MySQL / SQL Server / plain files), stores them in a file-backed partitioned queue, and POSTs each row to registered HTTP destinations with per-destination cursors and infinite retry. Consuming apps (e.g. `sumori`, `virtual-agent`) run it via docker-compose as their local event bus.

Written in **Haskell** (GHC2021). No relation to the TypeScript app repos' conventions — this repo follows ordinary Haskell/cabal practice.

## Dev environment (macOS, arm64)

```bash
# Toolchain — match CI: GHC 9.10.1 (cabal 3.10+ works)
ghcup install ghc 9.10.1 && ghcup set ghc 9.10.1

# The vendored libs are a git submodule — nothing builds without this:
git submodule update --init

# Native libraries the Haskell bindings link against:
brew install libpq mysql-client openssl@3 zstd pcre
```

Homebrew's libpq/openssl are keg-only, and cabal's `--extra-*-dirs` CLI flags do **not** propagate to dependency configure steps — put the paths in the user-global cabal config (`~/.config/cabal/config`) instead:

```
extra-include-dirs: /opt/homebrew/opt/<pkg>/include
extra-lib-dirs: /opt/homebrew/opt/<pkg>/lib
```

for each of `libpq`, `mysql-client`, `openssl@3`, `zstd`, `pcre`. Also keep `/opt/homebrew/opt/libpq/bin` and `/opt/homebrew/opt/mysql-client/bin` on `PATH` (for `pg_config` / `mysql_config`).

## Commands

```bash
./utils.sh build          # cabal build lib:emulator
./utils.sh run            # run the emulator (needs --config FILE)
./utils.sh test           # cabal run emulator-tests  (see test infra below)
./utils.sh test -- --match "some description"   # subset by hspec description
./utils.sh typecheck      # ghcid fast-feedback loop on the library
./utils.sh bench          # benchmarks
./utils.sh build-docker   # release image (static Alpine binary, build/Dockerfile.static)
```

Tests expect live **PostgreSQL and MySQL** servers (CI provisions Postgres 16 + MySQL 9; see `.github/workflows/build.yml` for the exact setup). Config/queue/projector suites are pure and run without databases — use `--match` to scope. SQL Server needs no provisioning: the harness `docker run`s `mcr.microsoft.com/azure-sql-edge` itself (works on Apple Silicon).

### Running the full suite locally on macOS (Docker-backed DBs)

The harnesses shell out to `psql`/`mysql` and self-provision users/databases, with three macOS traps: the Haskell `mysql` library always dials the Unix socket `/tmp/mysql.sock` (host `localhost`), Unix sockets can't cross the macOS↔Docker-VM boundary, and MySQL 9's `caching_sha2_password` rejects cold-cache auth over a bridged socket (client treats a socket as a secure channel and sends cleartext; the server sees insecure TCP and refuses it). Verified recipe (286/286 pass, 2026-07-16):

```bash
# Throwaway servers — port 5432 must be free (stop any app stack using it first)
docker run -d --rm --name emu-test-pg -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 postgres:16
# mysql:8.0 + native password sidesteps the caching_sha2 cold-cache failure (8.0-only flag; removed in 9)
docker run -d --rm --name emu-test-mysql -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -p 3306:3306 \
  mysql:8.0 --default-authentication-plugin=mysql_native_password

# Bridge the socket the Haskell client insists on to the container's TCP port.
# backlog=511 matters: hspec runs specs in parallel and socat's default backlog of 5
# overflows into ECONNREFUSED mid-suite.
socat UNIX-LISTEN:/tmp/mysql.sock,fork,reuseaddr,backlog=511 TCP:127.0.0.1:3306 &

PATH="$HOME/.ghcup/bin:/opt/homebrew/opt/libpq/bin:/opt/homebrew/opt/mysql-client/bin:$PATH" \
  PGHOST=127.0.0.1 PGUSER=postgres cabal test emulator-tests --test-show-details=direct
```

## Architecture

```
source DB ──(polling SELECT, RepeatableRead)──▶ topic (file-backed, partitioned)
                                                    │  one topic per source, partitioned by partitioningColumn
                                                    ▼
                              one Projector per destination (consumer group = destination id)
                                    │ per-partition consumers, per-destination cursor
                                    │ optional per-destination filter (skip-and-commit)
                                    ▼
                              HTTP POST Message{data_source_id, …, payload: <row JSON>}
                              (infinite fibonacci-backoff retry, commit after success)
```

| Concern | Where |
|---|---|
| Orchestration (`emulate`) | `src/Ambar/Emulator.hs` |
| Config schema + YAML parsing | `src/Ambar/Emulator/Config.hs` |
| Delivery loop, filtering, envelope | `src/Ambar/Emulator/Projector.hs` |
| Polling connectors | `src/Ambar/Emulator/Connector/{Postgres,MySQL,MicrosoftSQLServer,Poll}.hs` |
| File-backed queue/topics | `src/Ambar/Emulator/Queue/…` |
| HTTP transport (auth, retry policy decode) | `src/Ambar/Transport/Http.hs` |
| Source cursors (`state.json`, saved every 30s) | `src/Ambar/Emulator.hs` |

Key semantics:

- **At-least-once, per-partition ordered** delivery; a destination's consumer commits only after a successful send (or an intentional filter skip).
- **Per-destination filter** (optional `filter: {column, values}` in config): non-matching records are skipped but committed. Records missing the column, or with a non-string value, are **delivered** (fail-open) — config mistakes must degrade to the pre-filter behaviour, never to silent data loss.
- The full source row (all configured `columns`) is the `payload`; consumers see it wrapped in the `Message` envelope.

## Consumers of this repo

- Apps reference the released image `docker.io/ambarltd/emulator:vX.Y` in their docker-compose (`event-bus` service) and mount their config at `/opt/emulator/config/config.yaml`.
- Releases: git tags (`v1.x`) + static Docker image via `./utils.sh build-docker`; CI (`.github/workflows/`) builds and tests on push.
- The consuming apps' config registers one destination per projection/reaction endpoint. Keep any destination `filter.values` lists in lockstep with what the consumer actually accepts — a stale filter silently starves the consumer (the apps should own a drift check on their side).

## Git

- Feature branches; never commit to `main` or push without explicit approval.
- `deps/haskell-libs` is a pinned submodule — don't update its ref as a side effect.
