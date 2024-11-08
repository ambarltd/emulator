#
# Build a statically-linked binary for Linux
#
FROM alpine:3.19

# Install system deps
RUN apk update
RUN apk add \
    autoconf \
    automake \
    bash \
    binutils \
    binutils-gold \
    build-base \
    coreutils \
    cpio \
    curl \
    g++  \
    gcc \
    git \
    gmp-dev \
    libc-dev \
    libffi-dev \
    libpq-dev \
    libxml2-dev \
    libxml2-static \
    linux-headers \
    lld \
    lm-sensors \
    lz4-static \
    make \
    musl-dev \
    ncurses-dev \
    ncurses-dev \
    ncurses-libs \
    ncurses-static \
    openssl \
    openssl-dev \
    openssl-libs-static \
    perl \
    pkgconfig \
    postgresql-dev \
    readline-dev \
    readline-static \
    tar \
    wget \
    xz \
    zlib-dev \
    zlib-static \
    zstd-static

# Install ghcup
RUN curl --proto '=https' --tlsv1.2 -sSf https://get-ghcup.haskell.org | \
    BOOTSTRAP_HASKELL_NONINTERACTIVE=1 \
    BOOTSTRAP_HASKELL_GHC_VERSION="9.10.1" \
    BOOTSTRAP_HASKELL_INSTALL_NO_STACK=1 \
    sh
ENV PATH="/root/.ghcup/bin:$PATH"
ENV PATH="$HOME/.cabal/bin:$PATH"

WORKDIR /opt/emulator

# Add only the build files and build only the dependencies. Docker will cache
# this layer, freeing us up to modify source code without re-building the
# dependencies (unless the .cabal file changes)
COPY ./emulator.cabal       /opt/emulator
COPY ./cabal.project        /opt/emulator

# Add and build application code.
# The 'cabal install' command will also add the 'emulator' binary to the PATH.
# For maximum caching we only copy source code files.
COPY ./src          /opt/emulator/src
COPY ./tests        /opt/emulator/tests
COPY ./benchmarks   /opt/emulator/benchmarks

RUN cabal build --dependencies-only

RUN cabal build \
    --enable-executable-static \
    --ghc-options='-optl-lpq -optl-lssl -optl-lcrypto -optl-lpgcommon -optl-lpgport -optl-lzstd -optl-llz4 -optl-lxml2 -optl-lssl -optl-lcrypto -optl-lz -optl-lreadline -optl-lm' \
    emulator

ENV PATH="/root/.local/bin:${PATH}"

CMD ["emulator"]
