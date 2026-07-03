#!/bin/sh
# Builds the C++ http-over-capnp interop echo server against a local capnproto
# checkout. This is an opt-in tool for cross-implementation interop testing; it is
# NOT part of the normal `cargo test` run.
#
# Requires:
#   * a built capnproto C++ checkout (default: ../../../capnproto/c++ relative to
#     this script), i.e. `.libs/*.a` present and `./capnp` runnable.
#   * a C++23-capable compiler (Apple clang needs -std=c++2b -fsized-deallocation).
#
# Usage: interop/build.sh [CAPNP_CXX_DIR]
#   Produces: target/interop/echo_server

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
CAPNP_CXX="${1:-$SCRIPT_DIR/../../../capnproto/c++}"
OUT_DIR="$SCRIPT_DIR/../../target/interop"

if [ ! -f "$CAPNP_CXX/.libs/libcapnp-rpc.a" ]; then
  echo "error: capnproto C++ libs not found at $CAPNP_CXX/.libs" >&2
  echo "       point this script at a built capnproto/c++ checkout." >&2
  exit 1
fi

mkdir -p "$OUT_DIR"
SRC="$CAPNP_CXX/src"

# Regenerate the C++ generated code for the compat schemas (idempotent).
( cd "$CAPNP_CXX" && ./capnp compile -o./capnpc-c++:src -Isrc --src-prefix src \
    src/capnp/compat/byte-stream.capnp src/capnp/compat/http-over-capnp.capnp )

# Bare compiler flags that are deliberately word-split into separate arguments.
CXXFLAGS="-std=c++2b -fsized-deallocation -O1"

# Build the argument list with `set --` so include/library paths survive spaces
# in $CAPNP_CXX / $SRC, while the bare flags above still split into words.
# shellcheck disable=SC2086
set -- $CXXFLAGS -I"$SRC" -o "$OUT_DIR/echo_server" \
  "$SCRIPT_DIR/echo_server.c++" \
  "$SRC/capnp/compat/byte-stream.c++" \
  "$SRC/capnp/compat/byte-stream.capnp.c++" \
  "$SRC/capnp/compat/http-over-capnp.c++" \
  "$SRC/capnp/compat/http-over-capnp.capnp.c++" \
  "$CAPNP_CXX/.libs/libcapnp-rpc.a" \
  "$CAPNP_CXX/.libs/libcapnp.a" \
  "$CAPNP_CXX/.libs/libkj-http.a" \
  "$CAPNP_CXX/.libs/libkj-async.a" \
  "$CAPNP_CXX/.libs/libkj.a" \
  -lz

c++ "$@"

echo "built $OUT_DIR/echo_server"
