#! /bin/sh

set -e
set -x

cargo build -p capnpc
capnp compile \
  -otarget/debug/capnpc-rust:capnp-byte-stream/src \
  capnp-byte-stream/schema/capnp/compat/byte-stream.capnp \
  --src-prefix capnp-byte-stream/schema/capnp/compat/ \
  -Icapnp-byte-stream/schema --no-standard-import
rustfmt capnp-byte-stream/src/byte_stream_capnp.rs
