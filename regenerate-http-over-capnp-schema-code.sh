#! /bin/sh

# Regenerates capnp-http/src/http_over_capnp_capnp.rs (committed generated code).
#
# Unlike the other regenerate-*.sh scripts, this one cannot use the capnpc-rust
# CLI plugin, because the cross-crate `crate_provides` option (which makes the
# generated code reference the `capnp-byte-stream` crate rather than regenerating
# the ByteStream types) is only available via `capnpc::CompilerCommand`. So we run
# a small helper (an example) that calls that API.

set -e
set -x

cargo run -p capnp-http --example regenerate_schema
rustfmt capnp-http/src/http_over_capnp_capnp.rs
