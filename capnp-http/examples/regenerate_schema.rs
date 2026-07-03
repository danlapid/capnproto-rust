// Copyright (c) 2026 the capnproto-rust contributors
// Licensed under the MIT License:
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//
// Regenerates `src/http_over_capnp_capnp.rs` (committed generated code).
//
// This is a developer tool, not a usage example. It exists as an example (rather
// than a build script) because the generated code is committed: `capnp-http` does
// NOT regenerate at build time, so downstream builds need neither the `capnp`
// binary nor `capnpc`.
//
// It uses `capnpc::CompilerCommand` because the cross-crate `crate_provides`
// option (which makes the generated code reference `capnp_byte_stream` instead of
// regenerating the `ByteStream` types) is only available through that builder API,
// not the `capnpc-rust` CLI plugin.
//
// Run via `regenerate-http-over-capnp-schema-code.sh`, or directly:
//   cargo run -p capnp-http --example regenerate_schema

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // File id of byte-stream.capnp (capnp_byte_stream::BYTE_STREAM_CAPNP_FILE_ID).
    const BYTE_STREAM_CAPNP_FILE_ID: u64 = 0x8f5d_14e1_c273_738d;

    let manifest = env!("CARGO_MANIFEST_DIR");
    capnpc::CompilerCommand::new()
        .src_prefix(format!("{manifest}/schema/capnp/compat"))
        // This crate only vendors `http-over-capnp.capnp`; its imports
        // (`byte-stream.capnp`, `c++.capnp`, and the streaming `stream.capnp`)
        // are resolved from the canonical copies in the `capnp-byte-stream`
        // crate rather than duplicated here.
        .import_path(format!("{manifest}/schema"))
        .import_path(format!("{manifest}/../capnp-byte-stream/schema"))
        .no_standard_import()
        .crate_provides("capnp_byte_stream", [BYTE_STREAM_CAPNP_FILE_ID])
        .output_path(format!("{manifest}/src"))
        .file(format!(
            "{manifest}/schema/capnp/compat/http-over-capnp.capnp"
        ))
        .run()?;

    println!("regenerated {manifest}/src/http_over_capnp_capnp.rs");
    Ok(())
}
