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

//! Adapters between the Cap'n Proto [`ByteStream`] RPC protocol and asynchronous
//! byte streams.
//!
//! This crate is the Rust counterpart of the C++ `capnp/compat/byte-stream.{h,c++}`
//! library. It provides conversions between a capnp [`ByteStream`] capability and
//! the [`futures::io::AsyncRead`] / [`futures::io::AsyncWrite`] traits, so that
//! HTTP bodies (and other byte streams) can be carried over Cap'n Proto RPC.
//!
//! It is the foundation that `capnp-http` (the `http-over-capnp` port) is built
//! on.
//!
//! [`ByteStream`]: crate::byte_stream_capnp::byte_stream::Client

/// Code generated from
/// [`byte-stream.capnp`](https://github.com/capnproto/capnproto/blob/master/c%2B%2B/src/capnp/compat/byte-stream.capnp).
pub mod byte_stream_capnp;

mod reader;
mod server;
mod writer;

pub use reader::{
    byte_stream_reader, byte_stream_reader_with_expected_len, byte_stream_reader_with_tls,
    ByteStreamReader,
};
pub use server::async_write_to_byte_stream;
pub use writer::{byte_stream_to_async_write, ByteStreamWriter};

/// The file id of `byte-stream.capnp`, for use with
/// `capnpc::CompilerCommand::crate_provides` in downstream schemas that
/// `import "/capnp/compat/byte-stream.capnp"`.
pub const BYTE_STREAM_CAPNP_FILE_ID: u64 = 0x8f5d_14e1_c273_738d;
