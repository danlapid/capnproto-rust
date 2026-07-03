# capnp-byte-stream

Adapters between the Cap'n Proto
[`ByteStream`](https://github.com/capnproto/capnproto/blob/master/c%2B%2B/src/capnp/compat/byte-stream.capnp)
RPC protocol and asynchronous byte streams (`futures::io::AsyncRead` /
`AsyncWrite`). This is the Rust counterpart of the C++
`capnp/compat/byte-stream.{h,c++}` library, and the foundation that
[`capnp-http`](../capnp-http) builds on.

`byte_stream_to_async_write()` wraps a `ByteStream` client as an `AsyncWrite`;
`async_write_to_byte_stream()` exposes an `AsyncWrite` sink as a `ByteStream`
server; `byte_stream_reader()` hosts a `ByteStream` server whose incoming bytes
are read through an `AsyncRead`. Closing a writer sends `end()` (clean
end-of-stream), while dropping it without closing drops the capability without
`end()`, which the peer interprets as an abort (truncated stream). Backpressure
comes from the `write @0 (...) -> stream` flow control plus a bounded internal
buffer.

`getSubstream`-based path-shortening (a performance optimization) is not
implemented; `getSubstream` returns "unimplemented".
