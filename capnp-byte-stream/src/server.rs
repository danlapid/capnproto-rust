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

//! Adapter presenting a [`futures::io::AsyncWrite`] sink as a capnp
//! [`byte_stream::Client`].

use std::cell::RefCell;
use std::rc::Rc;

use capnp::Error;
use futures::io::{AsyncWrite, AsyncWriteExt};

use crate::byte_stream_capnp::byte_stream;

/// Wraps an [`AsyncWrite`] sink as a capnp [`byte_stream::Client`].
///
/// Incoming `write()` RPCs append to the sink; an `end()` RPC closes the sink
/// cleanly ([`AsyncWriteExt::close`]); dropping the capability *without* an
/// `end()` drops the sink without closing it, signalling an **abort**.
///
/// This always services writes through the RPC layer (no path-shortening); a
/// cap-level shortcut via a shared `CapabilityServerSet` could be layered on
/// later.
pub fn async_write_to_byte_stream<W>(sink: W) -> byte_stream::Client
where
    W: AsyncWrite + Unpin + 'static,
{
    capnp_rpc::new_client(ByteStreamServer {
        state: RefCell::new(ServerState::Active(sink)),
    })
}

struct ByteStreamServer<W> {
    state: RefCell<ServerState<W>>,
}

enum ServerState<W> {
    Active(W),
    /// A `write()` is currently borrowing the sink (await in progress).
    Busy,
    Ended,
    Aborted,
}

impl<W> byte_stream::Server for ByteStreamServer<W>
where
    W: AsyncWrite + Unpin + 'static,
{
    async fn write(self: Rc<Self>, params: byte_stream::WriteParams) -> Result<(), Error> {
        let reader = params.get()?;
        let bytes = reader.get_bytes()?;

        // Take the sink out of the cell for the duration of the (awaited) write.
        // Streaming-call ordering (capnp-rpc local.rs / wire flow control) delivers
        // writes one at a time, so `Busy` is not observable here.
        let mut sink = {
            let mut st = self.state.borrow_mut();
            match std::mem::replace(&mut *st, ServerState::Busy) {
                ServerState::Active(w) => w,
                ServerState::Busy => {
                    *st = ServerState::Busy;
                    return Err(Error::failed(
                        "concurrent write() on a ByteStream".to_string(),
                    ));
                }
                ServerState::Ended => {
                    *st = ServerState::Ended;
                    return Err(Error::failed(
                        "write() after end() on a ByteStream".to_string(),
                    ));
                }
                ServerState::Aborted => {
                    *st = ServerState::Aborted;
                    return Err(Error::disconnected("ByteStream was aborted".to_string()));
                }
            }
        };

        let result = sink.write_all(bytes).await;

        let mut st = self.state.borrow_mut();
        match result {
            Ok(()) => {
                if matches!(*st, ServerState::Busy) {
                    *st = ServerState::Active(sink);
                }
                Ok(())
            }
            Err(e) => {
                *st = ServerState::Aborted;
                Err(Error::failed(format!("ByteStream sink write failed: {e}")))
            }
        }
    }

    async fn end(
        self: Rc<Self>,
        _params: byte_stream::EndParams,
        _results: byte_stream::EndResults,
    ) -> Result<(), Error> {
        let mut sink = {
            let mut st = self.state.borrow_mut();
            match std::mem::replace(&mut *st, ServerState::Ended) {
                ServerState::Active(w) => w,
                ServerState::Busy => {
                    *st = ServerState::Busy;
                    return Err(Error::failed(
                        "end() called during a concurrent write() on a ByteStream".to_string(),
                    ));
                }
                ServerState::Ended => return Ok(()),
                ServerState::Aborted => {
                    *st = ServerState::Aborted;
                    return Err(Error::disconnected("ByteStream was aborted".to_string()));
                }
            }
        };

        sink.close()
            .await
            .map_err(|e| Error::failed(format!("ByteStream sink close failed: {e}")))
    }

    // `get_substream` and `start_tls` use the generated default implementations,
    // which return "unimplemented". `get_substream` is only called by a peer
    // performing path-shortening on a stream that resolves to a server in its own
    // process, so a remote peer never calls it. `start_tls` on the write side is
    // unused (CONNECT TLS initiation targets the read side; see
    // `byte_stream_reader_with_tls`).
}
