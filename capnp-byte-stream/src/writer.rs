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

//! Adapter presenting a capnp [`byte_stream::Client`] as a [`futures::io::AsyncWrite`].

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use capnp::capability::Promise;
use capnp::Error;
use futures::io::AsyncWrite;

use crate::byte_stream_capnp::byte_stream;

/// Maximum number of body bytes sent in a single `write()` RPC. Mirrors the C++
/// `MAX_BYTES_PER_WRITE` in `byte-stream.c++`.
pub(crate) const MAX_BYTES_PER_WRITE: usize = 1 << 16;

/// Wraps a capnp [`byte_stream::Client`] so that it can be written to through the
/// [`futures::io::AsyncWrite`] interface.
///
/// * Each [`poll_write`](AsyncWrite::poll_write) issues (at most) one `write()`
///   streaming RPC of up to `MAX_BYTES_PER_WRITE` bytes and resolves once the
///   RPC's flow-control promise completes, providing natural backpressure.
/// * [`poll_close`](AsyncWrite::poll_close) sends the `end()` RPC, signalling a
///   clean end-of-stream.
/// * Dropping the writer *without* calling `poll_close` drops the underlying
///   capability without sending `end()`, which the peer interprets as an
///   **abort** (a prematurely-terminated stream). This matches the semantics of
///   the C++ implementation.
pub struct ByteStreamWriter {
    client: byte_stream::Client,
    in_flight: Option<InFlight>,
    closed: bool,
}

enum InFlight {
    Write { len: usize, fut: Promise<(), Error> },
    Close(Promise<(), Error>),
}

pub fn byte_stream_to_async_write(client: byte_stream::Client) -> ByteStreamWriter {
    ByteStreamWriter {
        client,
        in_flight: None,
        closed: false,
    }
}

impl ByteStreamWriter {
    /// Consumes the writer and returns the underlying capnp client.
    ///
    /// Note: returning the client this way means no `end()` will be sent by the
    /// writer; the caller takes responsibility for the stream's termination.
    pub fn into_client(self) -> byte_stream::Client {
        self.client
    }

    /// Sends a `startTls` request on this stream, signalling the peer to begin a
    /// TLS handshake expecting `expected_server_hostname`. The stream is *not*
    /// terminated; keep using it after this resolves.
    ///
    /// Call this only when no `write` is in flight (i.e. after the previous write
    /// has completed), so that ordering is well-defined.
    pub async fn start_tls(&mut self, expected_server_hostname: &str) -> Result<(), Error> {
        let mut req = self.client.start_tls_request();
        req.get()
            .set_expected_server_hostname(expected_server_hostname);
        req.send().await
    }
}

fn capnp_err_to_io(e: &Error) -> std::io::Error {
    let kind = match e.kind {
        capnp::ErrorKind::Disconnected => std::io::ErrorKind::ConnectionReset,
        _ => std::io::ErrorKind::Other,
    };
    std::io::Error::new(kind, format!("{e}"))
}

impl AsyncWrite for ByteStreamWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        if this.closed {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "write after end() on ByteStream",
            )));
        }
        loop {
            match &mut this.in_flight {
                Some(InFlight::Close(_)) => {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "write while ByteStream is closing",
                    )));
                }
                Some(InFlight::Write { len, fut }) => match Pin::new(fut).poll(cx) {
                    Poll::Ready(Ok(())) => {
                        let n = *len;
                        this.in_flight = None;
                        return Poll::Ready(Ok(n));
                    }
                    Poll::Ready(Err(e)) => {
                        let io = capnp_err_to_io(&e);
                        this.in_flight = None;
                        return Poll::Ready(Err(io));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                None => {
                    if buf.is_empty() {
                        return Poll::Ready(Ok(0));
                    }
                    let chunk = core::cmp::min(buf.len(), MAX_BYTES_PER_WRITE);
                    let mut req = this.client.write_request();
                    req.get().set_bytes(&buf[..chunk]);
                    this.in_flight = Some(InFlight::Write {
                        len: chunk,
                        fut: req.send(),
                    });
                }
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        // There is no separate flush RPC: a `write()` is "flushed" once its
        // promise resolves. So flushing just means driving any in-flight write to
        // completion. A pending close that *succeeds* here also marks the writer
        // closed, so a later `poll_write` is rejected and a later `poll_close`
        // doesn't send a second `end()`. (On a close *error* the writer stays
        // un-closed, so a `poll_close` retry re-attempts the `end()`, the same
        // retry semantics as `poll_close` itself.)
        let this = self.get_mut();
        match &mut this.in_flight {
            None => Poll::Ready(Ok(())),
            Some(in_flight) => {
                let was_close = matches!(in_flight, InFlight::Close(_));
                let fut = match in_flight {
                    InFlight::Write { fut, .. } | InFlight::Close(fut) => fut,
                };
                match Pin::new(fut).poll(cx) {
                    Poll::Ready(Ok(())) => {
                        this.in_flight = None;
                        if was_close {
                            this.closed = true;
                        }
                        Poll::Ready(Ok(()))
                    }
                    Poll::Ready(Err(e)) => {
                        let io = capnp_err_to_io(&e);
                        this.in_flight = None;
                        Poll::Ready(Err(io))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if this.closed {
            return Poll::Ready(Ok(()));
        }
        loop {
            match &mut this.in_flight {
                // Finish any pending write before sending end(), to preserve order.
                Some(InFlight::Write { fut, .. }) => match Pin::new(fut).poll(cx) {
                    Poll::Ready(Ok(())) => {
                        this.in_flight = None;
                    }
                    Poll::Ready(Err(e)) => {
                        let io = capnp_err_to_io(&e);
                        this.in_flight = None;
                        return Poll::Ready(Err(io));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                Some(InFlight::Close(fut)) => match Pin::new(fut).poll(cx) {
                    Poll::Ready(Ok(())) => {
                        this.in_flight = None;
                        this.closed = true;
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Err(e)) => {
                        // Leave `closed` unset so the error is observed once and
                        // a retry re-attempts the close rather than silently
                        // returning `Ok(())` via the early-return above.
                        let io = capnp_err_to_io(&e);
                        this.in_flight = None;
                        return Poll::Ready(Err(io));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                None => {
                    let request = this.client.end_request();
                    let remote = request.send();
                    let fut = Promise::from_future(async move { remote.promise.await.map(|_| ()) });
                    this.in_flight = Some(InFlight::Close(fut));
                }
            }
        }
    }
}
