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

//! HTTP body bridging between capnp `ByteStream` and [`http_body::Body`].
//!
//! Two directions:
//!
//! * [`IncomingBody`]: a hosted `ByteStream` server exposed as an
//!   [`http_body::Body`]. Used for the request body on the server side and the
//!   response body on the client side.
//! * [`pump_body_to_byte_stream`]: drains an [`http_body::Body`] into a
//!   `ByteStream` client. Used for the request body on the client side and the
//!   response body on the server side.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use capnp::Error;
use http_body::{Body, Frame};

use capnp_byte_stream::byte_stream_capnp::byte_stream;
use capnp_byte_stream::{
    byte_stream_reader_with_expected_len, byte_stream_to_async_write, ByteStreamReader,
};

/// An [`http_body::Body`] fed by an associated capnp `ByteStream` server.
pub struct IncomingBody {
    reader: Option<ByteStreamReader>,
    /// The number of body bytes still expected, when the peer declared a fixed
    /// `bodySize`. This lets [`Body::size_hint`] report an exact length so the
    /// HTTP layer frames the message with `Content-Length` instead of chunked
    /// encoding. `None` means the length is unknown (chunked).
    remaining: Option<u64>,
    /// An opaque value kept alive for as long as the body exists. Used by the
    /// client adapter to attach the background request driver, so that dropping
    /// the response (and thus its body) cancels the in-flight `request()`.
    keepalive: Option<Box<dyn core::any::Any>>,
}

impl IncomingBody {
    /// An already-complete, empty body (no associated `ByteStream`).
    pub fn empty() -> Self {
        Self {
            reader: None,
            remaining: Some(0),
            keepalive: None,
        }
    }

    pub(crate) fn attach_keepalive(&mut self, keepalive: Box<dyn core::any::Any>) {
        self.keepalive = Some(keepalive);
    }
}

impl Body for IncomingBody {
    type Data = Bytes;
    type Error = Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Error>>> {
        let this = self.get_mut();
        let Some(reader) = &mut this.reader else {
            return Poll::Ready(None);
        };
        match reader.poll_chunk(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                if let Some(remaining) = this.remaining.as_mut() {
                    *remaining = remaining.saturating_sub(chunk.len() as u64);
                }
                Poll::Ready(Some(Ok(Frame::data(Bytes::from(chunk)))))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.reader {
            None => true,
            Some(reader) => reader.is_ended(),
        }
    }

    fn size_hint(&self) -> http_body::SizeHint {
        match self.remaining {
            Some(n) => http_body::SizeHint::with_exact(n),
            None => http_body::SizeHint::default(),
        }
    }
}

/// Creates a `ByteStream` client and an [`IncomingBody`] fed by it.
///
/// `len` is the declared fixed body length, if any; it lets the resulting
/// [`IncomingBody`] report an exact [`Body::size_hint`] (enabling
/// `Content-Length` framing) and is therefore *enforced*: a producer that
/// writes more than `len` bytes, or `end()`s before delivering all of them,
/// gets an error and the body is aborted, since a truncation/overrun must not
/// masquerade as a clean EOF once the exact size has been promised to the peer.
/// Pass `None` for an unknown-length (chunked) body.
pub(crate) fn incoming_body_with_len(len: Option<u64>) -> (byte_stream::Client, IncomingBody) {
    let (client, reader) = byte_stream_reader_with_expected_len(len);
    (
        client,
        IncomingBody {
            reader: Some(reader),
            remaining: len,
            keepalive: None,
        },
    )
}

/// The result of pumping an [`http_body::Body`] into a capnp `ByteStream`.
///
/// The two failure modes must be handled differently by callers, which is why
/// this is not a plain `Result`:
///
/// * [`SinkClosed`](PumpOutcome::SinkClosed) means the *peer* ended the
///   transfer: it stopped reading (e.g. a server that responds without
///   consuming the request body), went away, or rejected a `write()`/`end()`
///   (e.g. because the bytes violated the declared fixed body size; the
///   receiver already holds that error). The local body is still fully drained,
///   and the ongoing RPC call is unaffected: the exchange can complete
///   normally.
/// * [`SourceFailed`](PumpOutcome::SourceFailed) means the *local* body yielded an
///   error (a truncated/failed source). The byte stream cannot be ended cleanly;
///   the surrounding RPC call must be failed/cancelled so the truncation is
///   observed by the peer instead of deadlocking it. (A capnp `ByteStream` has no
///   "abort" message; `end()` is the only in-band terminator, so a mid-call
///   truncation can only be signalled by tearing down the call itself.)
pub enum PumpOutcome {
    /// The body completed and `end()` was sent: a clean EOF.
    Ended,
    /// The peer stopped reading; the body was drained but not ended.
    SinkClosed,
    /// The local body produced an error before completing.
    SourceFailed(Error),
}

/// Drains an [`http_body::Body`] into a capnp `ByteStream` client, ending the
/// stream cleanly when the body completes.
///
/// Trailers (if any) are dropped: the capnp `ByteStream` protocol has no trailer
/// representation. See [`PumpOutcome`] for how the (two) failure modes differ.
pub async fn pump_body_to_byte_stream<B>(body: B, client: byte_stream::Client) -> PumpOutcome
where
    B: Body,
    B::Data: Buf,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    use futures::io::AsyncWriteExt;

    let mut writer = byte_stream_to_async_write(client);
    let mut body = std::pin::pin!(body);
    // If a write fails (the peer stopped reading the body, e.g. a server that
    // responds without consuming the request), we must still drain the remaining
    // frames from the *local* body. Otherwise the surrounding HTTP/1 connection is
    // left with an unread body, which forces the HTTP server to abort the socket
    // (often an RST) and corrupts the in-flight response. So we record that the sink closed,
    // keep pulling frames to EOF (discarding them), and report `SinkClosed`.
    let mut sink_closed = false;
    while let Some(frame) = std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await {
        let frame = match frame {
            Ok(f) => f,
            // The local body failed: this is a truncated source, not a clean EOF.
            Err(e) => {
                return PumpOutcome::SourceFailed(Error::failed(format!(
                    "HTTP body error: {}",
                    e.into()
                )))
            }
        };
        if sink_closed {
            // Already disconnected from the sink: drain and discard.
            continue;
        }
        if let Ok(mut data) = frame.into_data() {
            while data.has_remaining() {
                let chunk = data.chunk();
                match writer.write_all(chunk).await {
                    Ok(()) => {
                        let n = chunk.len();
                        data.advance(n);
                    }
                    Err(_) => {
                        sink_closed = true;
                        break;
                    }
                }
            }
        }
        // Non-data frames (trailers) are ignored.
    }
    if sink_closed {
        return PumpOutcome::SinkClosed;
    }
    match writer.close().await {
        Ok(()) => PumpOutcome::Ended,
        // The peer went away just as we tried to end the stream, or rejected the
        // end() (e.g. fewer bytes than the declared fixed size; the receiver
        // already holds that error, so there is nothing further to report here).
        Err(_) => PumpOutcome::SinkClosed,
    }
}
