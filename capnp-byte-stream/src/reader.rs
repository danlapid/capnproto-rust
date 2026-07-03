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

//! Adapter presenting a capnp `ByteStream` *server* as a [`futures::io::AsyncRead`]:
//! bytes written to the hosted stream become readable.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use capnp::Error;
use futures::io::AsyncRead;

use crate::byte_stream_capnp::byte_stream;

/// Soft limit on buffered, not-yet-read bytes before `write()` applies backpressure.
const READ_BUFFER_CAPACITY: usize = 1 << 16;

enum Status {
    Open,
    Ended,
    Aborted(Error),
}

struct Shared {
    /// Unread chunks, oldest first.
    chunks: VecDeque<Vec<u8>>,
    /// Bytes already consumed from the front chunk.
    front_offset: usize,
    /// Total unread bytes buffered across all chunks.
    buffered: usize,
    status: Status,
    consumer_gone: bool,
    read_waker: Option<Waker>,
    write_waker: Option<Waker>,
    /// If set, the stream is expected to carry exactly this many bytes: a peer
    /// that writes more, or calls `end()` before delivering them all, gets an
    /// error and the stream is aborted.
    expected_len: Option<u64>,
    /// Total bytes accepted so far.
    received: u64,
}

impl Shared {
    fn wake_reader(&mut self) {
        if let Some(w) = self.read_waker.take() {
            w.wake();
        }
    }
    fn wake_writer(&mut self) {
        if let Some(w) = self.write_waker.take() {
            w.wake();
        }
    }
}

/// The read end of a hosted capnp `ByteStream`. Implements [`AsyncRead`].
pub struct ByteStreamReader {
    shared: Rc<RefCell<Shared>>,
}

impl Drop for ByteStreamReader {
    fn drop(&mut self) {
        let mut s = self.shared.borrow_mut();
        s.consumer_gone = true;
        s.wake_writer();
    }
}

impl ByteStreamReader {
    /// Chunk-oriented alternative to [`AsyncRead`]: yields buffered bytes a chunk
    /// at a time, `None` at clean EOF. Chunk boundaries follow the peer's
    /// `write()` calls but are not guaranteed.
    pub fn poll_chunk(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Vec<u8>, Error>>> {
        let mut s = self.shared.borrow_mut();
        if let Some(mut chunk) = s.chunks.pop_front() {
            if s.front_offset > 0 {
                chunk.drain(..s.front_offset);
                s.front_offset = 0;
            }
            s.buffered -= chunk.len();
            s.wake_writer();
            return Poll::Ready(Some(Ok(chunk)));
        }
        match &s.status {
            Status::Open => {
                s.read_waker = Some(cx.waker().clone());
                Poll::Pending
            }
            Status::Ended => Poll::Ready(None),
            Status::Aborted(e) => Poll::Ready(Some(Err(e.clone()))),
        }
    }

    /// Returns true once `end()` has been received and all bytes consumed.
    pub fn is_ended(&self) -> bool {
        let s = self.shared.borrow();
        s.buffered == 0 && matches!(s.status, Status::Ended)
    }
}

impl AsyncRead for ByteStreamReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        if buf.is_empty() {
            // A zero-length read is always "successful" and must not be mistaken
            // for (or block on) stream state.
            return Poll::Ready(Ok(0));
        }
        let mut s = self.shared.borrow_mut();
        if s.buffered > 0 {
            let mut written = 0;
            while written < buf.len() {
                let front_len = match s.chunks.front() {
                    Some(c) => c.len(),
                    None => break,
                };
                let start = s.front_offset;
                let n = std::cmp::min(buf.len() - written, front_len - start);
                buf[written..written + n].copy_from_slice(&s.chunks[0][start..start + n]);
                written += n;
                s.front_offset += n;
                s.buffered -= n;
                if s.front_offset == front_len {
                    s.chunks.pop_front();
                    s.front_offset = 0;
                }
            }
            s.wake_writer();
            return Poll::Ready(Ok(written));
        }
        match &s.status {
            Status::Open => {
                s.read_waker = Some(cx.waker().clone());
                Poll::Pending
            }
            Status::Ended => Poll::Ready(Ok(0)),
            Status::Aborted(e) => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                format!("{e}"),
            ))),
        }
    }
}

struct ReaderServer {
    shared: Rc<RefCell<Shared>>,
    /// If set, `startTls` requests are forwarded here (carrying the expected
    /// server hostname); otherwise `startTls` returns "unimplemented".
    tls_tx: Option<futures::channel::mpsc::UnboundedSender<String>>,
}

impl Drop for ReaderServer {
    fn drop(&mut self) {
        let mut s = self.shared.borrow_mut();
        if matches!(s.status, Status::Open) {
            s.status = Status::Aborted(Error::disconnected(
                "ByteStream dropped without end() (truncated)".to_string(),
            ));
            s.wake_reader();
        }
    }
}

impl byte_stream::Server for ReaderServer {
    async fn write(self: Rc<Self>, params: byte_stream::WriteParams) -> Result<(), Error> {
        let chunk = params.get()?.get_bytes()?.to_vec();
        let mut chunk = Some(chunk);
        futures::future::poll_fn(move |cx| {
            let mut s = self.shared.borrow_mut();
            if s.consumer_gone {
                return Poll::Ready(Err(Error::disconnected(
                    "ByteStream reader was dropped".to_string(),
                )));
            }
            // Soft limit: a single oversized chunk is accepted whole rather than
            // split, so the buffer can exceed the capacity by up to one chunk.
            if s.buffered < READ_BUFFER_CAPACITY {
                if let Some(c) = chunk.take() {
                    let new_total = s.received.saturating_add(c.len() as u64);
                    if let Some(expected) = s.expected_len {
                        if new_total > expected {
                            let e = Error::failed(format!(
                                "ByteStream received more than its expected size \
                                 (expected {expected}, received at least {new_total})"
                            ));
                            s.status = Status::Aborted(e.clone());
                            s.wake_reader();
                            return Poll::Ready(Err(e));
                        }
                    }
                    s.received = new_total;
                    if !c.is_empty() {
                        s.buffered += c.len();
                        s.chunks.push_back(c);
                        s.wake_reader();
                    }
                }
                Poll::Ready(Ok(()))
            } else {
                s.write_waker = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }

    async fn end(
        self: Rc<Self>,
        _params: byte_stream::EndParams,
        _results: byte_stream::EndResults,
    ) -> Result<(), Error> {
        let mut s = self.shared.borrow_mut();
        if matches!(s.status, Status::Open) {
            if let Some(expected) = s.expected_len {
                if s.received < expected {
                    let e = Error::failed(format!(
                        "ByteStream ended before its expected size \
                         (expected {expected}, received {})",
                        s.received
                    ));
                    s.status = Status::Aborted(e.clone());
                    s.wake_reader();
                    return Err(e);
                }
            }
            s.status = Status::Ended;
            s.wake_reader();
        }
        Ok(())
    }

    async fn start_tls(self: Rc<Self>, params: byte_stream::StartTlsParams) -> Result<(), Error> {
        let hostname = params
            .get()?
            .get_expected_server_hostname()?
            .to_str()
            .map_err(|e| Error::failed(format!("startTls hostname is not valid UTF-8: {e}")))?
            .to_owned();
        match &self.tls_tx {
            Some(tx) => {
                let _ = tx.unbounded_send(hostname);
                Ok(())
            }
            None => Err(Error::unimplemented(
                "startTls is not supported on this stream".to_string(),
            )),
        }
    }
}

/// Creates a capnp `ByteStream` client and a [`ByteStreamReader`] fed by it.
///
/// Bytes written to the returned client become readable from the reader; `end()`
/// is a clean EOF; dropping the client without `end()` makes the reader fail with
/// a disconnect error (truncation).
pub fn byte_stream_reader() -> (byte_stream::Client, ByteStreamReader) {
    let (client, reader, _) = byte_stream_reader_inner(false, None);
    (client, reader)
}

/// Like [`byte_stream_reader`], but the stream is expected to carry exactly
/// `expected_len` bytes (when `Some`). A peer that writes more than that, or
/// calls `end()` before delivering them all, gets an error and the reader fails.
pub fn byte_stream_reader_with_expected_len(
    expected_len: Option<u64>,
) -> (byte_stream::Client, ByteStreamReader) {
    let (client, reader, _) = byte_stream_reader_inner(false, expected_len);
    (client, reader)
}

/// Like [`byte_stream_reader`], but also returns a receiver of `startTls`
/// requests (each carrying the expected server hostname). Used to support TLS
/// initiation over a CONNECT tunnel.
pub fn byte_stream_reader_with_tls() -> (
    byte_stream::Client,
    ByteStreamReader,
    futures::channel::mpsc::UnboundedReceiver<String>,
) {
    let (client, reader, tls) = byte_stream_reader_inner(true, None);
    (client, reader, tls.expect("tls receiver requested"))
}

fn byte_stream_reader_inner(
    with_tls: bool,
    expected_len: Option<u64>,
) -> (
    byte_stream::Client,
    ByteStreamReader,
    Option<futures::channel::mpsc::UnboundedReceiver<String>>,
) {
    let shared = Rc::new(RefCell::new(Shared {
        chunks: VecDeque::new(),
        front_offset: 0,
        buffered: 0,
        status: Status::Open,
        consumer_gone: false,
        read_waker: None,
        write_waker: None,
        expected_len,
        received: 0,
    }));
    let (tls_tx, tls_rx) = if with_tls {
        let (tx, rx) = futures::channel::mpsc::unbounded();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };
    let client = capnp_rpc::new_client(ReaderServer {
        shared: shared.clone(),
        tls_tx,
    });
    (client, ByteStreamReader { shared }, tls_rx)
}
