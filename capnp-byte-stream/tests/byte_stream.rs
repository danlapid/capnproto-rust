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

//! Tier-0 (no path-shortening) tests for the `byte-stream` adapters. These wire
//! `async_write_to_byte_stream` directly to `byte_stream_to_async_write` through a
//! local capnp client, exercising both adapter directions without a full
//! two-party RPC connection.

use std::cell::{Cell, RefCell};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use capnp_byte_stream::byte_stream_capnp::byte_stream;
use capnp_byte_stream::{
    async_write_to_byte_stream, byte_stream_reader, byte_stream_to_async_write,
};
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use futures::task::LocalSpawnExt;
use futures::FutureExt;

/// Round-trip arbitrary data through the two adapters over a bounded in-memory
/// pipe (`async-byte-channel`). The bounded pipe forces real backpressure: the
/// writer cannot get ahead of the reader by more than the pipe's buffer.
fn round_trip(data: Vec<u8>) {
    let (sender, mut receiver) = async_byte_channel::channel();
    let client = async_write_to_byte_stream(sender);
    let mut writer = byte_stream_to_async_write(client);

    let mut pool = futures::executor::LocalPool::new();
    use futures::task::LocalSpawnExt;

    let data2 = data.clone();
    pool.spawner()
        .spawn_local(async move {
            writer.write_all(&data2).await.unwrap();
            writer.close().await.unwrap();
        })
        .unwrap();

    let mut got = vec![];
    pool.run_until(receiver.read_to_end(&mut got)).unwrap();

    assert_eq!(data.len(), got.len(), "length mismatch");
    assert_eq!(data, got, "content mismatch");
}

#[test]
fn write_round_trips() {
    round_trip(vec![]);
    round_trip(b"hello, byte stream".to_vec());
    // Exactly one chunk, then several times MAX_BYTES_PER_WRITE to exercise
    // chunk splitting in the writer.
    round_trip((0..=255u8).cycle().take(1 << 16).collect());
    round_trip((0..=255u8).cycle().take((1 << 16) * 3 + 1234).collect());
}

/// Round-trips data through the writer adapter into a `ByteStreamReader`
/// (`AsyncRead`), exercising both the send and receive byte-stream adapters.
fn reader_round_trip(data: Vec<u8>) {
    let (client, mut reader) = byte_stream_reader();
    let mut writer = byte_stream_to_async_write(client);

    let mut pool = futures::executor::LocalPool::new();
    use futures::task::LocalSpawnExt;

    let data2 = data.clone();
    pool.spawner()
        .spawn_local(async move {
            writer.write_all(&data2).await.unwrap();
            writer.close().await.unwrap();
        })
        .unwrap();

    let mut got = vec![];
    pool.run_until(reader.read_to_end(&mut got)).unwrap();
    assert_eq!(data, got);
}

#[test]
fn reader_round_trips() {
    reader_round_trip(b"hello reader".to_vec());
    reader_round_trip((0..=255u8).cycle().take((1 << 16) * 3 + 99).collect());
}

// A sink that records the bytes written and whether it was *cleanly closed*
// (poll_close) versus merely dropped (abort). Used to verify EOF-vs-abort.
#[derive(Clone)]
struct RecordingSink {
    data: Rc<RefCell<Vec<u8>>>,
    closed: Rc<Cell<bool>>,
}

impl RecordingSink {
    fn new() -> Self {
        Self {
            data: Rc::new(RefCell::new(vec![])),
            closed: Rc::new(Cell::new(false)),
        }
    }
}

impl AsyncWrite for RecordingSink {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.data.borrow_mut().extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.closed.set(true);
        Poll::Ready(Ok(()))
    }
}

#[test]
fn clean_end_calls_close() {
    let sink = RecordingSink::new();
    let data = sink.data.clone();
    let closed = sink.closed.clone();

    let client = async_write_to_byte_stream(sink);
    let mut writer = byte_stream_to_async_write(client);

    futures::executor::block_on(async move {
        writer.write_all(b"payload").await.unwrap();
        writer.close().await.unwrap();
    });

    assert_eq!(&data.borrow()[..], b"payload");
    assert!(
        closed.get(),
        "clean close() must propagate to end()/sink.close()"
    );
}

/// End-to-end through a real two-party `RpcSystem` (in-memory transport): the
/// server's bootstrap is a `ByteStream` backed by a sink; the client bootstraps
/// it, wraps it as an `AsyncWrite`, and streams a large payload. This exercises
/// the wire path including streaming flow control.
#[test]
fn wire_round_trip() {
    let mut pool = futures::executor::LocalPool::new();
    let spawner = pool.spawner();

    // RPC transport (two one-way channels forming a duplex).
    let (client_writer, server_reader) = async_byte_channel::channel();
    let (server_writer, client_reader) = async_byte_channel::channel();

    // The actual data pipe that the server's ByteStream writes into.
    let (data_sink, mut data_receiver) = async_byte_channel::channel();

    let client_network = Box::new(twoparty::VatNetwork::new(
        client_reader,
        client_writer,
        rpc_twoparty_capnp::Side::Client,
        Default::default(),
    ));
    let mut client_rpc = RpcSystem::new(client_network, None);
    let byte_stream: byte_stream::Client = client_rpc.bootstrap(rpc_twoparty_capnp::Side::Server);

    let server_bootstrap = async_write_to_byte_stream(data_sink);
    let server_network = Box::new(twoparty::VatNetwork::new(
        server_reader,
        server_writer,
        rpc_twoparty_capnp::Side::Server,
        Default::default(),
    ));
    let server_rpc = RpcSystem::new(server_network, Some(server_bootstrap.client));

    // Drive both RPC systems; ignore disconnect at teardown.
    spawner.spawn_local(client_rpc.map(|_| ())).unwrap();
    spawner.spawn_local(server_rpc.map(|_| ())).unwrap();

    let data: Vec<u8> = (0..=255u8).cycle().take(100_000).collect();
    let data2 = data.clone();
    spawner
        .spawn_local(async move {
            let mut writer = byte_stream_to_async_write(byte_stream);
            writer.write_all(&data2).await.unwrap();
            writer.close().await.unwrap();
        })
        .unwrap();

    let mut got = vec![];
    pool.run_until(data_receiver.read_to_end(&mut got)).unwrap();
    assert_eq!(data.len(), got.len());
    assert_eq!(data, got);
}

// A sink whose `poll_close` is Pending on the first call, so a test can observe
// a ByteStreamWriter with an in-flight (not yet resolved) end().
struct SlowCloseSink {
    closes: Rc<Cell<u32>>,
    close_polls: Cell<u32>,
}

impl AsyncWrite for SlowCloseSink {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let polls = self.close_polls.get() + 1;
        self.close_polls.set(polls);
        if polls == 1 {
            // Wake immediately so the next poll (from whatever future drives us)
            // is not lost, then report Pending once.
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.closes.set(self.closes.get() + 1);
        Poll::Ready(Ok(()))
    }
}

// When `poll_flush` is the call that completes an in-flight `end()`, the writer
// must be marked closed: a later write must fail locally and a later `close()`
// must not send a second `end()`.
#[test]
fn flush_completing_a_pending_close_marks_writer_closed() {
    let closes = Rc::new(Cell::new(0u32));
    let sink = SlowCloseSink {
        closes: closes.clone(),
        close_polls: Cell::new(0),
    };
    let client = async_write_to_byte_stream(sink);
    let mut writer = byte_stream_to_async_write(client);

    futures::executor::block_on(async move {
        writer.write_all(b"payload").await.unwrap();

        // Start close(); the sink's first poll_close is Pending, so this leaves
        // the end() in flight. Drop the future without completing it.
        {
            let mut close_fut = writer.close();
            assert!(
                futures::poll!(&mut close_fut).is_pending(),
                "first close poll should be pending (sink stalls once)"
            );
        }

        // flush() drives the pending end() to completion...
        writer.flush().await.unwrap();
        assert_eq!(closes.get(), 1, "end() must have closed the sink");

        // ...after which the writer must know it is closed: writes fail locally,
        // and close() is an idempotent no-op (no second end()).
        let err = writer.write_all(b"more").await.unwrap_err();
        assert!(
            err.to_string().contains("write after end"),
            "unexpected error: {err}"
        );
        writer.close().await.unwrap();
        assert_eq!(
            closes.get(),
            1,
            "close() after flush must not re-send end()"
        );
    });
}

#[test]
fn abort_does_not_close() {
    let sink = RecordingSink::new();
    let data = sink.data.clone();
    let closed = sink.closed.clone();

    let client = async_write_to_byte_stream(sink);
    let mut writer = byte_stream_to_async_write(client);

    futures::executor::block_on(async move {
        writer.write_all(b"payload").await.unwrap();
        // Drop the writer (and thus the only client ref) WITHOUT calling close().
        drop(writer);
    });

    assert_eq!(&data.borrow()[..], b"payload");
    assert!(
        !closed.get(),
        "dropping the writer without close() must NOT cleanly close the sink (abort semantics)"
    );
}
