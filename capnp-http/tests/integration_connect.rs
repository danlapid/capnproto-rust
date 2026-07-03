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

//! CONNECT (tunnel) integration tests, driven over a loopback TCP capnp session:
//!
//!   Rust CONNECT client (`svc.connect`) --capnp/TCP--> connect_service_to_capnp(ConnectBackend)

#![cfg(feature = "connect")]

mod common;

use std::cell::RefCell;
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::time::Duration;

use bytes::Bytes;
use capnp_http::{
    connect_service_to_capnp, service_to_capnp_with_connect, ConnectOutcome, ConnectResponse,
    ConnectService, ConnectSettings, HttpService, IncomingBody, Tunnel,
};
use futures::channel::oneshot;
use futures::io::{AsyncReadExt, AsyncWriteExt};
use http_body_util::{BodyExt, Full};

use common::start_client;

fn pattern(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i % 251) as u8).collect()
}

// A backend CONNECT service, routed by host prefix. The control handle lets a
// test observe a server-side EOF (the client dropping its tunnel without a
// clean close).
#[derive(Clone, Default)]
struct ConnectControl {
    client_gone_tx: Rc<RefCell<Option<oneshot::Sender<()>>>>,
}

struct ConnectBackend {
    ctl: ConnectControl,
}

impl ConnectService for ConnectBackend {
    async fn connect(
        self: Rc<Self>,
        host: String,
        headers: http::HeaderMap,
        settings: ConnectSettings,
        mut tunnel: Tunnel,
        response: ConnectResponse,
    ) -> Result<(), capnp::Error> {
        if host.starts_with("deny") {
            response
                .reject(
                    http::StatusCode::FORBIDDEN,
                    &http::HeaderMap::new(),
                    b"denied",
                )
                .await?;
            return Ok(());
        }

        if host.starts_with("headers") {
            // Echo the CONNECT request headers back as (prefixed) accept-response
            // headers.
            let mut resp_headers = http::HeaderMap::new();
            for (name, value) in headers.iter() {
                let echoed = http::HeaderName::try_from(format!("x-echo-{name}")).unwrap();
                resp_headers.insert(echoed, value.clone());
            }
            response.accept(http::StatusCode::OK, &resp_headers).await?;
            let _ = tunnel.close().await;
            return Ok(());
        }

        response
            .accept(http::StatusCode::OK, &http::HeaderMap::new())
            .await?;

        if host.starts_with("checktls") {
            let msg = if settings.use_tls {
                "tls=on"
            } else {
                "tls=off"
            };
            tunnel
                .write_all(msg.as_bytes())
                .await
                .map_err(|e| capnp::Error::failed(format!("tunnel write: {e}")))?;
            let _ = tunnel.close().await;
            return Ok(());
        }

        if host.starts_with("eof") {
            // A client that drops its tunnel without a clean close manifests
            // server-side either as read-EOF or as the `connect()` call being
            // cancelled (the tunnel's keepalive guard aborts the RPC, dropping
            // this handler future mid-read). A drop guard fires the signal in
            // both cases.
            struct FireOnDrop(Option<oneshot::Sender<()>>);
            impl Drop for FireOnDrop {
                fn drop(&mut self) {
                    if let Some(tx) = self.0.take() {
                        let _ = tx.send(());
                    }
                }
            }
            let _fire = FireOnDrop(self.ctl.client_gone_tx.borrow_mut().take());

            let mut buf = vec![0u8; 8192];
            loop {
                match tunnel.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
            return Ok(());
        }

        if host.starts_with("tls") {
            if let Some(requested) = tunnel.next_tls_request().await {
                tunnel
                    .write_all(requested.as_bytes())
                    .await
                    .map_err(|e| capnp::Error::failed(format!("tunnel write: {e}")))?;
                let _ = tunnel.close().await;
            }
            return Ok(());
        }

        // Default: echo bytes until the client closes the tunnel's write side.
        let mut buf = vec![0u8; 8192];
        loop {
            let n = tunnel
                .read(&mut buf)
                .await
                .map_err(|e| capnp::Error::failed(format!("tunnel read: {e}")))?;
            if n == 0 {
                break;
            }
            tunnel
                .write_all(&buf[..n])
                .await
                .map_err(|e| capnp::Error::failed(format!("tunnel write: {e}")))?;
        }
        let _ = tunnel.close().await;
        Ok(())
    }
}

fn with_connect<F, Fut>(body: F)
where
    F: FnOnce(capnp_http::CapnpHttpService) -> Fut + 'static,
    Fut: Future<Output = ()>,
{
    with_connect_ctl(|svc, _ctl| body(svc));
}

fn with_connect_ctl<F, Fut>(body: F)
where
    F: FnOnce(capnp_http::CapnpHttpService, ConnectControl) -> Fut + 'static,
    Fut: Future<Output = ()>,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async move {
        let ctl = ConnectControl::default();
        let svc = start_client(connect_service_to_capnp(ConnectBackend {
            ctl: ctl.clone(),
        }))
        .await;
        body(svc, ctl).await;
    });
}

#[test]
fn reject_with_body() {
    with_connect(|svc| async move {
        let outcome = svc
            .connect(
                "deny.example.com:443",
                &http::HeaderMap::new(),
                ConnectSettings::default(),
            )
            .await
            .unwrap();
        match outcome {
            ConnectOutcome::Rejected { status, body, .. } => {
                assert_eq!(status, 403);
                let got = body.collect().await.unwrap().to_bytes();
                assert_eq!(&got[..], b"denied");
            }
            ConnectOutcome::Accepted { .. } => panic!("expected reject"),
        }
    });
}

#[test]
fn start_tls_signal_crosses() {
    with_connect(|svc| async move {
        let outcome = svc
            .connect(
                "tls.example.com:443",
                &http::HeaderMap::new(),
                ConnectSettings { use_tls: false },
            )
            .await
            .unwrap();
        match outcome {
            ConnectOutcome::Accepted { mut tunnel, .. } => {
                tunnel.start_tls("sni.example").await.unwrap();
                let mut got = String::new();
                tunnel.read_to_string(&mut got).await.unwrap();
                assert_eq!(got, "sni.example");
            }
            ConnectOutcome::Rejected { .. } => panic!("expected accept"),
        }
    });
}

#[test]
fn large_bidirectional_stream() {
    with_connect(|svc| async move {
        let outcome = svc
            .connect(
                "example.com:443",
                &http::HeaderMap::new(),
                ConnectSettings::default(),
            )
            .await
            .unwrap();
        let ConnectOutcome::Accepted { tunnel, .. } = outcome else {
            panic!("expected accept");
        };
        let total = 1024 * 1024;
        let data = pattern(total);

        let (mut reader, mut writer) = tunnel.split();
        let data_for_writer = data.clone();
        let writer_task = tokio::task::spawn_local(async move {
            writer.write_all(&data_for_writer).await.unwrap();
            // Close the write side so the echo server sees EOF and closes back.
            writer.close().await.unwrap();
        });

        let mut got = Vec::new();
        reader.read_to_end(&mut got).await.unwrap();
        assert_eq!(got.len(), data.len());
        assert_eq!(got, data);
        writer_task.await.unwrap();
    });
}

#[test]
fn concurrent_tunnels() {
    with_connect(|svc| async move {
        let svc = &svc;
        let mut tasks = Vec::new();
        for i in 0..8u32 {
            tasks.push(async move {
                let outcome = svc
                    .connect(
                        "example.com:443",
                        &http::HeaderMap::new(),
                        ConnectSettings::default(),
                    )
                    .await
                    .unwrap();
                let ConnectOutcome::Accepted { mut tunnel, .. } = outcome else {
                    panic!("expected accept");
                };
                let msg = format!("tunnel-{i}");
                tunnel.write_all(msg.as_bytes()).await.unwrap();
                tunnel.flush().await.unwrap();
                let mut buf = vec![0u8; msg.len()];
                tunnel.read_exact(&mut buf).await.unwrap();
                assert_eq!(buf, msg.as_bytes());
                tunnel.close().await.unwrap();
            });
        }
        futures::future::join_all(tasks).await;
    });
}

#[test]
fn use_tls_initial_setting_crosses() {
    with_connect(|svc| async move {
        let outcome = svc
            .connect(
                "checktls.example:443",
                &http::HeaderMap::new(),
                ConnectSettings { use_tls: true },
            )
            .await
            .unwrap();
        let ConnectOutcome::Accepted { mut tunnel, .. } = outcome else {
            panic!("expected accept");
        };
        let mut got = String::new();
        tunnel.read_to_string(&mut got).await.unwrap();
        assert_eq!(got, "tls=on");
    });
}

#[test]
fn connect_headers_propagate_both_ways() {
    with_connect(|svc| async move {
        let mut headers = http::HeaderMap::new();
        headers.insert("x-tunnel-auth", "secret-token".parse().unwrap());
        headers.insert("x-other", "v2".parse().unwrap());
        let outcome = svc
            .connect("headers.example:443", &headers, ConnectSettings::default())
            .await
            .unwrap();
        let ConnectOutcome::Accepted {
            status,
            headers,
            mut tunnel,
        } = outcome
        else {
            panic!("expected accept");
        };
        assert_eq!(status, 200);
        assert_eq!(headers.get("x-echo-x-tunnel-auth").unwrap(), "secret-token");
        assert_eq!(headers.get("x-echo-x-other").unwrap(), "v2");
        // The backend closes the tunnel immediately; drain to EOF.
        let mut buf = Vec::new();
        tunnel.read_to_end(&mut buf).await.unwrap();
        assert!(buf.is_empty());
    });
}

#[test]
fn client_drop_seen_by_server() {
    with_connect_ctl(|svc, ctl| async move {
        let (tx, rx) = oneshot::channel();
        *ctl.client_gone_tx.borrow_mut() = Some(tx);

        let outcome = svc
            .connect(
                "eof.example:443",
                &http::HeaderMap::new(),
                ConnectSettings::default(),
            )
            .await
            .unwrap();
        let ConnectOutcome::Accepted { tunnel, .. } = outcome else {
            panic!("expected accept");
        };
        // Drop the tunnel without writing or closing; the server must observe the
        // end of its read side.
        drop(tunnel);

        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .expect("server did not observe client drop within timeout")
            .expect("control sender dropped without signalling");
    });
}

/// Minimal request backend: replies `200 "hello"` to any request.
#[derive(Clone)]
struct GreetService;

impl HttpService<IncomingBody> for GreetService {
    type ResBody = Full<Bytes>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<http::Response<Full<Bytes>>, Infallible>>>>;

    fn call(&self, _req: http::Request<IncomingBody>) -> Self::Future {
        Box::pin(async {
            Ok(http::Response::builder()
                .status(200)
                .body(Full::new(Bytes::from_static(b"hello")))
                .unwrap())
        })
    }
}

#[test]
fn combined_request_and_connect() {
    // `service_to_capnp_with_connect` serves both ordinary requests and CONNECT on
    // a single capability; exercise each over the same connection.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async move {
        let bootstrap = service_to_capnp_with_connect(
            GreetService,
            ConnectBackend {
                ctl: ConnectControl::default(),
            },
        );
        let svc = start_client(bootstrap).await;

        let resp = svc
            .call(
                http::Request::builder()
                    .uri("/")
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            )
            .await
            .expect("request should succeed");
        assert_eq!(resp.status(), 200);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"hello");

        let outcome = svc
            .connect(
                "example.com:443",
                &http::HeaderMap::new(),
                ConnectSettings::default(),
            )
            .await
            .expect("connect should succeed");
        let ConnectOutcome::Accepted {
            status, mut tunnel, ..
        } = outcome
        else {
            panic!("expected accept");
        };
        assert_eq!(status, 200);
        tunnel.write_all(b"xyz").await.unwrap();
        tunnel.flush().await.unwrap();
        let mut buf = [0u8; 3];
        tunnel.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"xyz");
        tunnel.close().await.unwrap();
    });
}
