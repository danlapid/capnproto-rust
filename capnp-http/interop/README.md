# Cross-implementation interop harness

An opt-in harness for verifying that the Rust `capnp-http` client is
wire-compatible with the reference C++ `http-over-capnp` implementation. It is
not part of the normal `cargo test` run because it requires a built C++
capnproto checkout and a C++23 toolchain:

```sh
cargo test -p capnp-http --test interop -- --ignored --nocapture
```

`echo_server.c++` is a minimal C++ server that exposes a `capnp::HttpService`
(an echo service) over a two-party RPC connection on a TCP port, built with the
reference `HttpOverCapnpFactory`. It prints the bound port on stdout, then
serves. `build.sh` compiles it together with the compat sources
(`byte-stream.c++`, `http-over-capnp.c++`) against a built `../capnproto/c++`
checkout, producing `target/interop/echo_server`.

Requirements:

- A consistently built capnproto C++ checkout: the prebuilt `.libs/*.a` must
  match the `src/` you compile against. (If `src/` is newer than the libs
  you'll get undefined-symbol errors; rebuild the checkout first with
  `make -C path/to/capnproto/c++`.)
- A C++23 compiler. On Apple clang this needs `-std=c++2b
  -fsized-deallocation` (already set in `build.sh`).
- zlib (`-lz`), pulled in by `libkj-http`.

To run the pieces by hand:

```sh
# Build the C++ echo server (point at your capnproto checkout if needed):
capnp-http/interop/build.sh /path/to/capnproto/c++

# Start it (prints the chosen port on the first stdout line):
./target/interop/echo_server 0
```

Then connect a Rust client to `127.0.0.1:<port>` with a `twoparty` VatNetwork
(`Side::Client`), bootstrap an `http_service::Client`, and wrap it with
`capnp_http::capnp_to_service` (this is exactly what `tests/interop.rs` does).
A GET returns `200 "hello-from-c++"`; a POST echoes its body.
