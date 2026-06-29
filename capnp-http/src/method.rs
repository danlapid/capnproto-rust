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

//! Conversion between the capnp `HttpMethod` enum and [`http::Method`].
//!
//! Note: the capnp `HttpMethod` enum does **not** include `CONNECT`; CONNECT
//! requests travel through the separate `HttpService.connect()` method.

use crate::http_over_capnp_capnp::HttpMethod;

fn method_token(method: HttpMethod) -> &'static str {
    use HttpMethod::*;
    match method {
        Get => "GET",
        Head => "HEAD",
        Post => "POST",
        Put => "PUT",
        Delete => "DELETE",
        Patch => "PATCH",
        Purge => "PURGE",
        Options => "OPTIONS",
        Trace => "TRACE",
        Copy => "COPY",
        Lock => "LOCK",
        Mkcol => "MKCOL",
        Move => "MOVE",
        Propfind => "PROPFIND",
        Proppatch => "PROPPATCH",
        Search => "SEARCH",
        Unlock => "UNLOCK",
        Acl => "ACL",
        Report => "REPORT",
        Mkactivity => "MKACTIVITY",
        Checkout => "CHECKOUT",
        Merge => "MERGE",
        Msearch => "MSEARCH",
        Notify => "NOTIFY",
        Subscribe => "SUBSCRIBE",
        Unsubscribe => "UNSUBSCRIBE",
        Query => "QUERY",
        Ban => "BAN",
    }
}

/// Converts a capnp `HttpMethod` to an [`http::Method`].
pub(crate) fn from_capnp(method: HttpMethod) -> http::Method {
    http::Method::from_bytes(method_token(method).as_bytes())
        .expect("capnp HttpMethod tokens are always valid HTTP method tokens")
}

/// Converts an [`http::Method`] to a capnp `HttpMethod`.
///
/// Returns an error for methods not representable in the capnp enum (including
/// `CONNECT`, which is handled by `HttpService.connect()` instead).
pub(crate) fn to_capnp(method: &http::Method) -> Result<HttpMethod, capnp::Error> {
    use HttpMethod::*;
    Ok(match method.as_str() {
        "GET" => Get,
        "HEAD" => Head,
        "POST" => Post,
        "PUT" => Put,
        "DELETE" => Delete,
        "PATCH" => Patch,
        "PURGE" => Purge,
        "OPTIONS" => Options,
        "TRACE" => Trace,
        "COPY" => Copy,
        "LOCK" => Lock,
        "MKCOL" => Mkcol,
        "MOVE" => Move,
        "PROPFIND" => Propfind,
        "PROPPATCH" => Proppatch,
        "SEARCH" => Search,
        "UNLOCK" => Unlock,
        "ACL" => Acl,
        "REPORT" => Report,
        "MKACTIVITY" => Mkactivity,
        "CHECKOUT" => Checkout,
        "MERGE" => Merge,
        "MSEARCH" => Msearch,
        "NOTIFY" => Notify,
        "SUBSCRIBE" => Subscribe,
        "UNSUBSCRIBE" => Unsubscribe,
        "QUERY" => Query,
        "BAN" => Ban,
        other => {
            return Err(capnp::Error::failed(format!(
                "HTTP method `{other}` is not representable in http-over-capnp"
            )))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_all_methods() {
        // Every capnp HttpMethod variant must round-trip through http::Method.
        for raw in 0u16..=27 {
            let m = HttpMethod::try_from(raw).expect("known variant");
            let http = from_capnp(m);
            let back = to_capnp(&http).expect("round trips");
            assert_eq!(m as u16, back as u16, "method {m:?} did not round-trip");
        }
        // Guard against schema drift: 28 should not (yet) be a variant. If this
        // fails, a new method was added and the tables above need updating.
        assert!(HttpMethod::try_from(28u16).is_err());
    }

    #[test]
    fn connect_is_rejected() {
        assert!(to_capnp(&http::Method::CONNECT).is_err());
    }
}
