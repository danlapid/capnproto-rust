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

//! Conversion between `List(HttpHeader)` and [`http::HeaderMap`], including the
//! `CommonHeaderName` / `CommonHeaderValue` interning tables.
//!
//! Unlike the C++ implementation, which reads the `$commonText` annotation at
//! runtime via reflection, we hard-code the (fixed, small) tables here and verify
//! them against the schema with a unit test (`common_header_names_round_trip`).

use http::HeaderName;

use crate::http_over_capnp_capnp::{http_header, CommonHeaderName, CommonHeaderValue};

/// Maps a capnp `CommonHeaderName` to the corresponding [`http::HeaderName`].
/// Returns `None` for `Invalid` (or any future enumerant not yet mapped).
fn common_name_to_http(name: CommonHeaderName) -> Option<HeaderName> {
    use http::header::*;
    use CommonHeaderName::*;
    Some(match name {
        Invalid => return None,
        AcceptCharset => ACCEPT_CHARSET,
        AcceptEncoding => ACCEPT_ENCODING,
        AcceptLanguage => ACCEPT_LANGUAGE,
        AcceptRanges => ACCEPT_RANGES,
        Accept => ACCEPT,
        AccessControlAllowOrigin => ACCESS_CONTROL_ALLOW_ORIGIN,
        Age => AGE,
        Allow => ALLOW,
        Authorization => AUTHORIZATION,
        CacheControl => CACHE_CONTROL,
        ContentDisposition => CONTENT_DISPOSITION,
        ContentEncoding => CONTENT_ENCODING,
        ContentLanguage => CONTENT_LANGUAGE,
        ContentLength => CONTENT_LENGTH,
        ContentLocation => CONTENT_LOCATION,
        ContentRange => CONTENT_RANGE,
        ContentType => CONTENT_TYPE,
        Cookie => COOKIE,
        Date => DATE,
        Etag => ETAG,
        Expect => EXPECT,
        Expires => EXPIRES,
        From => FROM,
        Host => HOST,
        IfMatch => IF_MATCH,
        IfModifiedSince => IF_MODIFIED_SINCE,
        IfNoneMatch => IF_NONE_MATCH,
        IfRange => IF_RANGE,
        IfUnmodifiedSince => IF_UNMODIFIED_SINCE,
        LastModified => LAST_MODIFIED,
        Link => LINK,
        Location => LOCATION,
        MaxForwards => MAX_FORWARDS,
        ProxyAuthenticate => PROXY_AUTHENTICATE,
        ProxyAuthorization => PROXY_AUTHORIZATION,
        Range => RANGE,
        Referer => REFERER,
        Refresh => REFRESH,
        RetryAfter => RETRY_AFTER,
        Server => SERVER,
        SetCookie => SET_COOKIE,
        StrictTransportSecurity => STRICT_TRANSPORT_SECURITY,
        TransferEncoding => TRANSFER_ENCODING,
        UserAgent => USER_AGENT,
        Vary => VARY,
        Via => VIA,
        WwwAuthenticate => WWW_AUTHENTICATE,
    })
}

/// Maps an [`http::HeaderName`] to a capnp `CommonHeaderName`, if it is one of the
/// interned names. Matching is on the lowercase canonical form.
fn http_name_to_common(name: &HeaderName) -> Option<CommonHeaderName> {
    use CommonHeaderName::*;
    Some(match name.as_str() {
        "accept-charset" => AcceptCharset,
        "accept-encoding" => AcceptEncoding,
        "accept-language" => AcceptLanguage,
        "accept-ranges" => AcceptRanges,
        "accept" => Accept,
        "access-control-allow-origin" => AccessControlAllowOrigin,
        "age" => Age,
        "allow" => Allow,
        "authorization" => Authorization,
        "cache-control" => CacheControl,
        "content-disposition" => ContentDisposition,
        "content-encoding" => ContentEncoding,
        "content-language" => ContentLanguage,
        "content-length" => ContentLength,
        "content-location" => ContentLocation,
        "content-range" => ContentRange,
        "content-type" => ContentType,
        "cookie" => Cookie,
        "date" => Date,
        "etag" => Etag,
        "expect" => Expect,
        "expires" => Expires,
        "from" => From,
        "host" => Host,
        "if-match" => IfMatch,
        "if-modified-since" => IfModifiedSince,
        "if-none-match" => IfNoneMatch,
        "if-range" => IfRange,
        "if-unmodified-since" => IfUnmodifiedSince,
        "last-modified" => LastModified,
        "link" => Link,
        "location" => Location,
        "max-forwards" => MaxForwards,
        "proxy-authenticate" => ProxyAuthenticate,
        "proxy-authorization" => ProxyAuthorization,
        "range" => Range,
        "referer" => Referer,
        "refresh" => Refresh,
        "retry-after" => RetryAfter,
        "server" => Server,
        "set-cookie" => SetCookie,
        "strict-transport-security" => StrictTransportSecurity,
        "transfer-encoding" => TransferEncoding,
        "user-agent" => UserAgent,
        "vary" => Vary,
        "via" => Via,
        "www-authenticate" => WwwAuthenticate,
        _ => return None,
    })
}

fn common_value_bytes(value: CommonHeaderValue) -> Option<&'static [u8]> {
    match value {
        CommonHeaderValue::Invalid => None,
        CommonHeaderValue::GzipDeflate => Some(b"gzip, deflate"),
    }
}

fn bytes_to_common_value(value: &[u8]) -> Option<CommonHeaderValue> {
    match value {
        b"gzip, deflate" => Some(CommonHeaderValue::GzipDeflate),
        _ => None,
    }
}

fn not_in_schema(_e: ::capnp::NotInSchema) -> capnp::Error {
    capnp::Error::failed("unknown enumerant in http-over-capnp message".to_string())
}

/// Writes an [`http::HeaderMap`] into a pre-sized `List(HttpHeader)` builder.
///
/// `builder` must have been initialized with `headers.len()` elements.
///
/// Header values are written as **raw bytes** into the capnp `Text` field (rather
/// than validated as UTF-8). HTTP header values are byte strings (RFC 9110 obs-text
/// permits non-ASCII), and the C++ `http-over-capnp` reference does the same
/// (kj writes the value bytes unvalidated). This is a deliberate, interop-
/// preserving deviation from the nominal "Text is UTF-8" rule; in practice header
/// values are almost always ASCII.
pub(crate) fn to_capnp(
    headers: &http::HeaderMap,
    mut builder: ::capnp::struct_list::Builder<http_header::Owned>,
) -> Result<(), capnp::Error> {
    for (i, (name, value)) in headers.iter().enumerate() {
        let header = builder.reborrow().get(i as u32);
        match http_name_to_common(name) {
            Some(common) => {
                let mut c = header.init_common();
                c.set_name(common);
                if let Some(cv) = bytes_to_common_value(value.as_bytes()) {
                    c.set_common_value(cv);
                } else {
                    c.set_value(::capnp::text::Reader(value.as_bytes()));
                }
            }
            None => {
                let mut nv = header.init_uncommon();
                nv.set_name(::capnp::text::Reader(name.as_str().as_bytes()));
                nv.set_value(::capnp::text::Reader(value.as_bytes()));
            }
        }
    }
    Ok(())
}

/// Reads a `List(HttpHeader)` into an [`http::HeaderMap`].
///
/// Duplicate headers are preserved via [`http::HeaderMap::append`].
pub(crate) fn from_capnp(
    reader: ::capnp::struct_list::Reader<http_header::Owned>,
) -> Result<http::HeaderMap, capnp::Error> {
    use http_header::common::Which as CommonWhich;
    use http_header::Which;

    let mut map = http::HeaderMap::with_capacity(reader.len() as usize);
    for header in reader.iter() {
        match header.which().map_err(not_in_schema)? {
            Which::Common(common) => {
                let name = common_name_to_http(common.get_name().map_err(not_in_schema)?)
                    .ok_or_else(|| {
                        capnp::Error::failed("invalid common header name on the wire".to_string())
                    })?;
                let value = match common.which().map_err(not_in_schema)? {
                    CommonWhich::CommonValue(cv) => {
                        let bytes =
                            common_value_bytes(cv.map_err(not_in_schema)?).ok_or_else(|| {
                                capnp::Error::failed("invalid common header value".to_string())
                            })?;
                        http::HeaderValue::from_bytes(bytes)
                            .expect("interned common header values are always valid")
                    }
                    CommonWhich::Value(v) => header_value(v?.as_bytes())?,
                };
                map.append(name, value);
            }
            Which::Uncommon(nv) => {
                let nv = nv?;
                let name = HeaderName::from_bytes(nv.get_name()?.as_bytes())
                    .map_err(|e| capnp::Error::failed(format!("invalid header name: {e}")))?;
                let value = header_value(nv.get_value()?.as_bytes())?;
                map.append(name, value);
            }
        }
    }
    Ok(map)
}

fn header_value(bytes: &[u8]) -> Result<http::HeaderValue, capnp::Error> {
    http::HeaderValue::from_bytes(bytes)
        .map_err(|e| capnp::Error::failed(format!("invalid header value: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn common_header_names_round_trip() {
        // Walk every CommonHeaderName enumerant and verify it maps to an
        // http::HeaderName and back. This catches schema drift: a newly added
        // enumerant that isn't in the tables above will fail here.
        for raw in 1u16..256 {
            let Ok(name) = CommonHeaderName::try_from(raw) else {
                break;
            };
            let http = common_name_to_http(name).unwrap_or_else(|| {
                panic!("CommonHeaderName {name:?} (={raw}) has no http mapping")
            });
            let back = http_name_to_common(&http).unwrap_or_else(|| {
                panic!("http header {http:?} did not map back to a CommonHeaderName")
            });
            assert_eq!(
                name as u16, back as u16,
                "CommonHeaderName {name:?} did not round-trip"
            );
        }
    }

    #[test]
    fn common_value_round_trips() {
        let bytes = common_value_bytes(CommonHeaderValue::GzipDeflate).unwrap();
        assert_eq!(
            bytes_to_common_value(bytes),
            Some(CommonHeaderValue::GzipDeflate)
        );
    }
}
