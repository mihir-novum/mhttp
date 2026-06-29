use crate::CookieOptions;
use crate::body::Body;
use crate::compress::Compress;
use crate::connection::Connection;
use crate::cookie_store::CookieStore;
use crate::field_lines::FieldLines;
use bytes::Bytes;
use std::sync::Arc;
use tokio::io::{AsyncRead, BufReader};
use uuid::Uuid;

const RESERVED_HEADERS: &[&str] = &[
    "content-type",
    "content-length",
    "x-request-id",
    "access-control-allow-origin",
    "access-control-allow-credentials",
    "access-control-allow-methods",
    "access-control-request-headers",
    "access-control-max-age",
    "access-control-expose-headers",
    "cookie",
    "set-cookie",
    "accept-encoding",
    "date",
    "connection",
    "keep-alive",
    "content-range",
    "accept-ranges",
];

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum HttpStatusCode {
    Ok,
    Created,
    Accepted,
    NoContent,
    BadRequest,
    Unauthorized,
    Forbidden,
    NotFound,
    InternalServerError,
    NotImplemented,
    BadGateWay,
    ServiceUnavailable,
    ContentTooLarge,
    UriTooLong,
    RequestHeaderFieldsTooLarge,
    NotModified,
    MethodNotAllowed,
    PartialContent,
    RequestTimeout,
}

impl HttpStatusCode {
    pub(crate) fn to_bytes(&self) -> Bytes {
        match self {
            HttpStatusCode::Ok => Bytes::from("200 OK"),
            HttpStatusCode::Created => Bytes::from("201 Created"),
            HttpStatusCode::Accepted => Bytes::from("202 Accepted"),
            HttpStatusCode::NoContent => Bytes::from("204 No Content"),
            HttpStatusCode::PartialContent => Bytes::from("206 Partial Content"),
            HttpStatusCode::NotModified => Bytes::from("304 Not Modified"),
            HttpStatusCode::BadRequest => Bytes::from("400 Bad Request"),
            HttpStatusCode::Unauthorized => Bytes::from("401 Unauthorized"),
            HttpStatusCode::Forbidden => Bytes::from("403 Forbidden"),
            HttpStatusCode::NotFound => Bytes::from("404 Not Found"),
            HttpStatusCode::MethodNotAllowed => Bytes::from("405 Method Not Allowed"),
            HttpStatusCode::RequestTimeout => Bytes::from("408 Request Timeout"),
            HttpStatusCode::ContentTooLarge => Bytes::from("413 Content Too Large"),
            HttpStatusCode::UriTooLong => Bytes::from("414 URI Too Long"),
            HttpStatusCode::RequestHeaderFieldsTooLarge => {
                Bytes::from("431 Request Header Fields Too Large")
            }
            HttpStatusCode::InternalServerError => Bytes::from("500 Internal Server Error"),
            HttpStatusCode::NotImplemented => Bytes::from("501 Not Implemented"),
            HttpStatusCode::BadGateWay => Bytes::from("502 Bad Gateway"),
            HttpStatusCode::ServiceUnavailable => Bytes::from("503 Service Unavailable"),
        }
    }
}

impl From<HttpStatusCode> for u16 {
    fn from(value: HttpStatusCode) -> Self {
        match value {
            HttpStatusCode::Ok => 200,
            HttpStatusCode::Created => 201,
            HttpStatusCode::Accepted => 202,
            HttpStatusCode::NoContent => 204,
            HttpStatusCode::PartialContent => 206,
            HttpStatusCode::NotModified => 304,
            HttpStatusCode::BadRequest => 400,
            HttpStatusCode::Unauthorized => 401,
            HttpStatusCode::Forbidden => 403,
            HttpStatusCode::NotFound => 404,
            HttpStatusCode::MethodNotAllowed => 405,
            HttpStatusCode::RequestTimeout => 408,
            HttpStatusCode::ContentTooLarge => 413,
            HttpStatusCode::UriTooLong => 414,
            HttpStatusCode::RequestHeaderFieldsTooLarge => 431,
            HttpStatusCode::InternalServerError => 500,
            HttpStatusCode::NotImplemented => 501,
            HttpStatusCode::BadGateWay => 502,
            HttpStatusCode::ServiceUnavailable => 503,
        }
    }
}

pub(crate) type ResponseHook = Arc<dyn Fn(HttpStatusCode) + Send + Sync>;

pub struct HttpResponseBodyUnInitialized;
pub struct HttpResponseBodyInitialized;

pub struct HttpResponse<State> {
    pub(crate) http_version: Bytes,
    pub(crate) path: Arc<str>,
    pub(crate) status_code: HttpStatusCode,
    pub(crate) body: Option<Body>,
    pub(crate) field_lines: FieldLines,
    pub(crate) cookies: Option<CookieStore>,
    pub(crate) suppress_body: bool,
    pub(crate) on_sent: Option<ResponseHook>,
    pub(crate) _state: std::marker::PhantomData<State>,
}

// ── Construction ───────────────────────────────────────────────────────────

impl HttpResponse<HttpResponseBodyUnInitialized> {
    pub(crate) fn new<B: Into<Bytes>>(http_version: B, request_id: Uuid, path: Arc<str>) -> Self {
        let mut field_lines = FieldLines::new();
        field_lines.set("x-request-id", request_id.to_string());

        Self {
            http_version: http_version.into(),
            path,
            status_code: HttpStatusCode::Ok,
            body: None,
            field_lines,
            cookies: None,
            suppress_body: false,
            on_sent: None,
            _state: std::marker::PhantomData,
        }
    }

    fn into_initialized(self, body: Option<Body>) -> HttpResponse<HttpResponseBodyInitialized> {
        HttpResponse {
            http_version: self.http_version,
            path: self.path,
            status_code: self.status_code,
            body,
            field_lines: self.field_lines,
            cookies: self.cookies,
            suppress_body: self.suppress_body,
            on_sent: self.on_sent,
            _state: std::marker::PhantomData,
        }
    }

    // ── body initializers ──────────────────────────────────────────────

    pub fn json<V: serde::Serialize>(
        mut self,
        value: V,
    ) -> HttpResponse<HttpResponseBodyInitialized> {
        let json_bytes = Bytes::from(serde_json::to_vec(&value).unwrap());
        self.field_lines.set("content-type", "application/json");
        self.field_lines
            .set("content-length", json_bytes.len().to_string());
        self.into_initialized(Some(Body::from(
            &json_bytes,
            Some("application/json".to_owned()),
        )))
    }

    pub fn bytes<V: Into<Bytes>, C: Into<String>>(
        mut self,
        bytes: V,
        content_type: C,
    ) -> HttpResponse<HttpResponseBodyInitialized> {
        let bytes = bytes.into();
        let content_type = content_type.into();
        self.field_lines.set("content-type", content_type.clone());
        self.field_lines
            .set("content-length", bytes.len().to_string());
        self.into_initialized(Some(Body::from(&bytes, Some(content_type))))
    }

    pub fn stream<R: AsyncRead + Unpin + Send + Sync + 'static, C: Into<String>>(
        mut self,
        reader: R,
        content_len: u64,
        content_type: C,
    ) -> HttpResponse<HttpResponseBodyInitialized> {
        let content_type = content_type.into();
        self.field_lines.set("content-type", content_type.clone());
        self.field_lines
            .set("content-length", content_len.to_string());
        self.into_initialized(Some(Body::from_stream(
            reader,
            content_len,
            Some(content_type),
        )))
    }

    pub fn empty(self) -> HttpResponse<HttpResponseBodyInitialized> {
        self.into_initialized(None)
    }
}

// ── Compress (initialized only) ────────────────────────────────────────────

impl HttpResponse<HttpResponseBodyInitialized> {
    pub async fn compress(mut self) -> Self {
        let Some(body) = self.body.take() else {
            return self;
        };

        let content_type = self
            .field_lines
            .get("content-type")
            .unwrap_or_default()
            .to_owned();

        if !Compress::is_compressible(&content_type) {
            self.body = Some(body);
            return self;
        }

        let accept_encoding = self
            .field_lines
            .get("accept-encoding")
            .unwrap_or("")
            .to_owned();

        let encoding = if accept_encoding.contains("zstd") {
            "zstd"
        } else if accept_encoding.contains("br") {
            "br"
        } else if accept_encoding.contains("gzip") {
            "gzip"
        } else {
            self.body = Some(body);
            return self;
        };

        match body {
            Body::Bytes {
                bytes,
                content_type: ct,
            } => {
                if bytes.len() < 1024 {
                    self.body = Some(Body::Bytes {
                        bytes,
                        content_type: ct,
                    });
                    return self;
                }

                let compressed = match encoding {
                    "zstd" => Compress::with_zstd(&bytes).await,
                    "br" => Compress::with_brotli(&bytes).await,
                    _ => Compress::with_gzip(&bytes).await,
                };

                if compressed.len() >= bytes.len() {
                    self.body = Some(Body::Bytes {
                        bytes,
                        content_type: ct,
                    });
                    return self;
                }

                self.field_lines.set("content-encoding", encoding);
                self.field_lines
                    .set("content-length", compressed.len().to_string());
                self.field_lines.set("vary", "Accept-Encoding");
                self.body = Some(Body::Bytes {
                    bytes: Bytes::from(compressed),
                    content_type: ct,
                });
            }
            Body::Stream {
                reader,
                content_type: ct,
                ..
            } => {
                self.field_lines.remove("content-length");
                self.field_lines.set("transfer-encoding", "chunked");
                self.field_lines.set("content-encoding", encoding);
                self.field_lines.set("vary", "Accept-Encoding");

                let reader = BufReader::with_capacity(64 * 1024, reader);
                let compressed_reader: Box<dyn AsyncRead + Unpin + Send + Sync> = match encoding {
                    "zstd" => Box::new(async_compression::tokio::bufread::ZstdEncoder::new(reader)),
                    "br" => Box::new(async_compression::tokio::bufread::BrotliEncoder::new(
                        reader,
                    )),
                    _ => Box::new(async_compression::tokio::bufread::GzipEncoder::new(reader)),
                };

                self.body = Some(Body::Stream {
                    reader: compressed_reader,
                    content_length: 0,
                    content_type: ct,
                });
            }
        }

        self
    }
}

// ── Shared methods (both states) ───────────────────────────────────────────

impl<State> HttpResponse<State> {
    pub fn status_code(mut self, status_code: HttpStatusCode) -> Self {
        self.status_code = status_code;
        self
    }

    pub(crate) fn set_on_set(mut self, hook: ResponseHook) -> Self {
        self.on_sent = Some(hook);
        self
    }

    pub fn add_header<K: AsRef<str>, V: AsRef<str>>(
        mut self,
        field_name: K,
        field_value: V,
    ) -> Self {
        let field_name = field_name.as_ref();
        if !RESERVED_HEADERS
            .iter()
            .any(|h| field_name.eq_ignore_ascii_case(h))
        {
            self.field_lines.set(field_name, field_value.as_ref());
        }
        self
    }

    pub(crate) fn __add_header_internal<K: AsRef<str>, V: AsRef<str>>(
        mut self,
        field_name: K,
        field_value: V,
    ) -> Self {
        self.field_lines
            .set(field_name.as_ref(), field_value.as_ref());
        self
    }

    pub fn remove_header<S: AsRef<str>>(mut self, field_name: S) -> Self {
        let field_name = field_name.as_ref();
        if !RESERVED_HEADERS
            .iter()
            .any(|h| field_name.eq_ignore_ascii_case(h))
        {
            self.field_lines.remove(field_name);
        }
        self
    }

    pub(crate) fn __remove_header_internal<S: AsRef<str>>(mut self, field_name: S) -> Self {
        self.field_lines.remove(field_name.as_ref());
        self
    }

    pub fn add_cookie<K: Into<String>, V: Into<String>>(
        mut self,
        cookie_name: K,
        cookie_value: V,
        options: Option<CookieOptions>,
    ) -> Self {
        let cookie_store = self.cookies.get_or_insert_with(CookieStore::new);
        cookie_store.set(cookie_name.into(), cookie_value.into(), options);
        self
    }

    pub fn remove_cookie<K: Into<String>>(mut self, cookie_name: K) -> Self {
        if let Some(cookie_store) = self.cookies.as_mut() {
            cookie_store.remove(&cookie_name.into());
        }
        self
    }

    pub(crate) fn suppress_body(mut self) -> Self {
        self.suppress_body = true;
        self
    }
}

// ── HttpResponseInit — uninit handle, captures &mut Connection ────────────

pub struct HttpResponseInit<'a> {
    pub(crate) connection: &'a mut Connection,
    pub(crate) response: HttpResponse<HttpResponseBodyUnInitialized>,
}

impl<'a> HttpResponseInit<'a> {
    pub fn status_code(mut self, code: HttpStatusCode) -> Self {
        self.response = self.response.status_code(code);
        self
    }

    pub fn add_header<K: AsRef<str>, V: AsRef<str>>(mut self, k: K, v: V) -> Self {
        self.response = self.response.add_header(k, v);
        self
    }

    pub(crate) fn __add_header_internal<K: AsRef<str>, V: AsRef<str>>(
        mut self,
        k: K,
        v: V,
    ) -> Self {
        self.response = self.response.__add_header_internal(k, v);
        self
    }

    pub fn remove_header<K: AsRef<str>>(mut self, name: K) -> Self {
        self.response = self.response.remove_header(name);
        self
    }

    pub(crate) fn __remove_header_internal<K: AsRef<str>>(mut self, name: K) -> Self {
        self.response = self.response.__remove_header_internal(name);
        self
    }

    pub fn add_cookie<K: Into<String>, V: Into<String>>(
        mut self,
        name: K,
        value: V,
        opts: Option<CookieOptions>,
    ) -> Self {
        self.response = self.response.add_cookie(name, value, opts);
        self
    }

    pub fn remove_cookie<K: Into<String>>(mut self, name: K) -> Self {
        self.response = self.response.remove_cookie(name);
        self
    }

    // ── body methods — transition to HttpResponseReady ─────────────────

    pub fn json<V: serde::Serialize>(self, value: V) -> HttpResponseReady<'a> {
        HttpResponseReady {
            connection: self.connection,
            response: self.response.json(value),
        }
    }

    pub fn bytes<V: Into<Bytes>, C: Into<String>>(self, b: V, ct: C) -> HttpResponseReady<'a> {
        HttpResponseReady {
            connection: self.connection,
            response: self.response.bytes(b, ct),
        }
    }

    pub fn stream<R: AsyncRead + Unpin + Send + Sync + 'static, C: Into<String>>(
        self,
        reader: R,
        len: u64,
        ct: C,
    ) -> HttpResponseReady<'a> {
        HttpResponseReady {
            connection: self.connection,
            response: self.response.stream(reader, len, ct),
        }
    }

    pub fn empty(self) -> HttpResponseReady<'a> {
        HttpResponseReady {
            connection: self.connection,
            response: self.response.empty(),
        }
    }
}

// ── HttpResponseReady — init handle, only send() ──────────────────────────

pub struct HttpResponseReady<'a> {
    pub(crate) connection: &'a mut Connection,
    pub(crate) response: HttpResponse<HttpResponseBodyInitialized>,
}

impl<'a> HttpResponseReady<'a> {
    pub fn status_code(mut self, code: HttpStatusCode) -> Self {
        self.response = self.response.status_code(code);
        self
    }

    pub fn add_header<K: AsRef<str>, V: AsRef<str>>(mut self, k: K, v: V) -> Self {
        self.response = self.response.add_header(k, v);
        self
    }

    pub async fn compress(mut self) -> Self {
        self.response = self.response.compress().await;
        self
    }

    pub async fn failable_send(self) -> Result<(), tokio::io::Error> {
        self.connection.write_response(self.response).await
    }

    pub async fn send(self) {
        let _ = self.failable_send().await;
    }
}
