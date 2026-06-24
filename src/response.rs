use crate::CookieOptions;
use crate::body::Body;
use crate::compress::Compress;
use crate::cookie_store::CookieStore;
use crate::field_lines::FieldLines;
use crate::transport::Transport;
use bytes::{Bytes, BytesMut};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
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
}

impl HttpStatusCode {
    pub(crate) fn to_bytes(&self) -> Bytes {
        match self {
            HttpStatusCode::Ok => Bytes::from("200 OK"),
            HttpStatusCode::Created => Bytes::from("201 Created"),
            HttpStatusCode::Accepted => Bytes::from("202 Accepted"),
            HttpStatusCode::NoContent => Bytes::from("204 No Content"),
            HttpStatusCode::BadRequest => Bytes::from("400 Bad Request"),
            HttpStatusCode::Unauthorized => Bytes::from("401 Unauthorized"),
            HttpStatusCode::Forbidden => Bytes::from("403 Forbidden"),
            HttpStatusCode::NotFound => Bytes::from("404 Not Found"),
            HttpStatusCode::ContentTooLarge => Bytes::from("413 Content Too Large"),
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
            HttpStatusCode::BadRequest => 400,
            HttpStatusCode::Unauthorized => 401,
            HttpStatusCode::Forbidden => 403,
            HttpStatusCode::NotFound => 404,
            HttpStatusCode::ContentTooLarge => 413,
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
    stream: Transport,
    http_version: Bytes,
    path: Arc<str>,
    status_code: HttpStatusCode,
    body: Option<Body>,
    field_lines: FieldLines,
    cookies: Option<CookieStore>,
    suppress_body: bool,
    on_sent: Option<ResponseHook>,
    _state: std::marker::PhantomData<State>,
}

impl HttpResponse<HttpResponseBodyUnInitialized> {
    pub(crate) fn new<B>(
        stream: Transport,
        http_version: B,
        request_id: Uuid,
        path: Arc<str>,
    ) -> Self
    where
        B: Into<Bytes>,
    {
        let mut field_lines = FieldLines::new();
        field_lines.set("x-request-id", request_id.to_string());

        Self {
            stream,
            path,
            http_version: http_version.into(),
            status_code: HttpStatusCode::Ok,
            body: None,
            field_lines,
            cookies: None,
            suppress_body: false,
            on_sent: None,
            _state: std::marker::PhantomData,
        }
    }

    pub fn status_code(mut self, status_code: HttpStatusCode) -> Self {
        self.status_code = status_code;
        self
    }

    pub(crate) fn set_on_set(mut self, hook: ResponseHook) -> Self {
        self.on_sent = Some(hook);
        self
    }

    pub fn add_header<K, V>(mut self, field_name: K, field_value: V) -> Self
    where
        V: Into<String>,
        K: Into<String>,
    {
        let field_name = field_name.into();
        if !RESERVED_HEADERS
            .iter()
            .any(|h| field_name.eq_ignore_ascii_case(h))
        {
            self.field_lines.set(field_name, field_value.into());
        }
        self
    }

    pub(crate) fn __add_header_internal<K, V>(mut self, field_name: K, field_value: V) -> Self
    where
        V: Into<String>,
        K: Into<String>,
    {
        let field_name = field_name.into();
        self.field_lines.set(field_name, field_value.into());
        self
    }

    pub fn remove_header<S>(mut self, field_name: S) -> Self
    where
        S: Into<String>,
    {
        let field_name = field_name.into();
        if !RESERVED_HEADERS
            .iter()
            .any(|h| field_name.eq_ignore_ascii_case(h))
        {
            self.field_lines.remove(field_name.as_str());
        }
        self
    }

    pub fn add_cookie<K, V>(
        mut self,
        cookie_name: K,
        cookie_value: V,
        options: Option<CookieOptions>,
    ) -> Self
    where
        V: Into<String>,
        K: Into<String>,
    {
        let cookie_name = cookie_name.into();
        let cookie_store = self.cookies.get_or_insert_with(CookieStore::new);
        cookie_store.set(cookie_name, cookie_value.into(), options);
        self
    }

    pub fn remove_cookie<K>(mut self, cookie_name: K) -> Self
    where
        K: Into<String>,
    {
        let cookie_name = cookie_name.into();
        if let Some(cookie_store) = self.cookies.as_mut() {
            cookie_store.remove(&cookie_name);
        }
        self
    }

    pub(crate) fn __remove_header_internal<S>(mut self, field_name: S) -> Self
    where
        S: Into<String>,
    {
        let field_name = field_name.into();
        self.field_lines.remove(field_name.as_str());
        self
    }

    pub fn json<V>(mut self, value: V) -> HttpResponse<HttpResponseBodyInitialized>
    where
        V: serde::Serialize,
    {
        let json_bytes = Bytes::from(serde_json::to_vec(&value).unwrap());

        self.field_lines.set("content-type", "application/json");
        self.field_lines
            .set("content-length", json_bytes.len().to_string());

        HttpResponse {
            stream: self.stream,
            path: self.path,
            http_version: self.http_version,
            status_code: self.status_code,
            field_lines: self.field_lines,
            cookies: self.cookies,
            suppress_body: self.suppress_body,
            body: Some(Body::from(&json_bytes, Some("application/json".to_owned()))),
            on_sent: self.on_sent,
            _state: std::marker::PhantomData,
        }
    }

    pub fn bytes<V, C>(
        mut self,
        bytes: V,
        content_type: C,
    ) -> HttpResponse<HttpResponseBodyInitialized>
    where
        V: Into<Bytes>,
        C: Into<String>,
    {
        let bytes = bytes.into();
        let content_length = bytes.len();
        let content_type = content_type.into();

        self.field_lines.set("content-type", content_type.clone());
        self.field_lines
            .set("content-length", content_length.to_string());

        HttpResponse {
            stream: self.stream,
            path: self.path,
            http_version: self.http_version,
            status_code: self.status_code,
            field_lines: self.field_lines,
            cookies: self.cookies,
            suppress_body: self.suppress_body,
            body: Some(Body::from(&bytes, Some(content_type))),
            on_sent: self.on_sent,
            _state: std::marker::PhantomData,
        }
    }

    pub fn stream<R, C>(
        mut self,
        reader: R,
        content_len: u64,
        content_type: C,
    ) -> HttpResponse<HttpResponseBodyInitialized>
    where
        R: AsyncRead + Unpin + Send + 'static,
        C: Into<String>,
    {
        let content_type = content_type.into();
        self.field_lines.set("content-type", content_type.clone());
        self.field_lines
            .set("content-length", content_len.to_string());

        HttpResponse {
            stream: self.stream,
            path: self.path,
            http_version: self.http_version,
            status_code: self.status_code,
            field_lines: self.field_lines,
            cookies: self.cookies,
            suppress_body: self.suppress_body,
            body: Some(Body::from_stream(reader, content_len, Some(content_type))),
            on_sent: self.on_sent,
            _state: std::marker::PhantomData,
        }
    }
}

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

        if Compress::is_compressible(&content_type) {
            self.body = Some(body);
            return self;
        }

        let Some(encoding) = self
            .field_lines
            .get("accept-encoding")
            .map(|v| v.to_owned())
        else {
            self.body = Some(body);
            return self;
        };

        let encoding = if encoding.contains("zstd") {
            "zstd"
        } else if encoding.contains("gzip") {
            "gzip"
        } else if encoding.contains("br") {
            "br"
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

                let compressed_reader: Box<dyn AsyncRead + Unpin + Send> = match encoding {
                    "zstd" => Box::new(async_compression::tokio::bufread::ZstdEncoder::new(
                        BufReader::with_capacity(64 * 1024, reader),
                    )),
                    "br" => Box::new(async_compression::tokio::bufread::BrotliEncoder::new(
                        BufReader::with_capacity(64 * 1024, reader),
                    )),
                    _ => Box::new(async_compression::tokio::bufread::GzipEncoder::new(
                        BufReader::with_capacity(64 * 1024, reader),
                    )),
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

impl<State> HttpResponse<State> {
    pub(crate) fn suppress_body(mut self) -> Self {
        self.suppress_body = true;
        self
    }

    pub async fn fallible_send(mut self) -> Result<(), tokio::io::Error> {
        self.field_lines.set(
            "date",
            chrono::Utc::now()
                .format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string(),
        );

        let status_code = self.status_code;
        let status_code_copy = status_code.clone();

        let status_code = status_code.to_bytes();
        let mut response_line =
            BytesMut::with_capacity(self.http_version.len() + status_code.len() + 6);
        response_line.extend_from_slice(&self.http_version);
        response_line.extend_from_slice(b" ");
        response_line.extend_from_slice(&status_code);
        response_line.extend_from_slice(b"\r\n");

        self.stream.write_all(response_line.as_ref()).await?;
        self.stream.write_all(&self.field_lines.to_bytes()).await?;

        if let Some(cookie_store) = &self.cookies {
            let cookie_bytes = cookie_store.to_bytes();
            if !cookie_bytes.is_empty() {
                self.stream.write_all(&cookie_bytes).await?;
            }
        }

        self.stream.write_all(b"\r\n").await?;

        if !self.suppress_body {
            match self.body {
                Some(Body::Stream { reader, .. }) => {
                    let is_chunked = self
                        .field_lines
                        .get("transfer-encoding")
                        .map(|v| v.contains("chunked"))
                        .unwrap_or(false);

                    let mut buffered = BufReader::with_capacity(64 * 1024, reader);

                    if is_chunked {
                        let mut buf = vec![0u8; 64 * 1024];
                        loop {
                            let n = buffered.read(&mut buf).await?;
                            if n == 0 {
                                break;
                            }

                            self.stream
                                .write_all(format!("{:X}\r\n", n).as_bytes())
                                .await?;
                            self.stream.write_all(&buf[..n]).await?;
                            self.stream.write_all(b"\r\n").await?;
                        }

                        self.stream.write_all(b"0\r\n\r\n").await?;
                    } else {
                        tokio::io::copy(&mut buffered, &mut self.stream).await?;
                    }
                }
                Some(Body::Bytes { bytes, .. }) => {
                    let is_chunked = self
                        .field_lines
                        .get("transfer-encoding")
                        .map(|v| v.contains("chunked"))
                        .unwrap_or(false);

                    if is_chunked {
                        self.stream
                            .write_all(format!("{:X}\r\n", bytes.len()).as_bytes())
                            .await?;
                        self.stream.write_all(&bytes).await?;
                        self.stream.write_all(b"\r\n").await?;
                        self.stream.write_all(b"0\r\n\r\n").await?;
                    } else {
                        self.stream.write_all(&bytes).await?;
                    }
                }
                None => {}
            }
        }

        self.stream.shutdown().await?;

        if let Some(hook) = self.on_sent.take() {
            hook(status_code_copy);
        }

        Ok(())
    }

    pub async fn send(self) {
        let _ = self.fallible_send().await;
    }
}
