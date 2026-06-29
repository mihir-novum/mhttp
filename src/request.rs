use crate::body::Body;
use crate::cookie_store::CookieStore;
use crate::field_lines::FieldLines;
use bytes::Bytes;
use std::borrow::Cow;
use std::fmt::{Display, Formatter};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use std::sync::{Arc, OnceLock};

#[derive(thiserror::Error, Debug)]
pub enum HttpRequestError {
    #[error("{0}")]
    RequestLineParsingFailed(String),
    #[error("{0}")]
    HeaderParsingFailed(String),
    #[error("{0}")]
    BodyParsingFailed(String),
    #[error("{0}")]
    RequestParsingFailed(String),
    #[error("invalid http method")]
    InvalidHttpMethod(),
    #[error("payload too large")]
    PayloadTooLarge,
    #[error("request line too long")]
    RequestLineTooLong,
    #[error("headers too large")]
    HeadersTooLarge,
    #[error("io error: {0}")]
    Io(tokio::io::Error),
    #[error("request time out")]
    Timeout,
}

#[derive(Debug, PartialEq, Clone)]
pub enum HttpMethod {
    GET,
    POST,
    PATCH,
    PUT,
    DELETE,
    OPTIONS,
    HEAD,
    CONNECT,
    TRACE,
}

impl FromStr for HttpMethod {
    type Err = HttpRequestError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "get" => Ok(HttpMethod::GET),
            "post" => Ok(HttpMethod::POST),
            "patch" => Ok(HttpMethod::PATCH),
            "put" => Ok(HttpMethod::PUT),
            "delete" => Ok(HttpMethod::DELETE),
            "options" => Ok(HttpMethod::OPTIONS),
            "head" => Ok(HttpMethod::HEAD),
            "connect" => Ok(HttpMethod::CONNECT),
            "trace" => Ok(HttpMethod::TRACE),
            _ => Err(HttpRequestError::InvalidHttpMethod()),
        }
    }
}

impl Display for HttpMethod {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let method = match self {
            HttpMethod::GET => "GET",
            HttpMethod::POST => "POST",
            HttpMethod::PATCH => "PATCH",
            HttpMethod::PUT => "PUT",
            HttpMethod::DELETE => "DELETE",
            HttpMethod::OPTIONS => "OPTIONS",
            HttpMethod::HEAD => "HEAD",
            HttpMethod::CONNECT => "CONNECT",
            HttpMethod::TRACE => "TRACE",
        };

        f.write_str(method)
    }
}

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub enum HttpParam {
    Int(i64),
    Float(f64),
    Hex(u64),
    Bool(bool),
    IPv4(Ipv4Addr),
    IPv6(Ipv6Addr),
    Url(url::Url),
    Uuid(uuid::Uuid),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    DateTime(chrono::DateTime<chrono::FixedOffset>),
    Email(String),
    Text(String),
}

impl HttpParam {
    pub(crate) fn parse_query_params(url: &str) -> Vec<(Arc<str>, Arc<str>)> {
        let query = url.split_once('?').map(|(_, q)| q).unwrap_or("");

        form_urlencoded::parse(query.as_bytes())
            .into_iter()
            // Convert the Cow<str> into Arc<str>
            .map(|(k, v)| (Arc::from(k.as_ref()), Arc::from(v.as_ref())))
            .collect()
    }
}

pub(crate) enum BodyState {
    Unread {
        content_length: usize,
        is_chunked: bool,
    },
    Reading,
    Read(Body),
}

pub(crate) struct HttpRequest {
    client_ipv4_address: Option<Ipv4Addr>,
    client_ipv6_address: Option<Ipv6Addr>,
    host: Option<Arc<str>>,
    http_version: Bytes,
    method: HttpMethod,
    pub(crate) route: Arc<str>,
    field_lines: FieldLines,
    cookies: CookieStore,
    pub(crate) max_body_size: usize,
    pub(crate) body_state: BodyState,

    // OPTIMIZATION: Replaced HashMap with Vec.
    // Vec::new() allocates 0 bytes on the heap. HashMap::new() allocates ~100 bytes.
    pub(crate) path_params: Vec<(Arc<str>, Arc<str>)>,
    pub(crate) query_params: OnceLock<Vec<(Arc<str>, Arc<str>)>>,
}

impl HttpRequest {
    pub(crate) fn parse(
        header_bytes: Bytes,
        peer_addr: SocketAddr,
        max_body_size: usize,
    ) -> Result<Self, HttpRequestError> {
        let (client_ipv4_address, client_ipv6_address) = match peer_addr.ip() {
            IpAddr::V4(ip) => (Some(ip), None),
            IpAddr::V6(ip) => (None, Some(ip)),
        };

        let req_line_end = memchr::memchr(b'\n', &header_bytes).ok_or(
            HttpRequestError::RequestLineParsingFailed("Missing request line".into()),
        )?;

        let (method, route, http_version) = {
            let mut line = &header_bytes[..req_line_end];
            if line.last() == Some(&b'\r') {
                line = &line[..line.len() - 1];
            }

            let first_space = memchr::memchr(b' ', line).ok_or(
                HttpRequestError::RequestLineParsingFailed("Missing HTTP method".into()),
            )?;
            let after_method = first_space + 1;

            let second_space_rel = memchr::memchr(b' ', &line[after_method..]).ok_or(
                HttpRequestError::RequestLineParsingFailed("Missing route".into()),
            )?;
            let second_space = after_method + second_space_rel;

            let version_start = second_space + 1;

            let m_str = std::str::from_utf8(&line[0..first_space]).unwrap_or("");
            let method = HttpMethod::from_str(m_str).unwrap_or(HttpMethod::GET);

            let r_str = std::str::from_utf8(&line[after_method..second_space]).unwrap_or("");
            let route = Arc::from(r_str);

            // OPTIMIZATION: `.slice()` is zero-copy! It just bumps the ref-count of `header_bytes`.
            // `copy_from_slice()` allocates new memory.
            let version = header_bytes.slice(version_start..line.len());

            (method, route, version)
        };

        let field_lines = FieldLines::from(&header_bytes[req_line_end + 1..]);

        let host = field_lines.get("host").map(|h| Arc::from(h.trim()));

        let content_length = field_lines
            .get("content-length")
            .unwrap_or("0")
            .parse::<usize>()
            .unwrap_or(0);

        let is_chunked = field_lines
            .get("transfer-encoding")
            .map(|v| v.contains("chunked"))
            .unwrap_or(false);

        if content_length > max_body_size {
            return Err(HttpRequestError::PayloadTooLarge);
        }

        let cookies = match field_lines.get("cookie") {
            Some(cookie_header_value) => CookieStore::from(cookie_header_value.as_bytes()),
            None => CookieStore::new(),
        };

        Ok(Self {
            client_ipv4_address,
            client_ipv6_address,
            host,
            method,
            route,
            http_version,
            field_lines,
            cookies,
            max_body_size,
            body_state: BodyState::Unread {
                content_length,
                is_chunked,
            },
            path_params: Vec::new(), // 0 Heap Allocations!
            query_params: OnceLock::new(),
        })
    }

    pub(crate) fn client_ipv4_address(&self) -> Option<Ipv4Addr> {
        self.client_ipv4_address
    }

    pub(crate) fn client_ipv6_address(&self) -> Option<Ipv6Addr> {
        self.client_ipv6_address
    }

    pub(crate) fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }

    pub(crate) fn http_version(&self) -> &Bytes {
        &self.http_version
    }

    // OPTIMIZATION: Removed `S: Into<String>`!
    // This stops silent String allocations every time you check a header.
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.field_lines.get(name)
    }

    pub(crate) fn cookie(&self, name: &str) -> Option<&str> {
        self.cookies.get(name)
    }

    pub(crate) fn route(&self) -> &str {
        &self.route
    }

    pub(crate) fn method(&self) -> &HttpMethod {
        &self.method
    }
}
