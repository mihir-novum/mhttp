use crate::body::Body;
use crate::cookie_store::CookieStore;
use crate::field_lines::FieldLines;
use crate::transport::Transport;
use bytes::Bytes;
use regex::Regex;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};

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
    pub fn parse_path_params(regex: &Regex, input: &str) -> HashMap<Arc<str>, Arc<str>> {
        let caps = match regex.captures(input) {
            Some(caps) => caps,
            None => {
                return HashMap::new();
            }
        };

        regex
            .capture_names()
            .flatten()
            .filter_map(|name| {
                caps.name(name)
                    .map(|m| (Arc::from(name), Arc::from(m.as_str())))
            })
            .collect()
    }

    fn parse_query_params(url: &str) -> HashMap<Arc<str>, Arc<str>> {
        let query = url.split_once('?').map(|(_, q)| q).unwrap_or("");

        form_urlencoded::parse(query.as_bytes()).into_owned().fold(
            HashMap::new(),
            |mut map, (k, v)| {
                map.insert(Arc::from(k), Arc::from(v));
                map
            },
        )
    }
}

pub(crate) struct HttpRequest {
    client_ipv4_address: Option<Ipv4Addr>,
    client_ipv6_address: Option<Ipv6Addr>,
    host: Option<Arc<str>>,
    http_version: Bytes,
    method: HttpMethod,
    route: Arc<str>,
    field_lines: FieldLines,
    path_params: HashMap<Arc<str>, Arc<str>>,
    query_params: HashMap<Arc<str>, Arc<str>>,
    body: Body,
    cookies: CookieStore,
}

impl HttpRequest {
    pub(crate) async fn parse(
        reader: &mut BufReader<&mut Transport>,
        peer_addr: SocketAddr,
        max_body_size: usize,
    ) -> Result<Self, HttpRequestError> {
        let (client_ipv4_address, client_ipv6_address) = match peer_addr.ip() {
            IpAddr::V4(ip) => (Some(ip), None),
            IpAddr::V6(ip) => (None, Some(ip)),
        };

        let request_line = {
            let mut buf: Vec<u8> = Vec::new();
            let bytes_to_read = match reader.read_until(b'\n', &mut buf).await {
                Ok(size) => size,
                Err(_) => {
                    return Err(HttpRequestError::HeaderParsingFailed(
                        "Failed to parse request line".into(),
                    ));
                }
            };

            if bytes_to_read == 0 {
                return Err(HttpRequestError::HeaderParsingFailed(
                    "Stream closed before request line could be parsed".into(),
                ));
            }
            match Self::parse_request_line(buf) {
                Ok(v) => v,
                Err(e) => {
                    return Err(HttpRequestError::RequestLineParsingFailed(format!(
                        "Failed to parse request line: {}",
                        e
                    )));
                }
            }
        };

        let field_lines = {
            let mut buffer = Vec::new();
            loop {
                let bytes_to_read = match reader.read_until(b'\n', &mut buffer).await {
                    Ok(size) => size,
                    Err(_) => {
                        return Err(HttpRequestError::HeaderParsingFailed(
                            "Unable to parse header".into(),
                        ));
                    }
                };

                if bytes_to_read == 0 {
                    return Err(HttpRequestError::HeaderParsingFailed(
                        "Stream closed before header could be parsed".into(),
                    ));
                }

                if buffer.ends_with(b"\r\n\r\n") {
                    break;
                }
            }

            FieldLines::from(buffer.as_slice())
        };

        let host = field_lines
            .get("host")
            .map(|h| Arc::from(h.trim()))
            .or(None);

        let content_type = field_lines
            .get("content-type")
            .unwrap_or_default()
            .to_owned();

        let is_chunked = field_lines
            .get("transfer-encoding")
            .map(|v| v.contains("chunked"))
            .unwrap_or(false);

        let body = if is_chunked {
            match Body::read_chunked(reader, max_body_size, Some(content_type)).await {
                Ok(v) => v,
                Err(e) => {
                    return match e.as_str() {
                        "body too large" => Err(HttpRequestError::PayloadTooLarge),
                        err => Err(HttpRequestError::BodyParsingFailed(err.to_string())),
                    };
                }
            }
        } else {
            let content_length = field_lines
                .get("content-length")
                .unwrap_or("0")
                .parse::<usize>()
                .unwrap();

            match Body::read_exact(reader, content_length, max_body_size, Some(content_type)).await
            {
                Ok(v) => v,
                Err(e) => {
                    return match e.as_str() {
                        "body too large" => Err(HttpRequestError::PayloadTooLarge),
                        err => Err(HttpRequestError::BodyParsingFailed(err.to_string())),
                    };
                }
            }
        };

        let cookies = match field_lines.get("cookie") {
            Some(cookie_header_value) => CookieStore::from(cookie_header_value.as_bytes()),
            None => CookieStore::new(),
        };

        Ok(Self {
            client_ipv4_address,
            client_ipv6_address,
            host,
            method: match HttpMethod::from_str(str::from_utf8(&request_line.0).unwrap_or("")) {
                Ok(v) => v,
                Err(_) => {
                    return Err(HttpRequestError::RequestParsingFailed(
                        "Failed to parse HTTP method".into(),
                    ));
                }
            },
            route: match String::from_utf8(request_line.1.to_vec()) {
                Ok(s) => Arc::from(s),
                Err(_) => {
                    return Err(HttpRequestError::RequestParsingFailed(
                        "Failed to parse route".into(),
                    ));
                }
            },
            http_version: request_line.2,
            field_lines,
            path_params: HashMap::new(),
            query_params: HashMap::new(),
            body,
            cookies,
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

    pub(crate) fn header<S: Into<String>>(&self, name: S) -> Option<&str> {
        self.field_lines.get(name.into().as_str())
    }

    pub(crate) fn cookie<S: Into<String>>(&self, name: S) -> Option<&str> {
        self.cookies.get(name.into().as_str())
    }

    pub(crate) fn route(&self) -> &str {
        &self.route
    }

    pub(crate) fn method(&self) -> &HttpMethod {
        &self.method
    }

    pub(crate) fn parse_params(&mut self, route_regex: &Regex) {
        self.path_params = HttpParam::parse_path_params(route_regex, &self.route);
        self.query_params = HttpParam::parse_query_params(&self.route);
    }

    pub(crate) fn path_param<K>(&self, param_name: K) -> Option<&str>
    where
        K: Into<String>,
    {
        self.path_params
            .get(param_name.into().as_str())
            .map(|v| v.as_ref())
    }

    pub(crate) fn query_param<K>(&self, param_name: K) -> Option<&str>
    where
        K: Into<String>,
    {
        self.query_params
            .get(param_name.into().as_str())
            .map(|v| v.as_ref())
    }

    pub(crate) fn body(&self) -> &Body {
        &self.body
    }

    fn parse_request_line(buf: Vec<u8>) -> Result<(Bytes, Bytes, Bytes), &'static str> {
        let bytes = Bytes::from(buf);
        let mut end = bytes.len();

        if end > 0 && bytes[end - 1] == b'\n' {
            end -= 1;
        }
        if end > 0 && bytes[end - 1] == b'\r' {
            end -= 1;
        }

        let line = bytes.slice(0..end);

        let first_space = memchr::memchr(b' ', line.as_ref()).ok_or("missing HTTP method")?;
        if first_space == 0 {
            return Err("missing HTTP method");
        }

        let after_method = first_space + 1;
        if after_method >= line.len() {
            return Err("malformed request line");
        }
        let rest = &line[after_method..];
        let second_space_rel = memchr::memchr(b' ', rest).ok_or("missing route")?;
        if second_space_rel == 0 {
            return Err("missing route");
        }
        let second_space = after_method + second_space_rel;

        let version_start = second_space + 1;
        if version_start > line.len() {
            return Err("malformed request line");
        }
        if version_start == line.len() {
            return Err("missing HTTP version");
        }

        let method = line.slice(0..first_space);
        let route = line.slice(after_method..second_space);
        let version = line.slice(version_start..line.len());

        Ok((method, route, version))
    }
}
