use crate::HttpStatusCode;
use crate::body::Body;
use crate::field_lines::FieldLines;
use crate::request::{HttpRequest, HttpRequestError};
use crate::response::{HttpResponse, HttpResponseBodyInitialized};
use crate::transport::Transport;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::cell::RefCell;
use std::sync::OnceLock;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};


thread_local! {
    // 0 Locks, 0 Mutexes, Pure CPU Cache Locality
    static CACHED_DATE: RefCell<(u64, Vec<u8>)> = RefCell::new((0, Vec::new()));
}

fn write_date_header(buf: &mut Vec<u8>) {
    CACHED_DATE.with(|cell| {
        let mut cache = cell.borrow_mut();
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        if cache.0 != now_secs {
            let formatted = chrono::Utc::now()
                .format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string();
            cache.0 = now_secs;
            cache.1.clear();
            cache.1.extend_from_slice(formatted.as_bytes());
        }

        buf.extend_from_slice(b"date: ");
        buf.extend_from_slice(&cache.1);
        buf.extend_from_slice(b"\r\n");
    });
}

// Zero-allocation integer formatter for Keep-Alive timeouts
fn format_u64(mut n: u64, buf: &mut [u8; 20]) -> &[u8] {
    if n == 0 {
        buf[0] = b'0';
        return &buf[..1];
    }
    let mut i = 20;
    while n > 0 {
        i -= 1;
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    &buf[i..]
}

pub(crate) struct Connection {
    pub(crate) writer: BufWriter<Transport>,
    pub(crate) reader: BytesMut,
    pub(crate) header_buf: Vec<u8>, // Re-used per connection (Zero Allocations!)
    has_written_response: bool,
    keep_alive: bool,
    keep_alive_timeout_secs: u64,
}

impl Connection {
    pub(crate) fn new(transport: Transport) -> Self {
        Self {
            writer: BufWriter::with_capacity(8 * 1024, transport),
            reader: BytesMut::with_capacity(8 * 1024),
            header_buf: Vec::with_capacity(1024), // Pre-allocated once!
            has_written_response: false,
            keep_alive: true,
            keep_alive_timeout_secs: 75,
        }
    }

    pub(crate) fn set_keep_alive(&mut self, keep_alive: bool, timeout_secs: u64) {
        self.keep_alive = keep_alive;
        self.keep_alive_timeout_secs = timeout_secs;
    }

    pub(crate) fn has_written_response(&self) -> bool {
        self.has_written_response
    }

    pub(crate) async fn read_request(
        &mut self,
        max_body_size: usize,
        keep_alive_timeout: std::time::Duration,
        request_timeout: std::time::Duration,
    ) -> Result<Option<HttpRequest>, HttpRequestError> {
        let peer_addr = self.writer.get_ref().peer_addr();

        // ── STAGE 1: Idle Timeout ──
        if self.reader.is_empty() {
            if self.reader.capacity() < 4096 {
                self.reader.reserve(8192);
            }

            match tokio::time::timeout(
                keep_alive_timeout,
                self.writer.get_mut().read_buf(&mut self.reader),
            )
                .await
            {
                Ok(Ok(0)) | Err(_) => return Ok(None),
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Err(HttpRequestError::Io(e)),
            }
        }

        // ── STAGE 2: Parse Request ──
        let parse_future = async {
            loop {
                // SIMD search for the end of the headers
                if let Some(pos) = memchr::memmem::find(&self.reader, b"\r\n\r\n") {
                    let header_len = pos + 4;
                    let header_bytes = self.reader.split_to(header_len).freeze();
                    return HttpRequest::parse(header_bytes, peer_addr, max_body_size);
                }

                if self.reader.len() > 64 * 1024 {
                    return Err(HttpRequestError::HeadersTooLarge);
                }

                if self.reader.capacity() < 4096 {
                    self.reader.reserve(8192);
                }

                // ✨ PIPELINING DEADLOCK FIX:
                // We are about to block on read. We MUST ensure any partially completed
                // responses are flushed to the client before we block waiting for them.
                self.writer.flush().await.map_err(HttpRequestError::Io)?;

                let n = self
                    .writer
                    .get_mut()
                    .read_buf(&mut self.reader)
                    .await
                    .map_err(HttpRequestError::Io)?;

                if n == 0 {
                    return Err(HttpRequestError::HeaderParsingFailed(
                        "EOF before headers finished".into(),
                    ));
                }
            }
        };

        match tokio::time::timeout(request_timeout, parse_future).await {
            Ok(Ok(req)) => Ok(Some(req)),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(HttpRequestError::Timeout),
        }
    }

    pub(crate) async fn write_response(
        &mut self,
        response: HttpResponse<HttpResponseBodyInitialized>,
    ) -> Result<(), tokio::io::Error> {
        self.has_written_response = true;

        let status_forbids_body = matches!(
            response.status_code,
            HttpStatusCode::NoContent | HttpStatusCode::NotModified
        );

        // We build the HTTP header entirely in RAM reusing the `header_buf`
        self.header_buf.clear();

        // Write Status Line
        self.header_buf.extend_from_slice(&response.http_version);
        self.header_buf.extend_from_slice(b" ");
        self.header_buf.extend_from_slice(&*response.status_code.to_bytes());
        self.header_buf.extend_from_slice(b"\r\n");

        // Write Date Header
        write_date_header(&mut self.header_buf);

        // Write Keep-Alive Headers
        if self.keep_alive {
            self.header_buf.extend_from_slice(b"connection: keep-alive\r\n");
            self.header_buf.extend_from_slice(b"keep-alive: timeout=");

            let mut num_buf = [0u8; 20];
            let num_slice = format_u64(self.keep_alive_timeout_secs, &mut num_buf);
            self.header_buf.extend_from_slice(num_slice);
            self.header_buf.extend_from_slice(b"\r\n");
        } else {
            self.header_buf.extend_from_slice(b"connection: close\r\n");
        }

        // Write Content-Length zero dynamically if missing
        if !status_forbids_body
            && response.body.is_none()
            && response.field_lines.get("content-length").is_none()
        {
            self.header_buf.extend_from_slice(b"content-length: 0\r\n");
        }

        // Write User Headers
        for (field_name, field_value) in response.field_lines.iter() {
            // Prevent duplicate injected headers
            if field_name.eq_ignore_ascii_case("date")
                || field_name.eq_ignore_ascii_case("connection")
                || field_name.eq_ignore_ascii_case("keep-alive")
            {
                continue;
            }
            self.header_buf.extend_from_slice(field_name.as_bytes());
            self.header_buf.extend_from_slice(b": ");
            self.header_buf.extend_from_slice(field_value.as_bytes());
            self.header_buf.extend_from_slice(b"\r\n");
        }

        if let Some(store) = &response.cookies {
            let bytes = store.to_bytes();
            if !bytes.is_empty() {
                self.header_buf.extend_from_slice(&bytes);
            }
        }

        self.header_buf.extend_from_slice(b"\r\n");

        // Write Header Buffer into Tokio's BufWriter
        self.writer.write_all(&self.header_buf).await?;

        if !response.suppress_body && !status_forbids_body {
            self.write_body(response.body, &response.field_lines)
                .await?;
        }

        // ✨ THE MAGIC PIPELINING OPTIMIZATION ✨
        // Only flush to the OS if our incoming reader is empty.
        // If it isn't empty, it means the client stuffed MULTIPLE requests
        // into the same TCP packet! We skip flushing, loop back, process
        // the next request immediately, and send ALL responses in a single OS syscall!
        if self.reader.is_empty() {
            self.writer.flush().await?;
        }

        if let Some(hook) = response.on_sent {
            hook(response.status_code);
        }

        Ok(())
    }

    async fn write_body(
        &mut self,
        body: Option<Body>,
        field_lines: &FieldLines,
    ) -> Result<(), tokio::io::Error> {
        let is_chunked = field_lines
            .get("transfer-encoding")
            .map(|v| v.contains("chunked"))
            .unwrap_or(false);

        match body {
            Some(Body::Bytes { bytes, .. }) => {
                if is_chunked {
                    self.write_chunk(&bytes).await?;
                    self.write_terminating_chunk().await?;
                } else {
                    self.writer.write_all(&bytes).await?;
                }
            }
            Some(Body::Stream { reader, .. }) => {
                self.writer.flush().await?;

                let mut buffered = BufReader::with_capacity(64 * 1024, reader);
                if is_chunked {
                    let mut buf = vec![0u8; 64 * 1024];
                    loop {
                        let n = buffered.read(&mut buf).await?;
                        if n == 0 {
                            break;
                        }
                        self.write_chunk(&buf[..n]).await?;
                    }
                    self.write_terminating_chunk().await?;
                } else {
                    tokio::io::copy(&mut buffered, &mut self.writer).await?;
                }
            }
            None => {}
        }

        Ok(())
    }

    async fn write_chunk(&mut self, data: &[u8]) -> Result<(), tokio::io::Error> {
        let mut hex_buf = [0u8; 18];
        let hex_str = format_hex(data.len(), &mut hex_buf);

        self.writer.write_all(hex_str).await?;
        self.writer.write_all(data).await?;
        self.writer.write_all(b"\r\n").await?;
        Ok(())
    }

    async fn write_terminating_chunk(&mut self) -> Result<(), tokio::io::Error> {
        self.writer.write_all(b"0\r\n\r\n").await
    }

    pub async fn shutdown(&mut self) -> Result<(), tokio::io::Error> {
        self.writer.flush().await?;
        self.writer.get_mut().shutdown().await
    }

    pub(crate) fn into_transport(self) -> Transport {
        self.writer.into_inner()
    }
}

fn format_hex(n: usize, buf: &mut [u8; 18]) -> &[u8] {
    if n == 0 {
        buf[0] = b'0';
        buf[1] = b'\r';
        buf[2] = b'\n';
        return &buf[..3];
    }

    let mut i = 15usize;
    let mut val = n;
    while val > 0 {
        let digit = (val & 0xF) as u8;
        buf[i] = if digit < 10 {
            b'0' + digit
        } else {
            b'a' + digit - 10
        };
        i -= 1;
        val >>= 4;
    }
    i += 1;

    let end = 16;
    buf[end] = b'\r';
    buf[end + 1] = b'\n';

    &buf[i..end + 2]
}