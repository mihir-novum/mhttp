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

static CACHED_DATE: OnceLock<parking_lot::RwLock<(u64, String)>> = OnceLock::new();

thread_local! {
    static HEADER_BUF: RefCell<BytesMut> = RefCell::new(BytesMut::with_capacity(4096));
}

fn get_date_header() -> String {
    let cache = CACHED_DATE.get_or_init(|| parking_lot::RwLock::new((0, String::new())));

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    {
        let read = cache.read();
        if read.0 == now_secs {
            return read.1.clone();
        }
    }

    let formatted = chrono::Utc::now()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string();

    *cache.write() = (now_secs, formatted.clone());
    formatted
}

pub(crate) struct Connection {
    pub(crate)  writer: BufWriter<Transport>,
    pub(crate) reader: BytesMut,
    has_written_response: bool,
    keep_alive: bool,
    keep_alive_timeout_secs: u64,
}

impl Connection {
    pub(crate) fn new(transport: Transport) -> Self {
        Self {
            writer: BufWriter::with_capacity(8 * 1024, transport),
            reader: BytesMut::with_capacity(8 * 1024),
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
            // Reserve reclaims memory if `header_bytes` was dropped properly!
            if self.reader.capacity() < 4096 {
                self.reader.reserve(8192);
            }

            match tokio::time::timeout(
                keep_alive_timeout,
                self.writer.get_mut().read_buf(&mut self.reader)
            ).await {
                Ok(Ok(0)) | Err(_) => return Ok(None),
                Ok(Ok(_)) => {},
                Ok(Err(e)) => return Err(HttpRequestError::Io(e)),
            }
        }

        // ── STAGE 2: Parse Request ──
        let parse_future = async {
            loop {
                // SIMD search for the end of the headers
                if let Some(pos) = memchr::memmem::find(&self.reader, b"\r\n\r\n") {
                    let header_len = pos + 4;
                    // O(1) split. Leaves pipelined requests intact for the next loop!
                    let header_bytes = self.reader.split_to(header_len).freeze();
                    return HttpRequest::parse(header_bytes, peer_addr, max_body_size);
                }

                if self.reader.len() > 64 * 1024 {
                    return Err(HttpRequestError::HeadersTooLarge);
                }

                if self.reader.capacity() < 4096 {
                    self.reader.reserve(8192);
                }

                let n = self.writer.get_mut().read_buf(&mut self.reader).await.map_err(HttpRequestError::Io)?;
                if n == 0 {
                    return Err(HttpRequestError::HeaderParsingFailed("EOF before headers finished".into()));
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
        mut response: HttpResponse<HttpResponseBodyInitialized>,
    ) -> Result<(), tokio::io::Error> {
        self.has_written_response = true;

        response.field_lines.set("date", get_date_header());

        response.field_lines.set(
            "connection",
            if self.keep_alive {
                "keep-alive"
            } else {
                "close"
            },
        );

        if self.keep_alive {
            response.field_lines.set(
                "keep-alive",
                format!("timeout={}", self.keep_alive_timeout_secs),
            );
        }

        let status_forbids_body = matches!(
            response.status_code,
            HttpStatusCode::NoContent | HttpStatusCode::NotModified
        );

        if !status_forbids_body
            && response.body.is_none()
            && response.field_lines.get("content-length").is_none()
        {
            response.field_lines.set("content-length", "0");
        }

        let status_code = response.status_code.clone();
        let status_bytes = response.status_code.to_bytes();

        let header_bytes = HEADER_BUF.with(|cell| {
            let mut buf = cell.borrow_mut();
            buf.clear();

            buf.put_slice(&response.http_version);
            buf.put_slice(b" ");
            buf.put_slice(&status_bytes);
            buf.put_slice(b"\r\n");

            for (field_name, field_value) in response.field_lines.iter() {
                buf.put_slice(field_name.as_bytes());
                buf.put_slice(b": ");
                buf.put_slice(field_value.as_bytes());
                buf.put_slice(b"\r\n");
            }

            if let Some(store) = &response.cookies {
                let bytes = store.to_bytes();
                if !bytes.is_empty() {
                    buf.put_slice(&bytes);
                }
            }

            buf.put_slice(b"\r\n");

            buf.split().freeze()
        });

        self.writer.write_all(&header_bytes).await?;

        if !response.suppress_body && !status_forbids_body {
            self.write_body(response.body, &response.field_lines)
                .await?;
        }

        self.writer.flush().await?;

        if let Some(hook) = response.on_sent {
            hook(status_code);
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
        let mut hex_buf = [0u8; 18]; // max usize hex is 16 chars + \r\n
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

    // Write hex digits in reverse
    let mut i = 15usize; // leave room for \r\n at end
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
    i += 1; // i now points to first hex digit

    // Append \r\n
    let end = 16;
    buf[end] = b'\r';
    buf[end + 1] = b'\n';

    &buf[i..end + 2]
}
