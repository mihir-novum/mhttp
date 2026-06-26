use crate::HttpStatusCode;
use crate::body::Body;
use crate::field_lines::FieldLines;
use crate::request::{HttpRequest, HttpRequestError};
use crate::response::{HttpResponse, HttpResponseBodyInitialized};
use crate::transport::Transport;
use bytes::BytesMut;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

pub(crate) struct Connection {
    transport: Transport,
    has_written_response: bool,
    keep_alive: bool,
    keep_alive_timeout_secs: u64,
}

impl Connection {
    pub(crate) fn new(transport: Transport) -> Self {
        Self {
            transport,
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
        let peer_addr = self.transport.peer_addr();
        let mut reader = BufReader::new(&mut self.transport);

        // =====================================================================
        // STAGE 1: IDLE TIMEOUT (Keep-Alive)
        // Wait for the client to send the first byte. `fill_buf` pulls data
        // into the reader but does NOT consume it.
        // =====================================================================
        match tokio::time::timeout(keep_alive_timeout, reader.fill_buf()).await {
            Ok(Ok(buf)) if buf.is_empty() => {
                // EOF reached: Client cleanly closed the connection.
                return Ok(None);
            }
            Ok(Ok(_)) => {
                // Success: We see bytes! The client has started talking.
            }
            Ok(Err(e)) => return Err(HttpRequestError::Io(e)),
            Err(_) => {
                // Timeout: The client sat idle for 75 seconds. Close cleanly.
                return Ok(None);
            }
        }

        // =====================================================================
        // STAGE 2: PARSE TIMEOUT (Request Timeout)
        // The client is actively talking. Enforce a hard 60-second limit to
        // parse the headers and body to prevent Slowloris attacks.
        // =====================================================================
        match tokio::time::timeout(
            request_timeout,
            HttpRequest::parse(&mut reader, peer_addr, max_body_size),
        )
        .await
        {
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

        response.field_lines.set(
            "date",
            chrono::Utc::now()
                .format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string(),
        );

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

        let mut line =
            BytesMut::with_capacity(response.http_version.len() + status_bytes.len() + 6);
        line.extend_from_slice(&response.http_version);
        line.extend_from_slice(b" ");
        line.extend_from_slice(&status_bytes);
        line.extend_from_slice(b"\r\n");
        self.transport.write_all(&line).await?;

        self.transport
            .write_all(&response.field_lines.to_bytes())
            .await?;

        if let Some(store) = &response.cookies {
            let bytes = store.to_bytes();
            if !bytes.is_empty() {
                self.transport.write_all(&bytes).await?;
            }
        }

        self.transport.write_all(b"\r\n").await?;

        if !response.suppress_body && !status_forbids_body {
            self.write_body(response.body, &response.field_lines)
                .await?;
        }

        self.transport.flush().await?;

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
                    self.transport.write_all(&bytes).await?;
                }
            }
            Some(Body::Stream { reader, .. }) => {
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
                    tokio::io::copy(&mut buffered, &mut self.transport).await?;
                }
            }
            None => {}
        }

        Ok(())
    }

    async fn write_chunk(&mut self, data: &[u8]) -> Result<(), tokio::io::Error> {
        self.transport
            .write_all(format!("{:X}\r\n", data.len()).as_bytes())
            .await?;
        self.transport.write_all(data).await?;
        self.transport.write_all(b"\r\n").await?;
        Ok(())
    }

    async fn write_terminating_chunk(&mut self) -> Result<(), tokio::io::Error> {
        self.transport.write_all(b"0\r\n\r\n").await
    }

    pub async fn shutdown(&mut self) -> Result<(), tokio::io::Error> {
        self.transport.shutdown().await
    }

    pub(crate) fn into_transport(self) -> Transport {
        self.transport
    }
}
