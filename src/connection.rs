use crate::body::Body;
use crate::field_lines::FieldLines;
use crate::request::{HttpRequest, HttpRequestError};
use crate::response::HttpResponse;
use crate::transport::Transport;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

pub(crate) struct Connection {
    transport: Transport,
    has_written_response: bool,
}

impl Connection {
    pub(crate) fn new(transport: Transport) -> Self {
        Self {
            transport,
            has_written_response: false,
        }
    }
    pub(crate) fn has_written_response(&self) -> bool {
        self.has_written_response
    }

    pub(crate) async fn read_request(
        &mut self,
        max_body_size: usize,
    ) -> Result<HttpRequest, HttpRequestError> {
        let peer_addr = self.transport.peer_addr();
        let mut reader = BufReader::new(&mut self.transport);
        HttpRequest::parse(&mut reader, peer_addr, max_body_size).await
    }

    pub(crate) async fn write_response<S>(
        &mut self,
        mut response: HttpResponse<S>,
    ) -> Result<(), tokio::io::Error> {
        self.has_written_response = true;

        response.field_lines.set(
            "date",
            chrono::Utc::now()
                .format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string(),
        );

        if response.body.is_none() && response.field_lines.get("content-length").is_none() {
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
        if !response.suppress_body {
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
