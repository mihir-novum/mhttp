use crate::{HttpCall, HttpStatusCode};
use std::path::Path;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;

pub struct StaticFileOptions {
    pub dir: &'static str,
    pub index: Option<&'static str>,
}

pub async fn serve_static(call: &mut HttpCall, opts: StaticFileOptions) {
    let tail = call
        .path_param("__path__")
        .unwrap_or("")
        .trim_start_matches('/')
        .to_owned();

    let base = Path::new(opts.dir);

    // Empty tail or "/" → serve index directly
    if tail.is_empty() {
        serve_index(call, base, opts.index).await;
        return;
    }

    let requested = base.join(&tail);

    let canonical_base = match base.canonicalize() {
        Ok(p) => p,
        Err(_) => {
            call.response()
                .status_code(HttpStatusCode::InternalServerError)
                .empty()
                .send()
                .await;
            return;
        }
    };

    let canonical_req = match requested.canonicalize() {
        Ok(p) => p,
        Err(_) => {
            serve_index(call, base, opts.index).await;
            return;
        }
    };

    // Path traversal check
    if !canonical_req.starts_with(&canonical_base) {
        call.response()
            .status_code(HttpStatusCode::Forbidden)
            .empty()
            .send()
            .await;
        return;
    }

    if canonical_req.is_dir() {
        serve_index(call, base, opts.index).await;
        return;
    }

    serve_file(call, &canonical_req).await;
}

// Extracted helpers

async fn serve_index(call: &mut HttpCall, base: &Path, index: Option<&'static str>) {
    match index {
        Some(idx) => {
            let path = base.join(idx);
            if path.exists() {
                serve_file(call, &path).await;
            } else {
                call.response()
                    .status_code(HttpStatusCode::NotFound)
                    .empty()
                    .send()
                    .await;
            }
        }
        None => {
            call.response()
                .status_code(HttpStatusCode::Forbidden)
                .empty()
                .send()
                .await;
        }
    }
}

async fn serve_file(call: &mut HttpCall, path: &Path) {
    let mime = mime_from_path(path);

    let mut file = match tokio::fs::File::open(path).await {
        Ok(f) => f,
        Err(_) => {
            call.response()
                .status_code(HttpStatusCode::NotFound)
                .empty()
                .send()
                .await;
            return;
        }
    };

    let metadata = match file.metadata().await {
        Ok(m) => m,
        Err(_) => {
            call.response()
                .status_code(HttpStatusCode::InternalServerError)
                .empty()
                .send()
                .await;
            return;
        }
    };

    let file_len = metadata.len();

    // -- BUG FIX: Handle HTTP Range Requests --
    if let Some(range_header) = call.header("range") {
        if range_header.starts_with("bytes=") {
            let range_str = &range_header[6..];
            let parts: Vec<&str> = range_str.split('-').collect();

            let start: u64 = parts.first().unwrap_or(&"").parse().unwrap_or(0);
            let end: u64 = parts
                .get(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(file_len.saturating_sub(1)); // Default to end of file if not provided

            if start <= end && end < file_len {
                if let Ok(_) = file.seek(std::io::SeekFrom::Start(start)).await {
                    let content_length = end - start + 1;

                    call.response()
                        .status_code(HttpStatusCode::PartialContent)
                        .__add_header_internal(
                            "content-range",
                            format!("bytes {}-{}/{}", start, end, file_len),
                        )
                        .__add_header_internal("accept-ranges", "bytes")
                        // Take only the requested chunk using AsyncReadExt::take
                        .stream(file.take(content_length), content_length, mime)
                        .send()
                        .await;
                    return;
                }
            }
        }
    }

    // -- Normal full-file response --
    call.response()
        .add_header("accept-ranges", "bytes") // Tells the browser it CAN seek in the future
        .stream(file, file_len, mime)
        .send()
        .await;
}

fn mime_from_path(path: &Path) -> &'static str {
    match path.extension().and_then(|e| e.to_str()) {
        Some("html") | Some("htm") => "text/html; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("js") => "application/javascript",
        Some("json") => "application/json",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("svg") => "image/svg+xml",
        Some("ico") => "image/x-icon",
        Some("woff") => "font/woff",
        Some("woff2") => "font/woff2",
        Some("ttf") => "font/ttf",
        Some("pdf") => "application/pdf",
        Some("txt") => "text/plain; charset=utf-8",
        Some("xml") => "application/xml",
        Some("wasm") => "application/wasm",
        _ => "application/octet-stream",
    }
}
