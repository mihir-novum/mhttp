use crate::{HttpCall, HttpStatusCode};
use std::path::Path;
use tokio::io::AsyncReadExt;

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
                    .send()
                    .await;
            }
        }
        None => {
            call.response()
                .status_code(HttpStatusCode::Forbidden)
                .send()
                .await;
        }
    }
}

async fn serve_file(call: &mut HttpCall, path: &Path) {
    let mut file = match tokio::fs::File::open(path).await {
        Ok(f) => f,
        Err(_) => {
            call.response()
                .status_code(HttpStatusCode::NotFound)
                .send()
                .await;
            return;
        }
    };

    let mut contents = Vec::new();
    if file.read_to_end(&mut contents).await.is_err() {
        call.response()
            .status_code(HttpStatusCode::InternalServerError)
            .send()
            .await;
        return;
    }

    let mime = mime_from_path(path);

    call.response()
        .status_code(HttpStatusCode::Ok)
        .bytes(contents, mime)
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
