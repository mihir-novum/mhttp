use tokio::io::AsyncReadExt;

pub(crate) struct Compress {}

impl Compress {
    pub(crate) fn is_compressible(content_type: &str) -> bool {
        content_type.starts_with("text/")
            || content_type.contains("json")
            || content_type.contains("javascript")
            || content_type.contains("xml")
            || content_type.contains("svg")
    }

    pub(crate) async fn with_gzip(data: &[u8]) -> Vec<u8> {
        use async_compression::tokio::bufread::GzipEncoder;
        use tokio::io::AsyncReadExt;
        let mut enc = GzipEncoder::new(std::io::Cursor::new(data));
        let mut out = Vec::new();
        enc.read_to_end(&mut out).await.unwrap();
        out
    }

    pub(crate) async fn with_brotli(data: &[u8]) -> Vec<u8> {
        use async_compression::tokio::bufread::BrotliEncoder;
        let mut enc = BrotliEncoder::new(std::io::Cursor::new(data));
        let mut out = Vec::new();
        enc.read_to_end(&mut out).await.unwrap();
        out
    }

    pub(crate) async fn with_zstd(data: &[u8]) -> Vec<u8> {
        use async_compression::tokio::bufread::ZstdEncoder;
        let mut enc = ZstdEncoder::new(std::io::Cursor::new(data));
        let mut out = Vec::new();
        enc.read_to_end(&mut out).await.unwrap();
        out
    }
}
