use base64::Engine;
use bytes::Bytes;
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};
use serde_json::Value;
use std::collections::HashMap;

#[derive(thiserror::Error, Debug)]
pub enum MultipartError {
    #[error("boundary not found")]
    MissingBoundary,
    #[error("{0}")]
    Malformed(&'static str),
    #[error("{0}")]
    Utf8(#[from] std::str::Utf8Error),
}

pub struct Multipart {
    fields: HashMap<String, MultiPartField>,
}

impl Multipart {
    pub(crate) fn parse(body: &Bytes, boundary: &str) -> Result<Self, MultipartError> {
        let buf = body.as_ref();

        let open = {
            let mut v = Vec::with_capacity(2 + boundary.len() + 2);
            v.extend_from_slice(b"--");
            v.extend_from_slice(boundary.as_bytes());
            v.extend_from_slice(b"\r\n");
            v
        };
        let mid = {
            let mut v = Vec::with_capacity(2 + 2 + boundary.len());
            v.extend_from_slice(b"\r\n--");
            v.extend_from_slice(boundary.as_bytes());
            v
        };

        let open_finder = memchr::memmem::Finder::new(&open);
        let mid_finder = memchr::memmem::Finder::new(&mid);
        let hdr_end_finder = memchr::memmem::Finder::new(b"\r\n\r\n");

        let first = open_finder
            .find(buf)
            .ok_or(MultipartError::MissingBoundary)?;
        let mut pos = first + open.len();

        let mut fields: HashMap<String, MultiPartField> = HashMap::new();
        let arc = body.clone();

        loop {
            let hdr_rel = hdr_end_finder
                .find(&buf[pos..])
                .ok_or(MultipartError::Malformed("headers missing CRLFCRLF"))?;
            let hdr_end = pos + hdr_rel;
            let headers = &buf[pos..hdr_end];
            let (name, content_type) = Self::parse_headers_memchr(headers)?;

            let data_start = hdr_end + 4;

            let search = &buf[data_start..];
            let rel = mid_finder
                .find(search)
                .ok_or(MultipartError::Malformed("next boundary not found"))?;
            let delim_at = data_start + rel;

            let data = arc.slice(data_start..delim_at);

            let after = delim_at + mid.len();
            let is_final = buf.get(after..after + 2) == Some(b"--");
            if is_final {
                fields.insert(
                    name.clone(),
                    MultiPartField {
                        name,
                        content_type,
                        content: data,
                    },
                );
                break;
            } else {
                if buf.get(after..after + 2) != Some(b"\r\n") {
                    return Err(MultipartError::Malformed("boundary line missing CRLF"));
                }
                fields.insert(
                    name.clone(),
                    MultiPartField {
                        name,
                        content_type,
                        content: data,
                    },
                );
                pos = after + 2;
                if pos >= buf.len() {
                    break;
                }
            }
        }

        Ok(Self { fields })
    }

    pub fn get(&self, name: &str) -> Option<&MultiPartField> {
        self.fields.get(name)
    }

    pub fn to_bytes(&self) -> Bytes {
        Bytes::from(serde_json::to_vec(&self.fields).unwrap())
    }

    #[inline]
    fn parse_headers_memchr(hdr: &[u8]) -> Result<(String, Option<String>), MultipartError> {
        let mut name: Option<String> = None;
        let mut filename: Option<String> = None;
        let mut ctype: Option<String> = None;

        let mut start = 0;
        while start <= hdr.len() {
            let next = match memchr::memchr(b'\n', &hdr[start..]) {
                Some(off) => start + off,
                None => {
                    if start < hdr.len() {
                        Self::parse_header_line(
                            &hdr[start..],
                            &mut name,
                            &mut filename,
                            &mut ctype,
                        )?;
                    }
                    break;
                }
            };

            let line = if next > start && hdr[next - 1] == b'\r' {
                &hdr[start..next - 1]
            } else {
                &hdr[start..next]
            };

            if !line.is_empty() {
                Self::parse_header_line(line, &mut name, &mut filename, &mut ctype)?;
            }

            start = next + 1;
        }

        let name = name.ok_or(MultipartError::Malformed(
            "missing Content-Disposition name",
        ))?;

        if ctype.is_none() && filename.is_some() {
            ctype = Some("application/octet-stream".to_string());
        }
        Ok((name, ctype))
    }

    #[inline]
    fn parse_header_line(
        line: &[u8],
        name: &mut Option<String>,
        filename: &mut Option<String>,
        ctype: &mut Option<String>,
    ) -> Result<(), MultipartError> {
        let s = std::str::from_utf8(line)?;

        let Some(colon) = s.find(':') else {
            return Ok(());
        };
        let key = s[..colon].trim();
        let val = s[colon + 1..].trim();

        if key.eq_ignore_ascii_case("Content-Type") {
            if !val.is_empty() {
                *ctype = Some(val.to_string());
            }
        } else if key.eq_ignore_ascii_case("Content-Disposition") {
            for seg in val.split(';').map(|p| p.trim()) {
                if let Some(v) = seg.strip_prefix("name=") {
                    *name = Some(Self::unquote(v).to_string());
                } else if let Some(v) = seg.strip_prefix("filename=") {
                    *filename = Some(Self::unquote(v).to_string());
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn unquote(s: &str) -> &str {
        let s = s.trim();
        if s.len() >= 2 && s.as_bytes()[0] == b'"' && s.as_bytes()[s.len() - 1] == b'"' {
            &s[1..s.len() - 1]
        } else {
            s
        }
    }
}

#[derive(Debug)]
pub struct MultiPartField {
    name: String,
    content_type: Option<String>,
    content: Bytes,
}

impl Serialize for MultiPartField {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let num_fields = 2 + if self.content_type.is_some() { 1 } else { 0 };
        let mut state = serializer.serialize_struct("MultiPartField", num_fields)?;

        state.serialize_field("name", &self.name)?;

        if let Some(ct) = &self.content_type {
            state.serialize_field("content_type", ct)?;
        }

        let b64 = base64::engine::general_purpose::STANDARD.encode(&self.content[..]);
        state.serialize_field("data", &b64)?;

        state.end()
    }
}

impl MultiPartField {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }
    pub fn content(&self) -> &Bytes {
        &self.content
    }
}

pub struct Body {
    bytes: Bytes,
    content_type: Option<String>,
}

impl Body {
    pub(crate) fn from(bytes: &Bytes, content_type: Option<String>) -> Self {
        Self {
            bytes: bytes.clone(),
            content_type,
        }
    }

    pub fn as_json(&self) -> Option<Value> {
        match self.content_type {
            Some(ref ct) if ct.starts_with("application/json") => {
                serde_json::from_slice(&self.bytes[..]).ok()
            }
            _ => None,
        }
    }

    pub fn as_multipart(&self) -> Option<Multipart> {
        match self.content_type {
            Some(ref content_type) if content_type.starts_with("multipart/form-data") => {
                let boundary = content_type
                    .split(';')
                    .find_map(|param| param.trim().strip_prefix("boundary="))
                    .unwrap_or_default();

                Multipart::parse(&self.bytes, boundary).ok()
            }
            _ => None,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        self.bytes.clone()
    }
}

impl From<Value> for Body {
    fn from(json: Value) -> Self {
        Self {
            bytes: serde_json::to_vec(&json).unwrap().into(),
            content_type: Some("application/json".into()),
        }
    }
}

impl From<serde_json::Map<String, Value>> for Body {
    fn from(json: serde_json::Map<String, Value>) -> Self {
        Self {
            bytes: serde_json::to_vec(&json).unwrap().into(),
            content_type: Some("application/json".into()),
        }
    }
}
