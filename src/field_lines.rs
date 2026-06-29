use bytes::{BufMut, Bytes, BytesMut};
use std::collections::HashMap;

pub(crate) struct FieldLines {
    map: HashMap<String, String>,
}

impl FieldLines {
    pub(crate) fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub(crate) fn set<K, V>(&mut self, key: K, value: V)
    where
        K: Into<String>,
        V: Into<String>,
    {
        match self
            .map
            .entry(key.into().to_ascii_lowercase().trim().into())
        {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(value.into());
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.get_mut().push_str(", ");
                entry.get_mut().push_str(value.into().as_str());
            }
        }
    }

    pub(crate) fn get(&self, key: &str) -> Option<&str> {
        self.map.get(key).map(|s| s.as_str())
    }

    pub(crate) fn remove(&mut self, key: &str) -> Option<String> {
        self.map.remove(key)
    }

    pub(crate) fn to_bytes(&self) -> Bytes {
        let total_size: usize = self.map.iter().map(|(k, v)| k.len() + v.len() + 4).sum();

        let mut buf = BytesMut::with_capacity(total_size + 4);

        for (field_name, field_value) in self.map.iter() {
            buf.put_slice(field_name.as_bytes());
            buf.put_slice(b": ");
            buf.put_slice(field_value.as_bytes());
            buf.put_slice(b"\r\n");
        }

        buf.freeze()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.map.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }
}

impl From<&[u8]> for FieldLines {
    fn from(bytes: &[u8]) -> Self {
        #[inline(always)]
        fn trim_ascii(mut s: &[u8]) -> &[u8] {
            while !s.is_empty() && (s[0] == b' ' || s[0] == b'\t') {
                s = &s[1..];
            }
            while !s.is_empty() && (s[s.len() - 1] == b' ' || s[s.len() - 1] == b'\t') {
                s = &s[..s.len() - 1];
            }
            s
        }

        let mut end = bytes.len();
        if end >= 4 && &bytes[end - 4..end] == b"\r\n\r\n" {
            end -= 4;
        } else if end >= 2 && (&bytes[end - 2..end] == b"\n\n" || &bytes[end - 2..end] == b"\r\n") {
            end -= 2;
        } else if end >= 1 && &bytes[end - 1..end] == b"\n" {
            end -= 1;
        }

        let lines = bytes[..end].iter().filter(|b| **b == b'\n').count().max(1);

        let mut map: HashMap<String, String> = HashMap::with_capacity(lines);

        let mut i: usize = 0;
        while i < end {
            let line_end = match memchr::memchr(b'\n', &bytes[i..end]) {
                Some(pos) => i + pos,
                None => end,
            };

            let mut line = &bytes[i..line_end];

            if line.ends_with(b"\r") {
                line = &line[..line.len() - 1];
            }

            if line.is_empty() {
                i = if line_end < end {
                    line_end + 1
                } else {
                    line_end
                };
                continue;
            }

            let colon_position = match memchr::memchr(b':', line) {
                Some(pos) => pos,
                None => {
                    i = if line_end < end {
                        line_end + 1
                    } else {
                        line_end
                    };
                    continue;
                }
            };

            let name_bytes = trim_ascii(&line[..colon_position]);
            let value_bytes = trim_ascii(&line[colon_position + 1..]);

            if name_bytes.is_empty() {
                i = if line_end < end {
                    line_end + 1
                } else {
                    line_end
                };
                continue;
            }

            let field_name = match std::str::from_utf8(name_bytes) {
                Ok(s) => s.to_ascii_lowercase(),
                Err(_) => {
                    i = if line_end < end {
                        line_end + 1
                    } else {
                        line_end
                    };
                    continue;
                }
            };

            let field_value = match std::str::from_utf8(value_bytes) {
                Ok(s) => s,
                Err(_) => {
                    i = if line_end < end {
                        line_end + 1
                    } else {
                        line_end
                    };
                    continue;
                }
            };

            match map.entry(field_name) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(field_value.into());
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    entry.get_mut().push_str(", ");
                    entry.get_mut().push_str(field_value);
                }
            }

            i = if line_end < end {
                line_end + 1
            } else {
                line_end
            };
        }

        Self { map }
    }
}
