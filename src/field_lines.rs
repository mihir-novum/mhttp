use smallvec::SmallVec;

pub(crate) struct FieldLines {
    // A single contiguous memory pool for all header strings (Zero-allocation updates)
    arena: Vec<u8>,
    // Store indices: (name_start, name_len, value_start, value_len)
    headers: Vec<(u32, u32, u32, u32)>,
}

impl FieldLines {
    pub(crate) fn new() -> Self {
        Self {
            arena: Vec::new(),
            headers: Vec::new(),
        }
    }

    pub(crate) fn set<K: AsRef<str>, V: AsRef<str>>(&mut self, key: K, value: V) {
        let key_bytes = key.as_ref().as_bytes();
        let val_bytes = value.as_ref().as_bytes();

        // Linear scan is mathematically faster than hashing for < 30 items due to CPU cache
        if let Some(idx) = self.headers.iter().position(|&(np, nl, _, _)| {
            let name = &self.arena[np as usize..(np + nl) as usize];
            name.eq_ignore_ascii_case(key_bytes)
        }) {
            // Header exists: Relocate and append the new value (e.g., "old_val, new_val")
            let (_, _, vp, vl) = self.headers[idx];
            let old_vp = vp as usize;
            let old_vl = vl as usize;

            let new_vp = self.arena.len() as u32;
            self.arena.reserve(old_vl + 2 + val_bytes.len());

            // Safe copy within the same arena
            for j in 0..old_vl {
                self.arena.push(self.arena[old_vp + j]);
            }
            self.arena.extend_from_slice(b", ");
            self.arena.extend_from_slice(val_bytes);

            let new_vl = self.arena.len() as u32 - new_vp;
            self.headers[idx].2 = new_vp;
            self.headers[idx].3 = new_vl;
        } else {
            // New header: Push directly into the arena
            let np = self.arena.len() as u32;
            self.arena.reserve(key_bytes.len() + val_bytes.len());

            for &b in key_bytes {
                self.arena.push(b.to_ascii_lowercase());
            }
            let nl = key_bytes.len() as u32;

            let vp = self.arena.len() as u32;
            self.arena.extend_from_slice(val_bytes);
            let vl = val_bytes.len() as u32;

            self.headers.push((np, nl, vp, vl));
        }
    }

    pub(crate) fn get(&self, key: &str) -> Option<&str> {
        let key_bytes = key.as_bytes();
        for &(np, nl, vp, vl) in &self.headers {
            let name = &self.arena[np as usize..(np + nl) as usize];
            if name.eq_ignore_ascii_case(key_bytes) {
                // Guaranteed valid UTF-8 because we checked during parsing
                return unsafe {
                    std::str::from_utf8_unchecked(&self.arena[vp as usize..(vp + vl) as usize])
                        .into()
                };
            }
        }
        None
    }

    pub(crate) fn remove(&mut self, key: &str) -> Option<String> {
        let key_bytes = key.as_bytes();
        if let Some(idx) = self.headers.iter().position(|&(np, nl, _, _)| {
            let name = &self.arena[np as usize..(np + nl) as usize];
            name.eq_ignore_ascii_case(key_bytes)
        }) {
            let (_, _, vp, vl) = self.headers.remove(idx);
            // We return an owned string only if a user specifically requests the removed value
            return String::from_utf8(self.arena[vp as usize..(vp + vl) as usize].to_vec()).ok();
        }
        None
    }

    pub(crate) fn to_bytes(&self) -> bytes::Bytes {
        let mut total_size = 0;
        for &(_, nl, _, vl) in &self.headers {
            total_size += nl as usize + vl as usize + 4;
        }

        let mut buf = bytes::BytesMut::with_capacity(total_size);
        for &(np, nl, vp, vl) in &self.headers {
            buf.extend_from_slice(&self.arena[np as usize..(np + nl) as usize]);
            buf.extend_from_slice(b": ");
            buf.extend_from_slice(&self.arena[vp as usize..(vp + vl) as usize]);
            buf.extend_from_slice(b"\r\n");
        }
        buf.freeze()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.headers.iter().map(move |&(np, nl, vp, vl)| {
            let k = unsafe {
                std::str::from_utf8_unchecked(&self.arena[np as usize..(np + nl) as usize])
            };
            let v = unsafe {
                std::str::from_utf8_unchecked(&self.arena[vp as usize..(vp + vl) as usize])
            };
            (k, v)
        })
    }
}

impl From<&[u8]> for FieldLines {
    fn from(bytes: &[u8]) -> Self {
        // Keeps the Tokio Future tiny (24 bytes), but pre-allocates the exact heap size needed instantly.
        let mut arena = Vec::with_capacity(bytes.len());
        let mut headers = Vec::with_capacity(16);

        #[inline(always)]
        fn trim_ascii(mut s: &[u8]) -> &[u8] {
            while !s.is_empty() && (s[0] == b' ' || s[0] == b'\t') { s = &s[1..]; }
            while !s.is_empty() && (s[s.len() - 1] == b' ' || s[s.len() - 1] == b'\t' || s[s.len() - 1] == b'\r') { s = &s[..s.len() - 1]; }
            s
        }

        let mut end = bytes.len();
        if end >= 4 && &bytes[end - 4..end] == b"\r\n\r\n" { end -= 4; }
        else if end >= 2 && (&bytes[end - 2..end] == b"\n\n" || &bytes[end - 2..end] == b"\r\n") { end -= 2; }
        else if end >= 1 && &bytes[end - 1..end] == b"\n" { end -= 1; }

        let mut i = 0;
        while i < end {
            let line_end = match memchr::memchr(b'\n', &bytes[i..end]) {
                Some(pos) => i + pos,
                None => end,
            };

            let line = &bytes[i..line_end];
            if line.is_empty() || line == b"\r" {
                i = line_end + 1;
                continue;
            }

            if let Some(colon_pos) = memchr::memchr(b':', line) {
                let name_bytes = trim_ascii(&line[..colon_pos]);
                let val_bytes = trim_ascii(&line[colon_pos + 1..]);

                if !name_bytes.is_empty() && std::str::from_utf8(name_bytes).is_ok() && std::str::from_utf8(val_bytes).is_ok() {
                    if let Some(idx) = headers.iter().position(|&(np, nl, _, _)| {
                        let name = &arena[np as usize..(np + nl) as usize];
                        name.eq_ignore_ascii_case(name_bytes)
                    }) {
                        let (_, _, vp, vl) = headers[idx];
                        let old_vp = vp as usize;
                        let old_vl = vl as usize;

                        let new_vp = arena.len() as u32;

                        // OPTIMIZATION 1: extend_from_within uses ptr::copy_nonoverlapping
                        // to instantly copy memory in bulk! (No more `for` loop).
                        arena.reserve(old_vl + 2 + val_bytes.len());
                        arena.extend_from_within(old_vp..old_vp + old_vl);
                        arena.extend_from_slice(b", ");
                        arena.extend_from_slice(val_bytes);

                        headers[idx].2 = new_vp;
                        headers[idx].3 = arena.len() as u32 - new_vp;
                    } else {
                        let np = arena.len() as u32;

                        // OPTIMIZATION 2: Bulk copy, then SIMD lowercase!
                        // This avoids the byte-by-byte iteration entirely.
                        arena.extend_from_slice(name_bytes);
                        arena[np as usize..].make_ascii_lowercase();

                        let nl = name_bytes.len() as u32;

                        let vp = arena.len() as u32;
                        arena.extend_from_slice(val_bytes);

                        headers.push((np, nl, vp, arena.len() as u32 - vp));
                    }
                }
            }

            i = line_end + 1;
        }

        Self { arena, headers }
    }
}