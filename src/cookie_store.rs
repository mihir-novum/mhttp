use bytes::{BufMut, Bytes, BytesMut};
use std::collections::HashMap;
use std::time::SystemTime;

#[derive(Debug, Clone)]
pub enum SameSite {
    Strict,
    Lax,
    None,
}

impl SameSite {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            SameSite::Strict => "Strict",
            SameSite::Lax => "Lax",
            SameSite::None => "None",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CookieExpiration {
    Seconds(u64),
    Minutes(u64),
    Hours(u64),
    Days(u64),
    Months(u64),
    Years(u64),
    At(u64),
}

impl CookieExpiration {
    pub(crate) fn as_secs_duration(&self) -> u64 {
        match self {
            CookieExpiration::Seconds(s) => *s,
            CookieExpiration::Minutes(m) => m * 60,
            CookieExpiration::Hours(h) => h * 3_600,
            CookieExpiration::Days(d) => d * 86_400,
            CookieExpiration::Months(m) => m * 2_592_000,
            CookieExpiration::Years(y) => y * 31_536_000,
            CookieExpiration::At(ts) => {
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                if *ts > now { *ts - now } else { 0 }
            }
        }
    }

    pub(crate) fn timestamp(&self) -> u64 {
        if let CookieExpiration::At(ts) = self {
            return *ts;
        }

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        now + self.as_secs_duration()
    }
}

pub trait ExpireExt {
    fn seconds(self) -> CookieExpiration;
    fn minutes(self) -> CookieExpiration;
    fn hours(self) -> CookieExpiration;
    fn days(self) -> CookieExpiration;
    fn months(self) -> CookieExpiration;
    fn years(self) -> CookieExpiration;
}

impl ExpireExt for u64 {
    fn seconds(self) -> CookieExpiration {
        CookieExpiration::Seconds(self)
    }
    fn minutes(self) -> CookieExpiration {
        CookieExpiration::Minutes(self)
    }
    fn hours(self) -> CookieExpiration {
        CookieExpiration::Hours(self)
    }
    fn days(self) -> CookieExpiration {
        CookieExpiration::Days(self)
    }
    fn months(self) -> CookieExpiration {
        CookieExpiration::Months(self)
    }
    fn years(self) -> CookieExpiration {
        CookieExpiration::Years(self)
    }
}

#[derive(Debug, Clone, Default)]
pub struct CookieOptions {
    pub domain: Option<String>,
    pub path: Option<String>,
    pub expires: Option<CookieExpiration>,
    pub max_age: Option<CookieExpiration>,
    pub secure: bool,
    pub http_only: bool,
    pub same_site: Option<SameSite>,
}

#[derive(Debug, Clone)]
pub(crate) struct Cookie {
    pub value: String,
    pub options: Option<CookieOptions>,
}

pub(crate) struct CookieStore {
    map: HashMap<String, Cookie>,
}

impl CookieStore {
    pub(crate) fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub(crate) fn set<K, V>(&mut self, key: K, value: V, options: Option<CookieOptions>)
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.map.insert(
            key.into().trim().into(),
            Cookie {
                value: value.into(),
                options,
            },
        );
    }

    pub(crate) fn get(&self, key: &str) -> Option<&str> {
        self.map.get(key).map(|c| c.value.as_str())
    }

    pub(crate) fn remove(&mut self, key: &str) -> Option<String> {
        self.map.remove(key).map(|c| c.value)
    }

    pub(crate) fn to_bytes(&self) -> Bytes {
        if self.map.is_empty() {
            return Bytes::new();
        }

        let mut buf = BytesMut::new();

        for (cookie_name, cookie) in self.map.iter() {
            buf.put_slice(b"Set-Cookie: ");
            buf.put_slice(cookie_name.as_bytes());
            buf.put_slice(b"=");
            buf.put_slice(cookie.value.as_bytes());

            if let Some(options) = &cookie.options {
                if let Some(domain) = &options.domain {
                    buf.put_slice(b"; Domain=");
                    buf.put_slice(domain.as_bytes());
                }

                if let Some(path) = &options.path {
                    buf.put_slice(b"; Path=");
                    buf.put_slice(path.as_bytes());
                }

                if let Some(expires) = &options.expires {
                    buf.put_slice(b"; Expires=");
                    buf.put_slice(format_http_date(expires.timestamp()).as_bytes());
                }

                if let Some(max_age) = &options.max_age {
                    buf.put_slice(b"; Max-Age=");
                    buf.put_slice(max_age.as_secs_duration().to_string().as_bytes());
                }

                if options.secure {
                    buf.put_slice(b"; Secure");
                }

                if options.http_only {
                    buf.put_slice(b"; HttpOnly");
                }

                if let Some(same_site) = &options.same_site {
                    buf.put_slice(b"; SameSite=");
                    buf.put_slice(same_site.as_str().as_bytes());
                }
            }

            buf.put_slice(b"\r\n");
        }

        buf.freeze()
    }
}

impl From<&[u8]> for CookieStore {
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

        let end = bytes.len();
        let cookie_count = bytes.iter().filter(|b| **b == b';').count() + 1;
        let mut map: HashMap<String, Cookie> = HashMap::with_capacity(cookie_count);

        let mut i: usize = 0;
        while i < end {
            let pair_end = match memchr::memchr(b';', &bytes[i..end]) {
                Some(pos) => i + pos,
                None => end,
            };

            let pair = trim_ascii(&bytes[i..pair_end]);

            if pair.is_empty() {
                i = if pair_end < end {
                    pair_end + 1
                } else {
                    pair_end
                };
                continue;
            }

            let (name_bytes, value_bytes) = match memchr::memchr(b'=', pair) {
                Some(pos) => (trim_ascii(&pair[..pos]), trim_ascii(&pair[pos + 1..])),
                None => (pair, &[0u8; 0][..]),
            };

            if name_bytes.is_empty() {
                i = if pair_end < end {
                    pair_end + 1
                } else {
                    pair_end
                };
                continue;
            }

            let cookie_name = match std::str::from_utf8(name_bytes) {
                Ok(s) => s.to_string(),
                Err(_) => {
                    i = if pair_end < end {
                        pair_end + 1
                    } else {
                        pair_end
                    };
                    continue;
                }
            };

            let cookie_value = match std::str::from_utf8(value_bytes) {
                Ok(s) => s.to_string(),
                Err(_) => {
                    i = if pair_end < end {
                        pair_end + 1
                    } else {
                        pair_end
                    };
                    continue;
                }
            };

            map.insert(
                cookie_name,
                Cookie {
                    value: cookie_value,
                    options: None,
                },
            );

            i = if pair_end < end {
                pair_end + 1
            } else {
                pair_end
            };
        }

        Self { map }
    }
}

pub(crate) fn format_http_date(secs: u64) -> String {
    let days_since_epoch = secs / 86400;
    let time_of_day_secs = secs % 86400;

    let wday = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"][(days_since_epoch % 7) as usize];

    let h = time_of_day_secs / 3600;
    let m = (time_of_day_secs % 3600) / 60;
    let s = time_of_day_secs % 60;

    let jdn = days_since_epoch as i64 + 2440588;
    let j = jdn + 32044;
    let g = j / 146097;
    let dg = j % 146097;
    let c = (dg / 36524 + 1) * 3 / 4;
    let dc = dg - c * 36524;
    let b = dc / 1461;
    let db = dc % 1461;
    let a = (db / 365 + 1) * 3 / 4;
    let da = db - a * 365;
    let y = g * 400 + c * 100 + b * 4 + a;
    let m_month = (da * 5 + 308) / 153 - 2;
    let d = da - (m_month + 4) * 153 / 5 + 122;

    let year = y - 4800 + (m_month + 2) / 12;
    let month = (m_month + 2) % 12 + 1;
    let day = d + 1;

    let month_str = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ][(month - 1) as usize];

    format!(
        "{}, {:02} {} {:04} {:02}:{:02}:{:02} GMT",
        wday, day, month_str, year, h, m, s
    )
}
