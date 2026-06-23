use crate::response::{HttpResponse, HttpResponseBodyUnInitialized};
use crate::{HttpCall, HttpMethod, HttpStatusCode};
use std::marker::PhantomData;
use std::str::FromStr;

#[derive(Clone, Debug)]
pub enum AllowedOrigin {
    Any,
    Only(Vec<String>),
}

#[derive(Clone, Debug)]
pub struct Cors {
    pub(crate) allowed_origin: AllowedOrigin,
    pub(crate) allowed_methods: Vec<HttpMethod>,
    pub(crate) allowed_headers: Vec<String>,
    pub(crate) exposed_headers: Vec<String>,
    pub(crate) allow_credentials: bool,
    pub(crate) max_age: Option<u64>,
}

impl Cors {
    pub(crate) async fn handle_preflight(&self, call: &mut HttpCall) {
        self.apply_cors(call, true).await;
    }

    pub fn add_cors_headers(
        &self,
        call: &HttpCall,
        resp: HttpResponse<HttpResponseBodyUnInitialized>,
    ) -> HttpResponse<HttpResponseBodyUnInitialized> {
        let origin = call.header("origin");

        let allow_origin = match (&self.allowed_origin, origin) {
            (AllowedOrigin::Any, Some(o)) if self.allow_credentials => o.to_string(),
            (AllowedOrigin::Any, _) => "*".to_string(),
            (AllowedOrigin::Only(list), Some(o))
                if list.iter().any(|allowed| allowed.eq_ignore_ascii_case(o)) =>
            {
                o.to_string()
            }
            _ => return resp,
        };

        let mut response = resp.__add_header_internal("access-control-allow-origin", &allow_origin);

        let mut vary: Vec<&str> = Vec::new();

        if self.allow_credentials {
            response = response.__add_header_internal("access-control-allow-credentials", "true");
            vary.push("Origin");
        } else if !matches!(self.allowed_origin, AllowedOrigin::Any) {
            vary.push("Origin");
        }

        let methods_str = self
            .allowed_methods
            .iter()
            .map(|m| m.to_string())
            .collect::<Vec<_>>()
            .join(",");
        response = response.__add_header_internal("access-control-allow-methods", methods_str);

        if !self.exposed_headers.is_empty() {
            response = response.__add_header_internal(
                "access-control-expose-headers",
                self.exposed_headers.join(","),
            );
        }

        if !vary.is_empty() {
            response = response.__add_header_internal("vary", vary.join(", "));
        }

        response
    }

    async fn apply_cors(&self, call: &mut HttpCall, is_preflight: bool) {
        let origin = call.header("origin");

        let allow_origin = match (&self.allowed_origin, origin) {
            (AllowedOrigin::Any, Some(o)) if self.allow_credentials => o.to_string(),
            (AllowedOrigin::Any, _) => "*".to_string(),
            (AllowedOrigin::Only(list), Some(o))
                if list.iter().any(|allowed| allowed.eq_ignore_ascii_case(o)) =>
            {
                o.to_string()
            }
            _ => {
                if is_preflight {
                    call.response()
                        .status_code(HttpStatusCode::NoContent)
                        .send()
                        .await;
                }
                return;
            }
        };

        if is_preflight {
            let requested_method = match call.header("access-control-request-method") {
                Some(val) => match HttpMethod::from_str(val) {
                    Ok(m) => m,
                    Err(_) => {
                        call.response()
                            .status_code(HttpStatusCode::NoContent)
                            .send()
                            .await;
                        return;
                    }
                },
                None => {
                    call.response()
                        .status_code(HttpStatusCode::NoContent)
                        .send()
                        .await;
                    return;
                }
            };

            if !self.allowed_methods.contains(&requested_method) {
                call.response()
                    .status_code(HttpStatusCode::NoContent)
                    .send()
                    .await;
                return;
            }
        }

        let mut resp = if is_preflight {
            call.response().status_code(HttpStatusCode::NoContent)
        } else {
            call.response()
        };

        resp = resp.__add_header_internal("access-control-allow-origin", &allow_origin);

        let mut vary: Vec<&str> = Vec::new();

        if self.allow_credentials {
            resp = resp.__add_header_internal("access-control-allow-credentials", "true");
            vary.push("Origin");
        } else if !matches!(self.allowed_origin, AllowedOrigin::Any) {
            vary.push("Origin");
        }

        let methods_str = self
            .allowed_methods
            .iter()
            .map(|m| m.to_string())
            .collect::<Vec<_>>()
            .join(",");
        resp = resp.__add_header_internal("access-control-allow-methods", methods_str);

        if is_preflight {
            let request_headers = call
                .header("access-control-request-headers")
                .unwrap_or_default();
            let allow_headers = if self.allowed_headers.is_empty() {
                if request_headers.is_empty() {
                    String::new()
                } else {
                    request_headers.to_string()
                }
            } else if self
                .allowed_headers
                .iter()
                .any(|h| h.eq_ignore_ascii_case("*"))
            {
                request_headers.to_string()
            } else {
                self.allowed_headers.join(",")
            };

            if !allow_headers.is_empty() {
                resp = resp.__add_header_internal("access-control-allow-headers", allow_headers);
                vary.push("Access-Control-Request-Headers");
            }

            if let Some(max_age) = self.max_age {
                resp = resp.__add_header_internal("access-control-max-age", max_age.to_string());
            }
        }

        if !vary.is_empty() {
            resp = resp.__add_header_internal("vary", vary.join(", "));
        }

        if !self.exposed_headers.is_empty() {
            resp = resp.__add_header_internal(
                "access-control-expose-headers",
                self.exposed_headers.join(","),
            );
        }

        resp.send().await;
    }
}

pub struct Any;
pub struct Restricted<T>(PhantomData<T>);

pub struct CorsBuilder<OriginState, MethodState, HeaderState> {
    allowed_origin: AllowedOrigin,
    allowed_methods: Vec<HttpMethod>,
    allowed_headers: Vec<String>,
    exposed_headers: Vec<String>,
    allow_credentials: bool,
    max_age: Option<u64>,
    _origin: PhantomData<OriginState>,
    _method: PhantomData<MethodState>,
    _header: PhantomData<HeaderState>,
}

impl CorsBuilder<Any, Restricted<HttpMethod>, Restricted<String>> {
    pub fn new() -> Self {
        Self {
            allowed_origin: AllowedOrigin::Any,
            allowed_methods: Vec::new(),
            allowed_headers: Vec::new(),
            exposed_headers: Vec::new(),
            allow_credentials: false,
            max_age: None,
            _origin: PhantomData,
            _method: PhantomData,
            _header: PhantomData,
        }
    }
}

impl Default for CorsBuilder<Any, Restricted<HttpMethod>, Restricted<String>> {
    fn default() -> Self {
        Self::new()
    }
}

impl<MethodState, HeaderState> CorsBuilder<Any, MethodState, HeaderState> {
    pub fn allow_any_origin(self) -> Self {
        self
    }

    pub fn only_origins(self) -> CorsBuilder<Restricted<String>, MethodState, HeaderState> {
        CorsBuilder {
            allowed_origin: AllowedOrigin::Only(Vec::new()),
            allowed_methods: self.allowed_methods,
            allowed_headers: self.allowed_headers,
            exposed_headers: self.exposed_headers,
            allow_credentials: self.allow_credentials,
            max_age: self.max_age,
            _origin: PhantomData,
            _method: PhantomData,
            _header: PhantomData,
        }
    }
}

impl<MethodState, HeaderState> CorsBuilder<Restricted<String>, MethodState, HeaderState> {
    pub fn add_origin<S: Into<String>>(mut self, origin: S) -> Self {
        if let AllowedOrigin::Only(origins) = &mut self.allowed_origin {
            origins.push(origin.into());
        }
        self
    }
}

impl<OriginState, HeaderState> CorsBuilder<OriginState, Restricted<HttpMethod>, HeaderState> {
    pub fn add_method(mut self, method: HttpMethod) -> Self {
        self.allowed_methods.push(method);
        self
    }
}

impl<OriginState, MethodState> CorsBuilder<OriginState, MethodState, Restricted<String>> {
    pub fn add_header<S: Into<String>>(mut self, header: S) -> Self {
        self.allowed_headers.push(header.into());
        self
    }
}

impl<OriginState, MethodState, HeaderState> CorsBuilder<OriginState, MethodState, HeaderState> {
    pub fn add_exposed_header<S: Into<String>>(mut self, header: S) -> Self {
        self.exposed_headers.push(header.into());
        self
    }

    pub fn allow_credentials(mut self, value: bool) -> Self {
        self.allow_credentials = value;
        self
    }

    pub fn max_age(mut self, value: u64) -> Self {
        self.max_age = Some(value);
        self
    }

    pub fn build(self) -> Cors {
        Cors {
            allowed_origin: self.allowed_origin,
            allowed_methods: self.allowed_methods,
            allowed_headers: self.allowed_headers,
            exposed_headers: self.exposed_headers,
            allow_credentials: self.allow_credentials,
            max_age: self.max_age,
        }
    }
}
