use crate::request::{HttpMethod, PeerAddr};
use crate::server::HttpCall;
use regex::Regex;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

#[derive(thiserror::Error, Debug)]
pub enum RouteDefinitionError {
    #[error("regex {0} is invalid")]
    InvalidRegex(String),
}

pub type RouteHandler =
    Arc<dyn for<'r> Fn(&'r mut HttpCall) -> futures::future::BoxFuture<'r, ()> + Send + Sync>;

pub type MiddlewareHandler =
    Arc<dyn for<'r> Fn(&'r mut HttpCall) -> futures::future::BoxFuture<'r, ()> + Send + Sync>;

pub struct RouteDefinition {
    pub route: Regex,
    pub method: HttpMethod,
    pub handler: RouteHandler,
    pub middleware: Vec<MiddlewareHandler>,
}

impl RouteDefinition {
    pub fn new<S>(
        method: HttpMethod,
        route: S,
        handler: RouteHandler,
        middleware: Vec<MiddlewareHandler>,
    ) -> Result<Self, RouteDefinitionError>
    where
        S: Into<String>,
    {
        let route_str = route.into();
        let route_regex =
            Self::parse_route(&route_str).map_err(RouteDefinitionError::InvalidRegex)?;
        Ok(Self {
            route: route_regex,
            method,
            middleware: Vec::from(middleware.into_boxed_slice()),
            handler,
        })
    }

    fn parse_route(route: &str) -> Result<Regex, String> {
        if !route.starts_with('/') {
            return Err("route must start with '/'".into());
        }

        let mut re = String::with_capacity(route.len() * 2);
        re.push('^');

        let mut it = route.char_indices().peekable();
        while let Some((i, ch)) = it.next() {
            match ch {
                '{' => {
                    let start = i;
                    let mut name = String::new();
                    for (_, c) in it.by_ref() {
                        if c == '}' {
                            break;
                        }
                        name.push(c);
                    }
                    if name.is_empty() {
                        return Err(format!("empty parameter name at byte index {}", start));
                    }
                    if !Self::is_valid_param_name(&name) {
                        return Err(format!(
                            "invalid parameter name '{name}' (use [A-Za-z_][A-Za-z0-9_]*)"
                        ));
                    }
                    re.push_str("(?P<");
                    re.push_str(&name);
                    if name == "__path__" {
                        re.push_str(">[^?#]*)");
                    } else {
                        re.push_str(">[^/?#]+)");
                    }
                }
                '}' => return Err(format!("unexpected '}}' at byte index {}", i)),
                c => {
                    if Self::is_regex_meta(c) {
                        re.push('\\');
                    }
                    re.push(c);
                }
            }
        }

        if route != "/" {
            if re.ends_with('/') {
                re.pop();
            }

            if !re.ends_with(">[^?#]*)") {
                re.push_str("/?");
            }
        }

        re.push_str("(?:\\?[^#\\s]*)?(?:#[^\\s]*)?");

        re.push('$');
        Regex::new(&re).map_err(|e| e.to_string())
    }

    #[inline(always)]
    fn is_valid_param_name(s: &str) -> bool {
        let mut it = s.chars();
        match it.next() {
            Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
            _ => return false,
        }
        it.all(|c| c.is_ascii_alphanumeric() || c == '_')
    }

    #[inline(always)]
    fn is_regex_meta(c: char) -> bool {
        matches!(
            c,
            '\\' | '.' | '+' | '*' | '?' | '^' | '$' | '(' | ')' | '[' | ']' | '{' | '}' | '|'
        )
    }
}

pub struct RouteHandlerMissing;
pub struct RouteHandlerPresent;

pub struct RouteDefinitionBuilder<State> {
    route: String,
    method: HttpMethod,
    middleware: Vec<MiddlewareHandler>,
    handler: Option<RouteHandler>,
    _state: std::marker::PhantomData<State>,
}

impl RouteDefinitionBuilder<RouteHandlerMissing> {
    pub fn new<S>(route: S) -> Self
    where
        S: Into<String>,
    {
        Self {
            route: route.into(),
            method: HttpMethod::GET,
            middleware: Vec::new(),
            handler: None,
            _state: std::marker::PhantomData,
        }
    }

    pub fn handler(self, h: RouteHandler) -> RouteDefinitionBuilder<RouteHandlerPresent> {
        RouteDefinitionBuilder {
            route: self.route,
            method: self.method,
            middleware: self.middleware,
            handler: Some(h),
            _state: std::marker::PhantomData,
        }
    }
}

impl<State> RouteDefinitionBuilder<State> {
    pub fn method(mut self, method: HttpMethod) -> Self {
        self.method = method;
        self
    }

    pub fn middleware(mut self, m: MiddlewareHandler) -> Self {
        self.middleware.push(m);
        self
    }
}

impl RouteDefinitionBuilder<RouteHandlerPresent> {
    pub fn build(self) -> Result<RouteDefinition, RouteDefinitionError> {
        RouteDefinition::new(
            self.method,
            self.route,
            self.handler.unwrap(),
            self.middleware,
        )
    }
}

pub struct RouteFactory {
    pub factory: fn() -> RouteDefinition,
}

inventory::collect!(RouteFactory);
