use crate::request::HttpMethod;
use crate::server::HttpCall;
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum RouteDefinitionError {
    #[error("invalid route pattern: {0}")]
    InvalidPattern(String),
}

pub type RouteHandler =
    Arc<dyn for<'r> Fn(&'r mut HttpCall) -> futures::future::BoxFuture<'r, ()> + Send + Sync>;

pub type MiddlewareHandler =
    Arc<dyn for<'r> Fn(&'r mut HttpCall) -> futures::future::BoxFuture<'r, ()> + Send + Sync>;

pub struct RouteDefinition {
    pub pattern: Arc<str>,
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

        // Validate the route syntax at build-time
        Self::validate_route(&route_str).map_err(RouteDefinitionError::InvalidPattern)?;

        Ok(Self {
            pattern: Arc::from(route_str),
            method,
            middleware: Vec::from(middleware.into_boxed_slice()),
            handler,
        })
    }

    /// Validates route patterns (e.g. proper braces, valid parameter names)
    fn validate_route(route: &str) -> Result<(), String> {
        if !route.starts_with('/') {
            return Err("route must start with '/'".into());
        }

        let mut in_param = false;
        let mut param_start = 0;
        let mut param_name = String::new();

        for (i, ch) in route.char_indices() {
            match ch {
                '{' => {
                    if in_param {
                        return Err(format!("unexpected '{{' at byte index {}", i));
                    }
                    in_param = true;
                    param_start = i;
                    param_name.clear();
                }
                '}' => {
                    if !in_param {
                        return Err(format!("unexpected '}}' at byte index {}", i));
                    }
                    in_param = false;

                    if param_name.is_empty() {
                        return Err(format!(
                            "empty parameter name at byte index {}",
                            param_start
                        ));
                    }

                    if !Self::is_valid_param_name(&param_name) {
                        return Err(format!(
                            "invalid parameter name '{param_name}' (use [A-Za-z_][A-Za-z0-9_]*)"
                        ));
                    }
                }
                _ => {
                    if in_param {
                        param_name.push(ch);
                    }
                }
            }
        }

        if in_param {
            return Err("unclosed '{' in route pattern".into());
        }

        // Ensure the {__path__} catch-all only appears at the very end
        if let Some(idx) = route.find("{__path__}") {
            let tail = &route[idx + 10..];
            if !tail.is_empty() && tail != "/" {
                return Err("catch-all {__path__} must be at the end of the route".into());
            }
        }

        Ok(())
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
