mod active_set;
mod body;
mod cookie_store;
mod cors;
mod field_lines;
mod request;
mod response;
mod route_definition;
mod server;
mod tls;
mod transport;
pub mod static_files;
mod compress;

pub use request::HttpMethod;
pub use response::HttpStatusCode;
pub use route_definition::{
    MiddlewareHandler, RouteDefinition, RouteDefinitionBuilder, RouteDefinitionError, RouteFactory,
    RouteHandler,
};
pub use server::{HttpCall, HttpServer, HttpServerError};

pub use futures::future::BoxFuture as __future;
pub use inventory::submit as __create_route_factory;

pub use cors::{AllowedOrigin, Cors, CorsBuilder};

pub use cookie_store::{CookieOptions, ExpireExt, SameSite};
pub use tls::{TlsConfig, TlsConfigError};

pub use static_files::{serve_static,StaticFileOptions};

#[cfg(feature = "macros")]
pub use http_macros::{route, static_files};
