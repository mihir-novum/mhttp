use crate::active_set::ActiveSet;
use crate::body::Body;
use crate::cors::Cors;
use crate::request::{HttpRequest, HttpRequestError, PeerAddr};
use crate::response::{HttpResponse, HttpResponseBodyUnInitialized, ResponseHook};
use crate::route_definition::{RouteDefinition, RouteDefinitionError, RouteFactory};
use crate::tls::{TlsConfig, TlsConfigError};
use crate::transport::Transport;
use crate::{HttpMethod, HttpStatusCode};
use bytes::Bytes;
use serde_json::Value;
use std::collections::HashMap;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use telemetry::{__InstrumentTrait, TelemetryContext, warn};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;
use uuid::Uuid;

#[derive(thiserror::Error, Debug)]
pub enum HttpServerError {
    #[error("address 0.0.0.0:{0} is already in use")]
    AddrInUse(u16),
    #[error("{0}")]
    InvalidRouteDefinition(#[from] RouteDefinitionError),
    #[error("{0}")]
    Tls(#[from] TlsConfigError),
}

pub struct HttpCall {
    request: HttpRequest,
    response: Option<HttpResponse<HttpResponseBodyUnInitialized>>,
    extras: HashMap<String, String>,
    request_id: Uuid,
    transport_slot: Arc<tokio::sync::Mutex<Option<Transport>>>,
}

impl HttpCall {
    async fn parse(
        stream: Transport,
        request_id: Uuid,
        max_body_size: usize,
    ) -> Result<Self, (Transport, HttpRequestError)> {
        let transport_slot: Arc<tokio::sync::Mutex<Option<Transport>>> =
            Arc::new(tokio::sync::Mutex::new(None));

        let mut reader = BufReader::new(stream);
        let request = match HttpRequest::parse(&mut reader, max_body_size).await {
            Ok(request) => request,
            Err(err) => {
                return Err((reader.into_inner(), err));
            }
        };
        let http_version = request.http_version().clone();
        Ok(Self {
            response: Some(HttpResponse::new(
                reader.into_inner(),
                http_version,
                request_id,
                Arc::from(request.route()),
                transport_slot.clone(),
            )),
            request,
            extras: HashMap::new(),
            request_id,
            transport_slot,
        })
    }

    pub(crate) async fn take_transport(&self) -> Option<Transport> {
        self.transport_slot.lock().await.take()
    }

    pub(crate) fn method(&self) -> &HttpMethod {
        self.request.method()
    }

    pub fn client_ipv4_address(&self) -> Option<Ipv4Addr> {
        self.request.client_ipv4_address()
    }

    pub fn client_ipv6_address(&self) -> Option<Ipv6Addr> {
        self.request.client_ipv6_address()
    }

    pub fn host(&self) -> Option<&str> {
        self.request.host()
    }

    pub fn route(&self) -> &str {
        self.request.route()
    }

    pub fn header<S: Into<String>>(&self, name: S) -> Option<&str> {
        self.request.header(name)
    }

    pub fn cookie<S: Into<String>>(&self, name: S) -> Option<&str> {
        self.request.cookie(name)
    }

    pub fn request_id(&self) -> &Uuid {
        &self.request_id
    }

    pub fn body(&self) -> &Body {
        self.request.body()
    }

    pub fn path_param<K>(&self, param_name: K) -> Option<&str>
    where
        K: Into<String>,
    {
        self.request.path_param(param_name)
    }

    pub fn query_param<K>(&self, param_name: K) -> Option<&str>
    where
        K: Into<String>,
    {
        self.request.query_param(param_name)
    }

    pub fn set_extras<K, V>(&mut self, key: K, value: V)
    where
        K: Into<String>,
        V: Into<String>,
    {
        match self
            .extras
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

    pub fn get_extras(&self, key: &str) -> Option<&str> {
        self.extras.get(key).map(|s| s.as_str())
    }

    pub fn remove_extras(&mut self, key: &str) -> Option<String> {
        self.extras.remove(key)
    }

    pub fn response(&mut self) -> HttpResponse<HttpResponseBodyUnInitialized> {
        self.response.take().unwrap()
    }

    pub(crate) fn response_sent(&self) -> bool {
        self.response.is_none()
    }
}

#[derive(Clone, Debug)]
pub struct RequestInfo {
    id: Uuid,
    route: Arc<str>,
    method: HttpMethod,
    response_status: Option<HttpStatusCode>,
    started_at: chrono::DateTime<chrono::Utc>,
    ended_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl RequestInfo {
    fn new(id: Uuid, route: &str, method: HttpMethod) -> Self {
        Self {
            id,
            route: Arc::from(route),
            method,
            response_status: None,
            started_at: chrono::Utc::now(),
            ended_at: None,
        }
    }

    pub fn id(&self) -> &Uuid {
        &self.id
    }

    pub fn method(&self) -> &HttpMethod {
        &self.method
    }

    pub fn route(&self) -> &str {
        self.route.as_ref()
    }

    pub fn response_status(&self) -> Option<HttpStatusCode> {
        self.response_status.clone()
    }

    pub fn started_at(&self) -> chrono::DateTime<chrono::Utc> {
        self.started_at
    }

    pub fn ended_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.ended_at
    }

    fn set_response_status(&mut self, status: HttpStatusCode) {
        self.response_status = Some(status);
    }

    fn mark_as_end(&mut self) {
        self.ended_at = Some(chrono::Utc::now());
    }
}

pub type ActiveRequest = ActiveSet<Uuid, RequestInfo>;

type OnRequestComplete =
    Arc<dyn Fn(RequestInfo) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

pub struct HttpServer {
    routes: Vec<RouteDefinition>,
    listener: TcpListener,
    active_requests: ActiveRequest,
    on_request_complete: Option<OnRequestComplete>,
    cors: Option<Cors>,
    tls: Option<TlsAcceptor>,
    request_timeout: std::time::Duration,
    keep_alive_timeout: std::time::Duration,
    max_body_size: usize,
}

impl HttpServer {
    async fn bind(port: u16, cors: Option<Cors>) -> Result<Self, HttpServerError> {
        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|_| HttpServerError::AddrInUse(port))?;

        Ok(Self {
            routes: inventory::iter::<RouteFactory>()
                .map(|f| (f.factory)())
                .collect(),
            listener,
            active_requests: ActiveRequest::new(),
            on_request_complete: None,
            cors,
            tls: None,
            request_timeout: std::time::Duration::from_secs(60),
            keep_alive_timeout: std::time::Duration::from_secs(75),
            max_body_size: 10 * 1024 * 1024,
        })
    }

    pub async fn new(port: u16, cors: Option<Cors>) -> Result<Self, HttpServerError> {
        Ok(Self {
            tls: None,
            ..Self::bind(port, cors).await?
        })
    }

    pub async fn new_tls(
        port: u16,
        cors: Option<Cors>,
        tls_config: TlsConfig,
    ) -> Result<Self, HttpServerError> {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("failed to install rustls crypto provider");
        Ok(Self {
            tls: Some(tls_config.build()?),
            ..Self::bind(port, cors).await?
        })
    }

    pub fn request_timeout(mut self, duration: std::time::Duration) -> Self {
        self.request_timeout = duration;
        self
    }

    pub fn keep_alive_timeout(mut self, duration: std::time::Duration) -> Self {
        self.keep_alive_timeout = duration;
        self
    }

    pub fn max_body_size(mut self, bytes: usize) -> Self {
        self.max_body_size = bytes;
        self
    }

    pub fn on_request_complete<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(RequestInfo) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_request_complete = Some(Arc::new(move |meta| Box::pin(f(meta))));
        self
    }

    pub fn register_route(&mut self, route_definition: RouteDefinition) {
        self.routes.push(route_definition);
    }

    pub async fn listen(self) {
        let server = Arc::new(self);
        let mut thread_pool: JoinSet<()> = JoinSet::new();

        loop {
            tokio::select! {
                Ok((stream, _)) = server.listener.accept() => {
                    let server = server.clone();
                    let context = telemetry::create_context!("http.request");

                    thread_pool.spawn(async move {
                        let transport = match &server.tls {
                            None => {
                                let peer_addr = stream.peer_addr().unwrap();
                                Transport::new(stream, peer_addr)
                            }
                            Some(acceptor) => {
                                match acceptor.accept(stream).await {
                                    Ok(tls_stream) => {
                                        let peer_addr = tls_stream.get_ref().0.peer_addr().unwrap();
                                        Transport::new(tls_stream, peer_addr)
                                    }
                                    Err(e) => {
                                        warn!("TLS error: {}", e);
                                        return;
                                    }
                                }
                            }
                        };

                        // One task per connection, many requests per connection
                        server.handle_connection(transport).await;
                    }.instrument(context));
                }
            }
        }
    }

    async fn handle_connection(&self, mut transport: Transport) {
        loop {
            let request_id = Uuid::new_v4();

            match tokio::time::timeout(
                self.keep_alive_timeout, // idle timeout between requests
                self.handle_single_request(transport, request_id),
            )
            .await
            {
                Ok(Some(returned_transport)) => {
                    // Client wants keep-alive — loop with same transport
                    transport = returned_transport;
                }
                Ok(None) => {
                    // Connection: close or error — done
                    break;
                }
                Err(_) => {
                    // Client idle too long — close silently
                    break;
                }
            }
        }
    }

    async fn handle_single_request(
        &self,
        stream: Transport,
        request_id: Uuid,
    ) -> Option<Transport> {
        let mut call: HttpCall = match tokio::time::timeout(
            self.request_timeout,
            HttpCall::parse(stream, request_id, self.max_body_size),
        )
        .await
        {
            Ok(Ok(call)) => call,
            Ok(Err((stream, err))) => {
                let status = match err {
                    HttpRequestError::PayloadTooLarge => HttpStatusCode::ContentTooLarge,
                    _ => HttpStatusCode::BadRequest,
                };

                let slot = Arc::new(tokio::sync::Mutex::new(None));

                HttpResponse::new(
                    stream,
                    Bytes::from_static(b"HTTP/1.1"),
                    request_id,
                    Arc::from(""),
                    slot.clone(),
                )
                .status_code(status)
                .send()
                .await;

                if let Some(mut t) = slot.lock().await.take() {
                    let _ = t.shutdown().await;
                }

                return None;
            }
            Err(_) => return None,
        };

        let keep_alive = call
            .header("connection")
            .map(|v| !v.eq_ignore_ascii_case("close"))
            .unwrap_or(true);

        if let Some(resp) = call.response.take() {
            let resp = resp.__add_header_internal(
                "connection",
                if keep_alive { "keep-alive" } else { "close" },
            );
            let resp = if keep_alive {
                resp.__add_header_internal(
                    "keep-alive",
                    format!("timeout={}", self.keep_alive_timeout.as_secs()),
                )
            } else {
                resp
            };
            call.response = Some(resp);
        }

        if let Some(cors) = &self.cors {
            if *call.method() == HttpMethod::OPTIONS {
                cors.handle_preflight(&mut call).await;
                // Get transport back from call after preflight response
                return if keep_alive {
                    call.take_transport().await
                } else {
                    None
                };
            } else if let Some(resp) = call.response.take() {
                let resp_with_cors = cors.add_cors_headers(&call, resp);
                call.response = Some(resp_with_cors);
            }
        }

        let guard = self.active_requests.insert(
            request_id,
            RequestInfo::new(
                request_id,
                call.request.route(),
                call.request.method().clone(),
            ),
        );

        if let Some(resp) = call.response.take() {
            let g = guard.clone();
            let on_complete = self.on_request_complete.clone();
            let hook: ResponseHook = Arc::new(move |status_code| {
                g.update(|info| {
                    info.set_response_status(status_code.clone());
                    info.mark_as_end();
                });
                if let Some(on_request_complete) = &on_complete
                    && let Some(info) = g.value()
                {
                    let fut = on_request_complete({
                        let mut info = info.clone();
                        info.set_response_status(status_code);
                        info
                    });
                    telemetry::spawn!(fut);
                }
            });
            call.response = Some(resp.set_on_set(hook));
        }

        let is_head = *call.method() == HttpMethod::HEAD;
        if is_head {
            if let Some(resp) = call.response.take() {
                call.response = Some(resp.suppress_body());
            }
        }

        let route = match self.routes.iter().find(|route| {
            let method_match = &route.method == call.request.method()
                || (is_head && route.method == HttpMethod::GET);
            route.route.is_match(call.request.route()) && method_match
        }) {
            Some(route) => route,
            None => {
                call.response()
                    .status_code(HttpStatusCode::NotFound)
                    .send()
                    .await;
                return if keep_alive {
                    call.take_transport().await
                } else {
                    None
                };
            }
        };

        call.request.parse_params(&route.route);

        if !route.middleware.is_empty() {
            for middleware in route.middleware.iter() {
                middleware(&mut call).await;
                if call.response_sent() {
                    return if keep_alive {
                        call.take_transport().await
                    } else {
                        None
                    };
                }
            }
        }

        (route.handler)(&mut call).await;

        if keep_alive {
            call.take_transport().await
        } else {
            // Explicitly shut down — client said Connection: close
            if let Some(mut transport) = call.take_transport().await {
                let _ = transport.shutdown().await;
            }
            None
        }
    }
}
