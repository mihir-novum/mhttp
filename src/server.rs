use crate::active_set::ActiveSet;
use crate::body::Body;
use crate::cors::Cors;
use crate::request::{HttpRequest, HttpRequestError, PeerAddr};
use crate::response::{HttpResponse, HttpResponseBodyUnInitialized, ResponseHook};
use crate::route_definition::{RouteDefinition, RouteDefinitionError, RouteFactory};
use crate::tls::{TlsConfig, TlsConfigError};
use crate::transport::Transport;
use crate::{HttpMethod, HttpStatusCode};
use serde_json::Value;
use std::collections::HashMap;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use telemetry::{__InstrumentTrait, TelemetryContext};
use tokio::io::{AsyncRead, AsyncWrite, BufReader};
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
}

impl HttpCall {
    async fn parse(stream: Transport, request_id: Uuid) -> Result<Self, HttpRequestError> {
        let mut reader = BufReader::new(stream);
        let request = HttpRequest::parse(&mut reader).await?;
        let http_version = request.http_version().clone();
        Ok(Self {
            response: Some(HttpResponse::new(
                reader.into_inner(),
                http_version,
                request_id,
                Arc::from(request.route()),
            )),
            request,
            extras: HashMap::new(),
            request_id,
        })
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
                    let request_id = Uuid::new_v4();

                    thread_pool.spawn(async move {
                       TelemetryContext::add("request_id", Value::String(request_id.to_string()));

                        match &server.tls {
                            None => {
                                let peer_addr = stream.peer_addr().unwrap();
                                let transport = Transport::new(stream, peer_addr);
                                server.handle_request(transport, request_id).await;
                            },
                            Some(acceptor) => {
                                match acceptor.accept(stream).await{
                                    Ok(tls_stream) => {
                                        let peer_addr = tls_stream.get_ref().0.peer_addr().unwrap();
                                        let transport = Transport::new(tls_stream, peer_addr);
                                        server.handle_request(transport, request_id).await;
                                    },
                                    Err(e) => {
                                        println!("TLS error: {}", e);
                                    }
                                }
                            }
                        }

                    }.instrument(context));
                }
            }
        }
    }

    async fn handle_request(&self, stream: Transport, request_id: Uuid) {
        let mut call: HttpCall = match HttpCall::parse(stream, request_id).await {
            Ok(call) => call,
            Err(_) => {
                return;
            }
        };

        if let Some(cors) = &self.cors {
            if *call.method() == HttpMethod::OPTIONS {
                cors.handle_preflight(&mut call).await;
                return;
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

            call.response = Some(resp.set_on_set(hook))
        }

        let route = match self.routes.iter().find(|route| {
            route.route.is_match(call.request.route()) && &route.method == call.request.method()
        }) {
            Some(route) => route,
            None => return,
        };

        call.request.parse_params(&route.route);

        if !route.middleware.is_empty() {
            for middleware in route.middleware.iter() {
                middleware(&mut call).await;

                if call.response_sent() {
                    return;
                }
            }
        }

        (route.handler)(&mut call).await;
    }
}
