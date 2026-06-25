use crate::active_set::ActiveSet;
use crate::body::Body;
use crate::connection::Connection;
use crate::cors::Cors;
use crate::request::{HttpRequest, HttpRequestError};
use crate::response::{HttpResponse, HttpResponseInit, ResponseHook};
use crate::route_definition::{RouteDefinition, RouteDefinitionError, RouteFactory};
use crate::tls::{TlsConfig, TlsConfigError};
use crate::transport::Transport;
use crate::{HttpMethod, HttpStatusCode};
use bytes::Bytes;
use socket2::Socket;
use std::collections::HashMap;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use telemetry::__InstrumentTrait;
use tokio::net::TcpListener;
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
    pub(crate) connection: Connection,
    request: HttpRequest,
    extras: HashMap<String, String>,
    request_id: Uuid,
    restart_tx: tokio::sync::mpsc::UnboundedSender<bool>,
    response_hook: Option<ResponseHook>,
    suppress_body: bool,
}

impl HttpCall {
    async fn parse(
        transport: Transport,
        request_id: Uuid,
        max_body_size: usize,
        restart_tx: tokio::sync::mpsc::UnboundedSender<bool>,
    ) -> Result<Self, (Transport, HttpRequestError)> {
        let mut connection = Connection::new(transport);

        let request = match connection.read_request(max_body_size).await {
            Ok(r) => r,
            Err(e) => return Err((connection.into_transport(), e)),
        };

        Ok(Self {
            connection,
            request,
            extras: HashMap::new(),
            request_id,
            restart_tx,
            response_hook: None,
            suppress_body: false,
        })
    }

    pub(crate) fn method(&self) -> &HttpMethod {
        self.request.method()
    }

    pub fn restart(&self, force: bool) {
        let _ = self.restart_tx.send(force);
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

    pub(crate) fn response_sent(&self) -> bool {
        self.connection.has_written_response()
    }
}

impl HttpCall {
    pub fn response(&mut self) -> HttpResponseInit<'_> {
        let mut response = HttpResponse::new(
            self.request.http_version().clone(),
            self.request_id,
            Arc::from(self.request.route()),
        );

        if let Some(hook) = self.response_hook.take() {
            response = response.set_on_set(hook);
        }

        if self.suppress_body {
            response = response.suppress_body();
        }

        HttpResponseInit {
            connection: &mut self.connection,
            response,
        }
    }

    pub(crate) fn set_response_hook(&mut self, hook: ResponseHook) {
        self.response_hook = Some(hook);
    }

    pub(crate) fn set_suppress_body(&mut self) {
        self.suppress_body = true;
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
    listener: Option<TcpListener>,
    active_requests: ActiveRequest,
    on_request_complete: Option<OnRequestComplete>,
    cors: Option<Cors>,
    tls: Option<TlsAcceptor>,
    request_timeout: std::time::Duration,
    keep_alive_timeout: std::time::Duration,
    max_body_size: usize,
}

impl HttpServer {
    pub fn builder(port: u16) -> HttpServerBuilder {
        HttpServerBuilder::new(port)
    }

    pub async fn listen(mut self) {
        let listener = self.listener.take().expect("Listener must be initialized");
        let server = Arc::new(self);

        // Channel for programmatic restart triggers (from your route handlers)
        let (restart_tx, mut restart_rx) = tokio::sync::mpsc::unbounded_channel::<bool>();

        // Channel for the background task to report if the new process lived or died
        let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel::<Result<bool, ()>>();

        let mut thread_pool: JoinSet<()> = JoinSet::new();

        // Lock to prevent multiple spawn attempts running at the exact same time
        let mut restart_in_progress = false;

        // Docker SIGTERM listener
        #[cfg(unix)]
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();

        // Loop-as-expression: break produces the `force` bool directly, no Option needed.
        let force: bool = loop {
            tokio::select! {
                // 1. Keep accepting connections seamlessly!
                Ok((stream, _)) = listener.accept() => {
                    let server_clone = server.clone();
                    let restart_tx = restart_tx.clone();
                    let context = telemetry::create_context!("http.request");

                    thread_pool.spawn(async move {
                        let transport = match &server_clone.tls {
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
                                        println!("TLS error: {}", e);
                                        return;
                                    }
                                }
                            }
                        };

                        // One task per connection, many requests per connection
                        server_clone.handle_connection(transport, restart_tx).await;
                    }.instrument(context));
                }

                // 2. A restart is triggered from a route handler
                Some(force) = restart_rx.recv() => {
                    // Check the lock: if we are already restarting, discard duplicate requests
                    if restart_in_progress {
                        println!("Restart already in progress. Ignoring duplicate request.");
                        continue;
                    }

                    restart_in_progress = true; // Lock it!
                    println!("Program restart triggered! Force: {}", force);

                    let mut current_exe = match std::env::current_exe() {
                        Ok(exe) => exe,
                        Err(e) => {
                            println!("Failed to get executable path: {}", e);
                            restart_in_progress = false; // Unlock on failure
                            continue;
                        }
                    };

                    if !current_exe.exists() {
                        let argv0 = std::env::args().next().unwrap();
                        current_exe = std::path::PathBuf::from(argv0);
                    }

                    println!("Spawning new process: {}", current_exe.display());

                    let mut child = match std::process::Command::new(current_exe)
                        .args(std::env::args().skip(1))
                        .spawn()
                    {
                        Ok(c) => c,
                        Err(e) => {
                            println!("Failed to spawn new process: {}", e);
                            restart_in_progress = false; // Unlock on failure
                            continue;
                        }
                    };

                    telemetry::info!("New process spawned (PID {}). Verifying stability in background...", child.id());

                    let result_tx = result_tx.clone();

                    // BACKGROUND stability check (does not block accept loop!)
                    tokio::spawn(async move {
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

                        if let Ok(Some(status)) = child.try_wait() {
                            println!("New instance crashed with status: {}.", status);
                            let _ = result_tx.send(Err(())); // Send Failure
                        } else {
                            let _ = result_tx.send(Ok(force)); // Send Success
                        }
                    });
                }

                // 3. Background stability check result received
                Some(result) = result_rx.recv() => {
                    match result {
                        Ok(force) => {
                            println!("New instance is stable. Old instance handing over traffic.");
                            break force; // Loop produces the force value
                        }
                        Err(_) => {
                            println!("Restart failed. Unlocking for future restart attempts.");
                            // Unlock so developers can try fixing the code/config and hit /restart again!
                            restart_in_progress = false;
                        }
                    }
                }

                // 4. Docker / Manual SIGINT (Ctrl+C)
                _ = tokio::signal::ctrl_c() => {
                    println!("Received Ctrl+C (SIGINT). Initiating graceful shutdown...");
                    break false; // Always graceful for OS signals
                }

                // 5. Docker / Kubernetes SIGTERM
                _ = async {
                        #[cfg(unix)]
                        sigterm.recv().await;
                        #[cfg(not(unix))]
                        std::future::pending::<()>().await;
                    } => {
                        println!("Received SIGTERM from Docker/Kubernetes. Initiating graceful shutdown...");
                        break false; // Always graceful for OS signals
                    }
            }
        };

        // --- Server Shutdown Phase ---
        // Drop the listener. In Docker, the Load Balancer spots this.
        // In Bare-Metal, the OS instantly reroutes 100% of new traffic to the new PID.
        drop(listener);

        if force {
            println!("Force flag provided. Aborting active tasks...");
            thread_pool.abort_all();
        } else {
            println!("Graceful shutdown. Waiting for active tasks to finish...");
            // Uses the `Notify` ActiveSet implementation
            server.active_requests.wait_for_zero().await;
        }

        println!("Old instance shutdown complete.");
        std::process::exit(0);
    }

    async fn handle_connection(
        &self,
        mut transport: Transport,
        restart_tx: tokio::sync::mpsc::UnboundedSender<bool>,
    ) {
        loop {
            let request_id = Uuid::new_v4();

            match tokio::time::timeout(
                self.keep_alive_timeout,
                self.handle_single_request(transport, request_id, restart_tx.clone()), // Pass tx
            )
            .await
            {
                Ok(Some(returned_transport)) => transport = returned_transport,
                Ok(None) | Err(_) => break,
            }
        }
    }

    async fn handle_single_request(
        &self,
        transport: Transport,
        request_id: Uuid,
        restart_tx: tokio::sync::mpsc::UnboundedSender<bool>,
    ) -> Option<Transport> {
        let mut call: HttpCall = match tokio::time::timeout(
            self.request_timeout,
            HttpCall::parse(transport, request_id, self.max_body_size, restart_tx),
        )
        .await
        {
            Ok(Ok(call)) => call,
            Ok(Err((stream, err))) => {
                let status = match err {
                    HttpRequestError::PayloadTooLarge => HttpStatusCode::ContentTooLarge,
                    _ => HttpStatusCode::BadRequest,
                };

                let mut conn = Connection::new(stream);
                let _ = conn
                    .write_response(
                        HttpResponse::new(
                            Bytes::from_static(b"HTTP/1.1"),
                            request_id,
                            Arc::from(""),
                        )
                        .status_code(status),
                    )
                    .await;
                let _ = conn.shutdown().await;

                return None;
            }
            Err(_) => return None,
        };

        let keep_alive = call
            .header("connection")
            .map(|v| !v.eq_ignore_ascii_case("close"))
            .unwrap_or(true);

        if let Some(cors) = &self.cors {
            if *call.method() == HttpMethod::OPTIONS {
                cors.handle_preflight(&mut call).await;
                return if keep_alive {
                    Some(call.connection.into_transport())
                } else {
                    let _ = call.connection.shutdown().await;
                    None
                };
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

        {
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
            call.set_response_hook(hook);
        }

        let is_head = *call.method() == HttpMethod::HEAD;

        if is_head {
            call.set_suppress_body();
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
                    .empty()
                    .send()
                    .await;

                return if keep_alive {
                    Some(call.connection.into_transport())
                } else {
                    let _ = call.connection.shutdown().await;
                    None
                };
            }
        };

        call.request.parse_params(&route.route);

        for middleware in route.middleware.iter() {
            middleware(&mut call).await;
            if call.response_sent() {
                return if keep_alive {
                    Some(call.connection.into_transport())
                } else {
                    let _ = call.connection.shutdown().await;
                    None
                };
            }
        }

        (route.handler)(&mut call).await;

        if keep_alive {
            Some(call.connection.into_transport())
        } else {
            let _ = call.connection.shutdown().await;
            None
        }
    }
}

pub struct HttpServerBuilder {
    port: u16,
    cors: Option<Cors>,
    tls_config: Option<TlsConfig>,
    request_timeout: std::time::Duration,
    keep_alive_timeout: std::time::Duration,
    max_body_size: usize,
    on_request_complete: Option<OnRequestComplete>,
    routes: Vec<RouteDefinition>,
}

impl HttpServerBuilder {
    pub fn new(port: u16) -> Self {
        Self {
            port,
            cors: None,
            tls_config: None,
            request_timeout: std::time::Duration::from_secs(60),
            keep_alive_timeout: std::time::Duration::from_secs(75),
            max_body_size: 10 * 1024 * 1024,
            on_request_complete: None,
            routes: inventory::iter::<RouteFactory>()
                .map(|f| (f.factory)())
                .collect(),
        }
    }

    pub fn cors(mut self, cors: Cors) -> Self {
        self.cors = Some(cors);
        self
    }

    pub fn tls(mut self, tls_config: TlsConfig) -> Self {
        self.tls_config = Some(tls_config);
        self
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

    pub fn route(mut self, route_definition: RouteDefinition) -> Self {
        self.routes.push(route_definition);
        self
    }

    pub async fn build(self) -> Result<HttpServer, HttpServerError> {
        let addr = SocketAddr::from(([0, 0, 0, 0], self.port));
        let domain = if addr.is_ipv6() {
            socket2::Domain::IPV6
        } else {
            socket2::Domain::IPV4
        };

        let socket = Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))
            .map_err(|_| HttpServerError::AddrInUse(self.port))?;

        socket.set_reuse_address(true).unwrap_or(());
        #[cfg(any(target_family = "unix"))]
        socket.set_reuse_port(true).unwrap_or(());

        socket
            .bind(&addr.into())
            .map_err(|_| HttpServerError::AddrInUse(self.port))?;
        socket
            .listen(1024)
            .map_err(|_| HttpServerError::AddrInUse(self.port))?;
        socket.set_nonblocking(true).unwrap_or(());

        let std_listener: std::net::TcpListener = socket.into();
        let listener = tokio::net::TcpListener::from_std(std_listener)
            .map_err(|_| HttpServerError::AddrInUse(self.port))?;

        let tls = if let Some(tls_config) = self.tls_config {
            // Use ok() so it doesn't panic if called multiple times in the same application
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
            Some(tls_config.build()?)
        } else {
            None
        };

        Ok(HttpServer {
            routes: self.routes,
            listener: Some(listener),
            active_requests: ActiveRequest::new(),
            on_request_complete: self.on_request_complete,
            cors: self.cors,
            tls,
            request_timeout: self.request_timeout,
            keep_alive_timeout: self.keep_alive_timeout,
            max_body_size: self.max_body_size,
        })
    }
}
