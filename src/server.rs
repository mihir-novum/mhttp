use crate::active_set::ActiveSet;
use crate::body::Body;
use crate::connection::Connection;
use crate::cors::Cors;
use crate::request::{HttpRequest, HttpRequestError};
use crate::response::{
    HttpResponse, HttpResponseBodyUnInitialized, HttpResponseInit, ResponseHook,
};
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

pub struct RestartContext {
    pub active_requests: Vec<RequestInfo>,
}

type RestartHook =
    Box<dyn FnOnce(RestartContext) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

pub(crate) struct RestartRequest {
    pub(crate) force: bool,
    pub(crate) pre_hook: RestartHook,
}

#[derive(Clone)]
pub struct RestartHandle {
    restart_tx: tokio::sync::mpsc::UnboundedSender<RestartRequest>,
}

impl RestartHandle {
    pub fn restart<F, Fut>(&self, force: bool, pre_hook: F)
    where
        F: FnOnce(RestartContext) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let _ = self.restart_tx.send(RestartRequest {
            force,
            pre_hook: Box::new(move |ctx| Box::pin(pre_hook(ctx))),
        });
    }
}

pub struct HttpCall {
    pub(crate) connection: Connection,
    pub(crate) request: HttpRequest,
    extras: HashMap<String, String>,
    request_id: Uuid,
    restart_handle: RestartHandle,
    response_hook: Option<ResponseHook>,
    suppress_body: bool,
}

impl HttpCall {
    async fn parse(
        transport: Transport,
        request_id: Uuid,
        max_body_size: usize,
        restart_handle: RestartHandle,
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
            restart_handle,
            response_hook: None,
            suppress_body: false,
        })
    }

    pub(crate) fn method(&self) -> &HttpMethod {
        self.request.method()
    }

    pub fn restart<F, Fut>(&self, force: bool, pre_hook: F)
    where
        F: FnOnce(RestartContext) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.restart_handle.restart(force, pre_hook);
    }

    pub fn restart_handle(&self) -> RestartHandle {
        self.restart_handle.clone()
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

    pub fn path_param<K: Into<String>>(&self, param_name: K) -> Option<&str> {
        self.request.path_param(param_name)
    }

    pub fn query_param<K: Into<String>>(&self, param_name: K) -> Option<&str> {
        self.request.query_param(param_name)
    }

    pub fn set_extras<K: Into<String>, V: Into<String>>(&mut self, key: K, value: V) {
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

    pub(crate) fn set_response_hook(&mut self, hook: ResponseHook) {
        self.response_hook = Some(hook);
    }

    pub(crate) fn set_suppress_body(&mut self) {
        self.suppress_body = true;
    }

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
}

// ── RequestInfo ────────────────────────────────────────────────────────────

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

// ── HttpServer ─────────────────────────────────────────────────────────────

pub struct HttpServer {
    routes: Vec<RouteDefinition>,
    listener: Option<TcpListener>,
    active_requests: ActiveRequest,
    on_request_complete: Option<OnRequestComplete>,
    cors: Option<Cors>,
    tls: Option<TlsAcceptor>,
    request_timeout: std::time::Duration,
    keep_alive_timeout: std::time::Duration,
    restart_stability_window: std::time::Duration,
    restart_pre_hook_timeout: std::time::Duration,
    max_body_size: usize,
}

impl HttpServer {
    pub fn builder(port: u16) -> HttpServerBuilder {
        HttpServerBuilder::new(port)
    }

    pub fn listen(mut self) -> (RestartHandle, impl Future<Output = ()>) {
        let listener = self.listener.take().expect("Listener must be initialized");
        let server = Arc::new(self);

        let (restart_tx, mut restart_rx) = tokio::sync::mpsc::unbounded_channel::<RestartRequest>();
        let (result_tx, mut result_rx) =
            tokio::sync::mpsc::unbounded_channel::<Result<RestartRequest, ()>>();

        let handle = RestartHandle {
            restart_tx: restart_tx.clone(),
        };

        let fut = async move {
            let mut thread_pool: JoinSet<()> = JoinSet::new();
            let mut restart_in_progress = false;

            let child_pid: Arc<std::sync::Mutex<Option<u32>>> =
                Arc::new(std::sync::Mutex::new(None));

            let connection_handle = RestartHandle {
                restart_tx: restart_tx.clone(),
            };

            // Wrap in Option so we can drop early on restart
            // without affecting the SIGINT/SIGTERM paths
            let mut listener = Some(listener);

            #[cfg(unix)]
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();

            let force: bool = loop {
                tokio::select! {
                    Ok((stream, _)) = async {
                        match listener.as_ref() {
                            Some(l) => l.accept().await,
                            // Listener already dropped (restart path) —
                            // return pending so this arm never fires again
                            None => std::future::pending().await,
                        }
                    } => {
                        let server_clone = server.clone();
                        let restart_handle = connection_handle.clone();
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
                                            let peer_addr =
                                                tls_stream.get_ref().0.peer_addr().unwrap();
                                            Transport::new(tls_stream, peer_addr)
                                        }
                                        Err(e) => {
                                            telemetry::warn!("TLS error: {}", e);
                                            return;
                                        }
                                    }
                                }
                            };

                            server_clone
                                .handle_connection(transport, restart_handle)
                                .await;
                        }.instrument(context));
                    }

                    Some(restart_req) = restart_rx.recv() => {
                        if restart_in_progress {
                            telemetry::info!("Restart already in progress, ignoring duplicate.");
                            continue;
                        }

                        restart_in_progress = true;
                        telemetry::info!("Restart triggered. Force: {}", restart_req.force);

                        let mut current_exe = match std::env::current_exe() {
                            Ok(exe) => exe,
                            Err(e) => {
                                telemetry::warn!("Failed to get executable path: {}", e);
                                restart_in_progress = false;
                                continue;
                            }
                        };

                        if !current_exe.exists() {
                            current_exe =
                                std::path::PathBuf::from(std::env::args().next().unwrap());
                        }

                        let mut child =
                            match std::process::Command::new(&current_exe)
                                .args(std::env::args().skip(1))
                                .spawn()
                            {
                                Ok(c) => c,
                                Err(e) => {
                                    telemetry::warn!("Failed to spawn new process: {}", e);
                                    restart_in_progress = false;
                                    continue;
                                }
                            };

                        *child_pid.lock().unwrap() = Some(child.id());

                        telemetry::info!(
                            "New process spawned (PID {}). Verifying stability for {}s...",
                            child.id(),
                            server.restart_stability_window.as_secs(),
                        );

                        let result_tx = result_tx.clone();
                        let stability_window = server.restart_stability_window;

                        tokio::spawn(async move {
                            tokio::time::sleep(stability_window).await;

                            match child.try_wait() {
                                Ok(Some(status)) => {
                                    telemetry::warn!(
                                        "New instance crashed with status: {}",
                                        status
                                    );
                                    let _ = result_tx.send(Err(()));
                                }
                                Ok(None) => {
                                    telemetry::info!("New instance appears stable.");
                                    let _ = result_tx.send(Ok(restart_req));
                                }
                                Err(e) => {
                                    telemetry::warn!(
                                        "Failed to check new instance status: {}. Aborting restart.",
                                        e
                                    );
                                    let _ = result_tx.send(Err(()));
                                }
                            }
                        });
                    }

                    result = result_rx.recv() => {
                        match result {
                            Some(Ok(restart_req)) => {
                                telemetry::info!("New instance stable. Dropping listener...");

                                // Drop listener — OS immediately stops routing new
                                // connections to this process via SO_REUSEPORT.
                                // New connections go to the new process cleanly.
                                // No client gets a connection reset.
                                drop(listener.take());

                                let ctx = RestartContext {
                                    active_requests: server.active_requests
                                        .snapshot()
                                        .into_values()
                                        .collect(),
                                };

                                telemetry::info!(
                                    "Running pre-hook with {} active requests in flight...",
                                    ctx.active_requests.len()
                                );

                                match tokio::time::timeout(
                                    server.restart_pre_hook_timeout,
                                    (restart_req.pre_hook)(ctx),
                                ).await {
                                    Ok(_) => {
                                        telemetry::info!("Pre-hook complete. Handing over.");
                                    }
                                    Err(_) => {
                                        telemetry::warn!(
                                            "Pre-hook timed out after {}s. Proceeding with handover.",
                                            server.restart_pre_hook_timeout.as_secs()
                                        );
                                    }
                                }

                                break restart_req.force;
                            }
                            Some(Err(_)) => {
                                telemetry::warn!(
                                    "Restart failed. Unlocking for future attempts."
                                );
                                *child_pid.lock().unwrap() = None;
                                restart_in_progress = false;
                            }
                            None => {
                                telemetry::warn!(
                                    "Result channel closed unexpectedly. Unlocking."
                                );
                                *child_pid.lock().unwrap() = None;
                                restart_in_progress = false;
                            }
                        }
                    }

                    _ = tokio::signal::ctrl_c() => {
                        if restart_in_progress {
                            if let Some(pid) = *child_pid.lock().unwrap() {
                                telemetry::warn!(
                                    "SIGINT received during restart. Killing child PID {}.",
                                    pid
                                );
                                #[cfg(unix)]
                                unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL); }
                                #[cfg(not(unix))]
                                {
                                    let _ = std::process::Command::new("taskkill")
                                        .args(["/PID", &pid.to_string(), "/F"])
                                        .spawn();
                                }
                            }
                        }
                        telemetry::info!("SIGINT received. Graceful shutdown.");
                        break false;
                    }

                    _ = async {
                        #[cfg(unix)]
                        sigterm.recv().await;
                        #[cfg(not(unix))]
                        std::future::pending::<()>().await;
                    } => {
                        if restart_in_progress {
                            if let Some(pid) = *child_pid.lock().unwrap() {
                                telemetry::warn!(
                                    "SIGTERM received during restart. Killing child PID {}.",
                                    pid
                                );
                                #[cfg(unix)]
                                unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL); }
                                #[cfg(not(unix))]
                                {
                                    let _ = std::process::Command::new("taskkill")
                                        .args(["/PID", &pid.to_string(), "/F"])
                                        .spawn();
                                }
                            }
                        }
                        telemetry::info!("SIGTERM received. Graceful shutdown.");
                        break false;
                    }
                }
            };

            // Drop listener if still held (SIGINT/SIGTERM paths)
            drop(listener.take());

            if force {
                telemetry::info!("Force shutdown. Aborting active tasks.");
                thread_pool.abort_all();
            } else {
                telemetry::info!("Graceful shutdown. Waiting for active requests.");
                server.active_requests.wait_for_zero().await;
                while thread_pool.join_next().await.is_some() {}
            }

            telemetry::info!("Shutdown complete.");
            std::process::exit(0);
        };

        (handle, fut)
    }

    async fn handle_connection(&self, mut transport: Transport, restart_handle: RestartHandle) {
        loop {
            let request_id = Uuid::new_v4();

            match tokio::time::timeout(
                self.keep_alive_timeout,
                self.handle_single_request(transport, request_id, restart_handle.clone()),
            )
            .await
            {
                Ok(Some(returned)) => transport = returned,
                Ok(None) | Err(_) => break,
            }
        }
    }

    async fn handle_single_request(
        &self,
        transport: Transport,
        request_id: Uuid,
        restart_handle: RestartHandle,
    ) -> Option<Transport> {
        // ── Parse ──────────────────────────────────────────────────────
        let mut call = match tokio::time::timeout(
            self.request_timeout,
            HttpCall::parse(transport, request_id, self.max_body_size, restart_handle),
        )
        .await
        {
            Ok(Ok(call)) => call,
            Ok(Err((stream, err))) => {
                let status = match err {
                    HttpRequestError::PayloadTooLarge => HttpStatusCode::ContentTooLarge,
                    HttpRequestError::RequestLineTooLong => HttpStatusCode::UriTooLong,
                    HttpRequestError::HeadersTooLarge => {
                        HttpStatusCode::RequestHeaderFieldsTooLarge
                    }
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
                        .status_code(status)
                        .empty(),
                    )
                    .await;
                let _ = conn.shutdown().await;
                return None;
            }
            Err(_) => return None,
        };

        // ── Keep-alive ─────────────────────────────────────────────────
        let keep_alive = call
            .header("connection")
            .map(|v| !v.eq_ignore_ascii_case("close"))
            .unwrap_or(true);

        call.connection
            .set_keep_alive(keep_alive, self.keep_alive_timeout.as_secs());

        // ── CORS ───────────────────────────────────────────────────────
        if let Some(cors) = &self.cors {
            if *call.method() == HttpMethod::OPTIONS {
                cors.handle_preflight(&mut call).await;
                return self.finish(call.connection, keep_alive).await;
            }
        }

        // ── Active request tracking ────────────────────────────────────
        let guard = self.active_requests.insert(
            request_id,
            RequestInfo::new(
                request_id,
                call.request.route(),
                call.request.method().clone(),
            ),
        );

        // ── Response hook (telemetry / on_request_complete) ────────────
        {
            let g = guard.clone();
            let on_complete = self.on_request_complete.clone();
            call.set_response_hook(Arc::new(move |status_code| {
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
            }));
        }

        // ── HEAD suppression ───────────────────────────────────────────
        let is_head = *call.method() == HttpMethod::HEAD;
        if is_head {
            call.set_suppress_body();
        }

        // ── Route matching ─────────────────────────────────────────────
        let route = match self.routes.iter().find(|route| {
            let method_match = &route.method == call.request.method()
                || (is_head && route.method == HttpMethod::GET);
            route.route.is_match(call.request.route()) && method_match
        }) {
            Some(route) => route,
            None => {
                let mut allowed_methods = Vec::new();

                for route in &self.routes {
                    if route.route.is_match(call.request.route()) {
                        allowed_methods.push(route.method.to_string());
                    }
                }

                let mut resp = self.apply_cors_to_uninit(
                    &call,
                    HttpResponse::new(
                        call.request.http_version().clone(),
                        request_id,
                        Arc::from(call.request.route()),
                    ),
                );

                if allowed_methods.is_empty() {
                    resp = resp.status_code(HttpStatusCode::NotFound);
                } else {
                    if allowed_methods.iter().any(|m| m == "GET")
                        && !allowed_methods.iter().any(|m| m == "HEAD")
                    {
                        allowed_methods.push("HEAD".to_string());
                    }

                    resp = resp
                        .status_code(HttpStatusCode::MethodNotAllowed)
                        .add_header("allow", allowed_methods.join(", "));
                }

                let _ = call.connection.write_response(resp.empty()).await;

                return self.finish(call.connection, keep_alive).await;
            }
        };

        call.request.parse_params(&route.route);

        // ── Middleware ─────────────────────────────────────────────────
        for middleware in route.middleware.iter() {
            middleware(&mut call).await;
            if call.response_sent() {
                return self.finish(call.connection, keep_alive).await;
            }
        }

        // ── Handler ────────────────────────────────────────────────────
        (route.handler)(&mut call).await;

        self.finish(call.connection, keep_alive).await
    }

    /// Apply CORS headers to an uninit response for non-OPTIONS requests.
    fn apply_cors_to_uninit(
        &self,
        call: &HttpCall,
        resp: HttpResponse<HttpResponseBodyUnInitialized>,
    ) -> HttpResponse<HttpResponseBodyUnInitialized> {
        match &self.cors {
            Some(cors) => cors.add_cors_headers(call, resp),
            None => resp,
        }
    }

    /// Return transport for keep-alive or shut down cleanly.
    async fn finish(&self, mut connection: Connection, keep_alive: bool) -> Option<Transport> {
        if keep_alive {
            Some(connection.into_transport())
        } else {
            let _ = connection.shutdown().await;
            None
        }
    }
}

// ── HttpServerBuilder ──────────────────────────────────────────────────────

pub struct HttpServerBuilder {
    port: u16,
    cors: Option<Cors>,
    tls_config: Option<TlsConfig>,
    request_timeout: std::time::Duration,
    keep_alive_timeout: std::time::Duration,
    max_body_size: usize,
    on_request_complete: Option<OnRequestComplete>,
    routes: Vec<RouteDefinition>,
    restart_stability_window: std::time::Duration,
    restart_pre_hook_timeout: std::time::Duration,
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
            restart_stability_window: std::time::Duration::from_secs(5),
            restart_pre_hook_timeout: std::time::Duration::from_secs(30),
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

    pub fn restart_stability_window(mut self, duration: std::time::Duration) -> Self {
        self.restart_stability_window = duration;
        self
    }

    pub fn restart_pre_hook_timeout(mut self, duration: std::time::Duration) -> Self {
        self.restart_pre_hook_timeout = duration;
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
        #[cfg(target_family = "unix")]
        socket.set_reuse_port(true).unwrap_or(());

        socket
            .bind(&addr.into())
            .map_err(|_| HttpServerError::AddrInUse(self.port))?;
        socket
            .listen(1024)
            .map_err(|_| HttpServerError::AddrInUse(self.port))?;
        socket.set_nonblocking(true).unwrap_or(());

        let std_listener: std::net::TcpListener = socket.into();
        let listener = TcpListener::from_std(std_listener)
            .map_err(|_| HttpServerError::AddrInUse(self.port))?;

        let tls = if let Some(tls_config) = self.tls_config {
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
            restart_stability_window: self.restart_stability_window,
            restart_pre_hook_timeout: self.restart_pre_hook_timeout,
        })
    }
}
