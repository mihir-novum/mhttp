use crate::active_set::ActiveSet;
use crate::body::Body;
use crate::connection::Connection;
use crate::cors::Cors;
use crate::request::{BodyState, HttpParam, HttpRequest, HttpRequestError};
use crate::response::{
    HttpResponse, HttpResponseBodyUnInitialized, HttpResponseInit, ResponseHook,
};
use crate::route_definition::{RouteDefinition, RouteDefinitionError, RouteFactory};
use crate::router::Router;
use crate::tls::{TlsConfig, TlsConfigError};
use crate::transport::Transport;
use crate::{HttpMethod, HttpStatusCode};
use bytes::Bytes;
use socket2::Socket;
use std::collections::HashMap;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
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
        keep_alive_timeout: std::time::Duration,
        request_timeout: std::time::Duration,
        restart_handle: RestartHandle,
    ) -> Result<Option<Self>, (Transport, HttpRequestError)> {
        let mut connection = Connection::new(transport);

        let request = match connection
            .read_request(max_body_size, keep_alive_timeout, request_timeout)
            .await
        {
            Ok(Some(r)) => r,
            Ok(None) => return Ok(None), // Client disconnected cleanly while idle
            Err(e) => return Err((connection.into_transport(), e)),
        };

        Ok(Some(Self {
            connection,
            request,
            extras: HashMap::new(),
            request_id,
            restart_handle,
            response_hook: None,
            suppress_body: false,
        }))
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

    pub fn header(&self, name: &str) -> Option<&str> {
        self.request.header(name)
    }

    pub fn cookie(&self, name: &str) -> Option<&str> {
        self.request.cookie(name)
    }

    pub fn request_id(&self) -> &Uuid {
        &self.request_id
    }

    pub async fn body(&mut self) -> Result<&Body, String> {
        // If it's already read, return it instantly
        if matches!(self.request.body_state, BodyState::Read(_)) {
            if let BodyState::Read(ref b) = self.request.body_state {
                return Ok(b);
            }
        }

        // Steal the state so we can mutate it
        let state = std::mem::replace(&mut self.request.body_state, BodyState::Reading);

        let (content_length, is_chunked) = match state {
            BodyState::Unread {
                content_length,
                is_chunked,
            } => (content_length, is_chunked),
            _ => return Err("Body already read or error occurred".into()),
        };

        // 100-Continue handling (Lazy Evaluation!)
        if self
            .header("expect")
            .map(|v| v.eq_ignore_ascii_case("100-continue"))
            .unwrap_or(false)
        {
            let _ = self
                .connection
                .writer
                .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
                .await;
            let _ = self.connection.writer.flush().await;
        }

        let content_type = self.header("content-type").map(|s| s.to_string());

        let body = if is_chunked {
            Body::read_chunked(
                &mut self.connection,
                self.request.max_body_size,
                content_type,
            )
            .await?
        } else {
            Body::read_exact(
                &mut self.connection,
                content_length,
                self.request.max_body_size,
                content_type,
            )
            .await?
        };

        self.request.body_state = BodyState::Read(body);

        if let BodyState::Read(ref b) = self.request.body_state {
            Ok(b)
        } else {
            unreachable!()
        }
    }

    pub fn path_param(&self, param_name: &str) -> Option<&str> {
        self.request
            .path_params
            .iter()
            .find(|(k, _)| k.as_ref() == param_name)
            .map(|(_, v)| v.as_ref())
    }

    pub fn query_param(&self, param_name: &str) -> Option<&str> {
        let params = self
            .request
            .query_params
            .get_or_init(|| HttpParam::parse_query_params(&self.request.route));

        params
            .iter()
            .find(|(k, _)| k.as_ref() == param_name)
            .map(|(_, v)| v.as_ref())
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
    router: Router,
    listeners: Vec<TcpListener>,
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
        let listeners = std::mem::take(&mut self.listeners);
        let server = Arc::new(self);

        let (restart_tx, mut restart_rx) = tokio::sync::mpsc::unbounded_channel::<RestartRequest>();
        let (result_tx, mut result_rx) =
            tokio::sync::mpsc::unbounded_channel::<Result<RestartRequest, ()>>();

        let handle = RestartHandle {
            restart_tx: restart_tx.clone(),
        };

        let fut = async move {
            let mut restart_in_progress = false;

            let child_pid: Arc<std::sync::Mutex<Option<u32>>> =
                Arc::new(std::sync::Mutex::new(None));

            let connection_handle = RestartHandle {
                restart_tx: restart_tx.clone(),
            };

            #[cfg(unix)]
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();

            // ── Multi-Listener Accept Pool ──────────────────────────────────────────
            //
            // One task per SO_REUSEPORT socket (= one per CPU core).
            // Each task accepts and immediately spawns a free connection task —
            // NO intermediate channel, NO cross-task wakeup, NO serialisation.
            //
            // Hot path per connection: listener.accept() → tokio::spawn()
            // That's it. The control-plane select below never touches this path.
            let mut accept_pool: JoinSet<()> = JoinSet::new();

            for listener in listeners {
                let server_clone = server.clone();
                let restart_handle = connection_handle.clone();

                accept_pool.spawn(async move {
                    loop {
                        match listener.accept().await {
                            Ok((stream, _)) => {
                                let srv = server_clone.clone();
                                let rh = restart_handle.clone();

                                // Spawn and forget — the connection task is fully
                                // independent. Active-request tracking inside
                                // handle_single_request handles graceful drain.
                                tokio::spawn(async move {
                                    let transport = match &srv.tls {
                                        None => {
                                            let peer_addr = match stream.peer_addr() {
                                                Ok(a) => a,
                                                Err(_) => return, // already closed
                                            };
                                            Transport::new(stream, peer_addr)
                                        }
                                        Some(acceptor) => {
                                            match acceptor.accept(stream).await {
                                                Ok(tls_stream) => {
                                                    let peer_addr =
                                                        match tls_stream.get_ref().0.peer_addr() {
                                                            Ok(a) => a,
                                                            Err(_) => return,
                                                        };
                                                    Transport::new(tls_stream, peer_addr)
                                                }
                                                Err(_) => return, // TLS handshake failed
                                            }
                                        }
                                    };

                                    srv.handle_connection(transport, rh).await;
                                });
                            }
                            Err(_) => {
                                // Brief yield on EMFILE/ENFILE to avoid a busy-loop
                                // when the process hits its file-descriptor limit.
                                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                            }
                        }
                    }
                });
            }

            // ── Control Plane ───────────────────────────────────────────────────────
            //
            // This select loop is entirely off the hot path. It only wakes on
            // infrequent events: restart requests, OS signals, child-process results.
            // Connection throughput is completely unaffected by what happens here.
            let force: bool = loop {
                tokio::select! {
                    Some(restart_req) = restart_rx.recv() => {
                        if restart_in_progress {
                            continue;
                        }

                        restart_in_progress = true;

                        let mut current_exe = match std::env::current_exe() {
                            Ok(exe) => exe,
                            Err(_) => {
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
                                Err(_) => {
                                    restart_in_progress = false;
                                    continue;
                                }
                            };

                        *child_pid.lock().unwrap() = Some(child.id());

                        let result_tx = result_tx.clone();
                        let stability_window = server.restart_stability_window;

                        tokio::spawn(async move {
                            tokio::time::sleep(stability_window).await;

                            match child.try_wait() {
                                Ok(Some(_)) => { let _ = result_tx.send(Err(())); }
                                Ok(None)    => { let _ = result_tx.send(Ok(restart_req)); }
                                Err(_)      => { let _ = result_tx.send(Err(())); }
                            }
                        });
                    }

                    result = result_rx.recv() => {
                        match result {
                            Some(Ok(restart_req)) => {
                                // Stop accepting — SO_REUSEPORT immediately routes
                                // new connections to the new process. In-flight
                                // requests continue on the free connection tasks.
                                accept_pool.abort_all();

                                let ctx = RestartContext {
                                    active_requests: server.active_requests
                                        .snapshot()
                                        .into_values()
                                        .collect(),
                                };

                                match tokio::time::timeout(
                                    server.restart_pre_hook_timeout,
                                    (restart_req.pre_hook)(ctx),
                                ).await {
                                    Ok(_) => {}
                                    Err(_) => {}
                                }

                                break restart_req.force;
                            }
                            Some(Err(_)) | None => {
                                *child_pid.lock().unwrap() = None;
                                restart_in_progress = false;
                            }
                        }
                    }

                    _ = tokio::signal::ctrl_c() => {
                        if restart_in_progress {
                            if let Some(pid) = *child_pid.lock().unwrap() {
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
                        accept_pool.abort_all();
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
                        accept_pool.abort_all();
                        break false;
                    }
                }
            };

            // ── Drain & Exit ────────────────────────────────────────────────────────
            //
            // force=true  → exit immediately; OS kills every task and connection.
            // force=false → wait until every active request completes, then exit.
            //               Idle keep-alive connections are terminated by the OS on
            //               exit, matching standard graceful-restart semantics.
            if !force {
                server.active_requests.wait_for_zero().await;
            }

            std::process::exit(0);
        };

        (handle, fut)
    }

    async fn handle_connection(&self, mut transport: Transport, restart_handle: RestartHandle) {
        loop {
            let request_id = Uuid::new_v4();

            match self
                .handle_single_request(transport, request_id, restart_handle.clone())
                .await
            {
                Some(returned) => transport = returned,
                None => break,
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
        let mut call: HttpCall = match HttpCall::parse(
            transport,
            request_id,
            self.max_body_size,
            self.keep_alive_timeout,
            self.request_timeout,
            restart_handle,
        )
        .await
        {
            Ok(Some(call)) => call,
            Ok(None) => {
                // Client sat idle for 75s and disconnected gracefully.
                return None;
            }
            Err((stream, err)) => {
                // Parse failed (Bad Request, Payload Too Large, Slowloris)
                let status = match err {
                    HttpRequestError::PayloadTooLarge => HttpStatusCode::ContentTooLarge,
                    HttpRequestError::RequestLineTooLong => HttpStatusCode::UriTooLong,
                    HttpRequestError::HeadersTooLarge => {
                        HttpStatusCode::RequestHeaderFieldsTooLarge
                    }
                    HttpRequestError::Timeout => HttpStatusCode::RequestTimeout,
                    _ => HttpStatusCode::BadRequest,
                };
                let mut conn = Connection::new(stream);
                conn.set_keep_alive(false, 0);

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
            if self.on_request_complete.is_some() {
                let g = guard.clone();
                let on_complete = self.on_request_complete.clone();
                call.set_response_hook(Arc::new(move |status_code| {
                    g.update(|info| {
                        info.set_response_status(status_code.clone());
                        info.mark_as_end();
                    });
                    if let Some(cb) = &on_complete
                        && let Some(info) = g.value()
                    {
                        // Note: you also have a latent bug here — this future is
                        // created and immediately dropped, never executed.
                        // It needs tokio::spawn(cb(...)) to actually fire.
                        tokio::spawn(cb({
                            let mut i = info.clone();
                            i.set_response_status(status_code);
                            i
                        }));
                    }
                }));
            } else {
                // Guard still drops at end of scope — wait_for_zero() works correctly.
                // We just skip the Arc alloc and the hook dispatch.
            }
        }

        // ── HEAD suppression ───────────────────────────────────────────
        let is_head = *call.method() == HttpMethod::HEAD;
        if is_head {
            call.set_suppress_body();
        }

        // ── Route matching ─────────────────────────────────────────────

        let method_to_match = if is_head {
            &HttpMethod::GET
        } else {
            call.request.method()
        };

        let request_route = call.request.route.clone();

        let matched = match self.router.find(method_to_match, request_route.as_ref()) {
            Some(m) => m,
            None => {
                // Find all allowed methods for the 405 Allow Header
                let mut allowed_methods = Vec::new();
                for m in [
                    HttpMethod::GET,
                    HttpMethod::POST,
                    HttpMethod::PUT,
                    HttpMethod::DELETE,
                    HttpMethod::PATCH,
                ] {
                    if self.router.find(&m, call.request.route()).is_some() {
                        allowed_methods.push(m.to_string());
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

        call.request.path_params = matched
            .params
            .into_iter()
            .map(|(k, v)| (Arc::from(k), Arc::from(v)))
            .collect();

        // ── Middleware ─────────────────────────────────────────────────
        for middleware in matched.route.middleware.iter() {
            middleware(&mut call).await;
            if call.response_sent() {
                return self.finish(call.connection, keep_alive).await;
            }
        }

        // ── Handler ────────────────────────────────────────────────────
        (matched.route.handler)(&mut call).await;

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
    router: Vec<RouteDefinition>,
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
            router: inventory::iter::<RouteFactory>()
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
        self.router.push(route_definition);
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

        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let mut listeners = Vec::new();

        for _ in 0..cores {
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
            socket.set_tcp_nodelay(true).unwrap_or(());
            socket.set_recv_buffer_size(256 * 1024).unwrap_or(());
            socket.set_send_buffer_size(256 * 1024).unwrap_or(());
            socket
                .set_tcp_keepalive(
                    &socket2::TcpKeepalive::new()
                        .with_time(std::time::Duration::from_secs(60))
                        .with_interval(std::time::Duration::from_secs(10)),
                )
                .unwrap_or(());

            let std_listener: std::net::TcpListener = socket.into();
            let listener = TcpListener::from_std(std_listener)
                .map_err(|_| HttpServerError::AddrInUse(self.port))?;

            listeners.push(listener);
        }

        let tls = if let Some(tls_config) = self.tls_config {
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
            Some(tls_config.build()?)
        } else {
            None
        };

        Ok(HttpServer {
            router: Router::build(self.router),
            listeners,
            active_requests: ActiveSet::new_with_telemetry(self.on_request_complete.is_some()),
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
