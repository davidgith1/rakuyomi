//! JNI entry points for the Android companion app.
//!
//! The companion app (`git.shin.rakuyomi_bridge`) loads this crate as a
//! shared object via `System.loadLibrary("rakuyomi_server")` and calls:
//!
//! * `RakuyomiServer.nativeStart(homePath, port)` to spin up the HTTP
//!   server in a background tokio runtime.
//! * `RakuyomiServer.nativeStop()` to gracefully stop the server and
//!   tear down the runtime.
//! * `RakuyomiServer.nativeIsRunning()` to check whether a server is
//!   currently alive in this process.

use std::path::PathBuf;
use std::sync::{Mutex as StdMutex, OnceLock};
use std::time::Duration;

use jni::errors::LogErrorAndDefault;
use jni::objects::{JByteArray, JClass, JString};
use jni::sys::{jint, jlong, jstring};
use jni::EnvUnowned;

use log::error;
use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, Mutex as AsyncMutex};
use tokio::task::JoinHandle;

use crate::listener::pick_listener;

/// How long to wait for the server to drain before forcefully shutting down.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Helper for graceful shutdown: a oneshot channel where the sender
/// triggers the server to stop.
type ShutdownSignal = oneshot::Sender<()>;
type ShutdownReceiver = oneshot::Receiver<()>;

fn shutdown_pair() -> (ShutdownSignal, ShutdownReceiver) {
    oneshot::channel()
}

#[cfg(feature = "api_18")]
mod android_polyfills {
    use std::os::raw::{c_int, c_void};

    #[no_mangle]
    pub unsafe extern "C" fn epoll_create1(flags: c_int) -> c_int {
        let fd = libc::syscall(libc::SYS_epoll_create, 1) as c_int;
        if fd >= 0 && (flags & libc::O_CLOEXEC) != 0 {
            libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
        }
        fd
    }

    #[no_mangle]
    pub unsafe extern "C" fn dl_iterate_phdr(
        _callback: Option<unsafe extern "C" fn(*mut c_void, usize, *mut c_void) -> c_int>,
        _data: *mut c_void,
    ) -> c_int {
        0
    }

    #[no_mangle]
    pub unsafe extern "C" fn signal(_signum: c_int, _handler: usize) -> usize {
        0
    }
}

#[derive(Serialize, Deserialize)]
pub struct PendingRequest {
    pub id: u64,
    pub url: String,
    pub method: String,
    pub headers: std::collections::HashMap<String, String>,
    #[serde(with = "serde_bytes")]
    pub body: Option<Vec<u8>>,
}

pub struct NetworkResponse {
    pub status_code: u16,
    pub headers: std::collections::HashMap<String, String>,
    pub body: Option<Vec<u8>>,
}

type ResponseTx = oneshot::Sender<Result<NetworkResponse, String>>;

static JAVA_VM: OnceLock<jni::JavaVM> = OnceLock::new();
static SERVER_CLASS: OnceLock<jni::objects::Global<JClass<'static>>> = OnceLock::new();
static NET_PENDING: OnceLock<StdMutex<std::collections::HashMap<u64, ResponseTx>>> =
    OnceLock::new();
static REQ_ID_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn net_pending() -> &'static StdMutex<std::collections::HashMap<u64, ResponseTx>> {
    NET_PENDING.get_or_init(|| StdMutex::new(std::collections::HashMap::new()))
}

pub async fn bridge_net_send(
    req: &shared::source::wasm_store::RequestBuildingState,
) -> anyhow::Result<shared::source::wasm_store::ResponseData> {
    let id = REQ_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let (tx, rx) = oneshot::channel();

    let pending = PendingRequest {
        id,
        url: req.url.as_ref().map(|u| u.to_string()).unwrap_or_default(),
        method: req
            .method
            .as_ref()
            .map(|m| m.to_string())
            .unwrap_or_else(|| "GET".to_string()),
        headers: req.headers.clone(),
        body: req.body.clone(),
    };

    let json = serde_json::to_string(&pending).unwrap_or_default();

    if let Ok(mut pending_map) = net_pending().lock() {
        pending_map.insert(id, tx);
    }

    // Proactively call Kotlin
    if let Some(vm) = JAVA_VM.get() {
        vm.attach_current_thread(|env| {
            if let Some(class) = SERVER_CLASS.get() {
                if let Ok(jstr) = env.new_string(&json) {
                    let res = env.call_static_method(
                        class,
                        jni::jni_str!("onNetworkRequest"),
                        jni::jni_sig!("(JLjava/lang/String;)V"),
                        &[
                            jni::objects::JValue::Long(id as jlong),
                            jni::objects::JValue::Object(&jstr.into()),
                        ],
                    );
                    if let Err(e) = res {
                        error!("failed to call onNetworkRequest: {e}");
                    }
                } else {
                    error!("failed to create JString for request {id}");
                }
            } else {
                error!("SERVER_CLASS not initialized, cannot call Kotlin");
            }
            Ok::<(), jni::errors::Error>(())
        })?;
    } else {
        error!("JAVA_VM not initialized, cannot call Kotlin");
    }

    let res = rx
        .await
        .map_err(|_| anyhow::anyhow!("Bridge dropped"))?
        .map_err(anyhow::Error::msg)?;

    let mut header_map = reqwest::header::HeaderMap::new();
    for (k, v) in res.headers {
        if let (Ok(name), Ok(val)) = (
            reqwest::header::HeaderName::from_bytes(k.as_bytes()),
            reqwest::header::HeaderValue::from_str(&v),
        ) {
            header_map.insert(name, val);
        }
    }

    Ok(shared::source::wasm_store::ResponseData {
        url: reqwest::Url::parse(&pending.url)?,
        status_code: reqwest::StatusCode::from_u16(res.status_code)?,
        headers: header_map,
        body: res.body,
        bytes_read: 0,
    })
}

static LOG_QUEUE: OnceLock<StdMutex<Vec<String>>> = OnceLock::new();

fn log_queue() -> &'static StdMutex<Vec<String>> {
    LOG_QUEUE.get_or_init(|| StdMutex::new(Vec::new()))
}

pub fn push_log(log: String) {
    if let Ok(mut queue) = log_queue().lock() {
        queue.push(log);
        // Keep only last 1000 logs for safety
        if queue.len() > 1000 {
            queue.remove(0);
        }
    }
}

/// Status codes returned by the JNI entry points.
pub mod status {
    use jni::sys::jint;
    pub const OK: jint = 0;
    pub const ALREADY_RUNNING: jint = 1;
    pub const INVALID_ARGUMENT: jint = 2;
    pub const RUNTIME_INIT_FAILED: jint = 3;
    pub const RUNTIME_TASK_FAILED: jint = 4;
    pub const NOT_RUNNING: jint = 5;
    pub const INTERNAL_ERROR: jint = 100;
}

struct ServerState {
    runtime: tokio::runtime::Runtime,
    server_handle: JoinHandle<()>,
    shutdown_tx: Option<ShutdownSignal>,
}

static SERVER: OnceLock<AsyncMutex<Option<ServerState>>> = OnceLock::new();

fn state() -> &'static AsyncMutex<Option<ServerState>> {
    SERVER.get_or_init(|| AsyncMutex::new(None))
}

fn current_thread_name() -> Option<String> {
    std::thread::current().name().map(|s| s.to_string())
}

/// Clears `guard` if the server task it points to has already finished
/// (e.g. the axum accept loop died from an OS-level socket hiccup while
/// the app was backgrounded/frozen). Without this, `nativeIsRunning` and
/// the `ALREADY_RUNNING` check in `nativeStart` would keep reporting a
/// dead server as running forever, since only the process being killed
/// used to reset this state.
fn reap_if_dead(guard: &mut Option<ServerState>) {
    let dead = guard
        .as_ref()
        .is_some_and(|server| server.server_handle.is_finished());

    if dead {
        if let Some(server) = guard.take() {
            diag_log_default("reap_if_dead: task had finished, clearing stale state");
            log::warn!("rakuyomi server task had exited unexpectedly; clearing stale state");
            drop(server.runtime);
        }
    }
}

/// Start the server.
///
/// # Safety
#[no_mangle]
pub extern "system" fn Java_git_shin_rakuyomi_1bridge_RakuyomiServer_nativeStart<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    home_path: JString<'caller>,
    port: jint,
) -> jint {
    crate::init_logging();
    shared::usecases::install_update::cleanup_update_backup();

    // Store JVM for callbacks
    unowned_env
        .with_env(|env| {
            if let Ok(vm) = env.get_java_vm() {
                let _ = JAVA_VM.set(vm);
            }
            // Cache the class reference to avoid ClassLoader issues in background threads
            let class_name = jni::jni_str!("git/shin/rakuyomi_bridge/RakuyomiServer");
            if let Ok(cls) = env.find_class(class_name) {
                match env.new_global_ref(cls) {
                    Ok(global_cls) => {
                        let _ = SERVER_CLASS.set(global_cls);
                    }
                    Err(e) => error!("failed to create global ref for RakuyomiServer class: {e}"),
                }
            } else {
                error!("failed to find RakuyomiServer class");
            }
            Ok::<(), jni::errors::Error>(())
        })
        .resolve::<LogErrorAndDefault>();

    // Hook shared::source::wasm_imports::net::NET_SEND
    let _ = shared::source::wasm_imports::net::NET_SEND.set(|_token, builder| {
        tokio::runtime::Handle::current().block_on(bridge_net_send(builder))
    });

    let home_path_str: String = unowned_env
        .with_env(|_env| -> jni::errors::Result<String> {
            if home_path.is_null() {
                Ok(String::new())
            } else {
                Ok(home_path.to_string())
            }
        })
        .resolve::<LogErrorAndDefault>();

    let home_path = if home_path_str.is_empty() {
        PathBuf::from(".")
    } else {
        PathBuf::from(&home_path_str)
    };

    let _ = DIAG_HOME_PATH.set(home_path.clone());
    diag_log(&home_path, "nativeStart: called, acquiring lock");

    let mut guard = match state().try_lock() {
        Ok(g) => g,
        Err(_) => {
            diag_log(&home_path, "nativeStart: lock busy, returning INTERNAL_ERROR");
            error!("server state lock is busy");
            return status::INTERNAL_ERROR;
        }
    };

    reap_if_dead(&mut guard);

    if guard.is_some() {
        diag_log(&home_path, "nativeStart: guard already Some, returning ALREADY_RUNNING");
        return status::ALREADY_RUNNING;
    }

    // Force the listener to TCP because we are inside an Android app
    // where Unix domain sockets are not available across processes.
    std::env::set_var("RAKUYOMI_TCP_PORT", port.to_string());

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("rakuyomi-jni")
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            error!("failed to build tokio runtime: {e}");
            return status::RUNTIME_INIT_FAILED;
        }
    };

    let (tx, rx) = shutdown_pair();
    let home_path_for_task = home_path.clone();
    let home_path_for_diag = home_path.clone();
    diag_log(&home_path_for_diag, "nativeStart: spawning task");
    let server_handle = runtime.spawn(async move {
        diag_log(&home_path_for_diag, "task: entered, calling pick_listener()");
        match pick_listener().await {
            Ok(listener) => {
                diag_log(
                    &home_path_for_diag,
                    &format!("task: pick_listener() OK ({}), calling build_state()", listener.describe()),
                );
                match crate::build_state(home_path_for_task).await {
                Ok(state) => {
                    diag_log(&home_path_for_diag, "task: build_state() OK, serving");
                    let app = crate::build_router(state);
                    match listener {
                        crate::listener::ResolvedListener::Tcp(l) => {
                            let shutdown = async move {
                                let _ = rx.await;
                            };
                            if let Err(e) =
                                axum::serve(l, app).with_graceful_shutdown(shutdown).await
                            {
                                diag_log(&home_path_for_diag, &format!("task: server error: {e}"));
                                error!("server error: {e}");
                            }
                        }
                        crate::listener::ResolvedListener::Unix(l, _) => {
                            let shutdown = async move {
                                let _ = rx.await;
                            };
                            if let Err(e) =
                                axum::serve(l, app).with_graceful_shutdown(shutdown).await
                            {
                                diag_log(&home_path_for_diag, &format!("task: server error: {e}"));
                                error!("server error: {e}");
                            }
                        }
                    }
                    diag_log(&home_path_for_diag, "task: axum::serve() returned, task ending");
                }
                Err(e) => {
                    diag_log(&home_path_for_diag, &format!("task: build_state() FAILED: {e:#}"));
                    error!("failed to build state: {e}");
                }
            }
            }
            Err(e) => {
                diag_log(&home_path_for_diag, &format!("task: pick_listener() FAILED: {e:#}"));
                error!("failed to pick listener: {e}");
            }
        }
    });

    *guard = Some(ServerState {
        runtime,
        server_handle,
        shutdown_tx: Some(tx),
    });

    log_start(&home_path, port);
    status::OK
}

/// TEMPORARY diagnostic: appends a timestamped line to a plain file in the
/// shared home directory, since Rust's stderr logger doesn't reliably reach
/// logcat on this device. Safe to leave as a no-op-ish append; remove once
/// the restart-hang bug is root-caused.
fn diag_log(home_path: &std::path::Path, msg: &str) {
    use std::io::Write;
    let path = home_path.join("rakuyomi_bridge_diag.log");
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
    {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let _ = writeln!(f, "[{now}] {msg}");
    }
}

static DIAG_HOME_PATH: OnceLock<PathBuf> = OnceLock::new();

fn diag_log_default(msg: &str) {
    if let Some(home_path) = DIAG_HOME_PATH.get() {
        diag_log(home_path, msg);
    }
}

fn log_start(home_path: &std::path::Path, port: jint) {
    let thread = current_thread_name().unwrap_or_else(|| "unknown".into());
    log::info!(
        "rakuyomi server started on thread={thread} home={} port={port}",
        home_path.display()
    );
}

/// Stop the server. Safe to call even when no server is running.
#[no_mangle]
pub extern "system" fn Java_git_shin_rakuyomi_1bridge_RakuyomiServer_nativeStop<'caller>(
    _unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
) -> jint {
    crate::init_logging();
    diag_log_default("nativeStop: called, acquiring lock");
    let mut guard = match state().try_lock() {
        Ok(g) => g,
        Err(_) => {
            diag_log_default("nativeStop: lock busy, returning INTERNAL_ERROR");
            return status::INTERNAL_ERROR;
        }
    };

    let Some(mut server) = guard.take() else {
        diag_log_default("nativeStop: guard already None, returning NOT_RUNNING");
        return status::NOT_RUNNING;
    };

    // Release the lock before the (up to multi-second) blocking shutdown
    // drain below. `server` is already fully owned here, so holding the
    // guard any longer only serves to make concurrent nativeStart calls
    // fail with INTERNAL_ERROR instead of actually starting a new server.
    drop(guard);

    diag_log_default("nativeStop: signaling shutdown, starting drain wait");
    if let Some(tx) = server.shutdown_tx.take() {
        let _ = tx.send(());
    }

    // Best-effort: wait for graceful shutdown, then abort.
    let timeout = SHUTDOWN_TIMEOUT.min(Duration::from_secs(2));
    let _ = server
        .runtime
        .block_on(async { tokio::time::timeout(timeout, &mut server.server_handle).await });

    // If the server task didn't exit in time, abort it.
    server.server_handle.abort();

    // Drop the runtime, which cancels any remaining tasks.
    drop(server.runtime);

    diag_log_default("nativeStop: runtime dropped, returning OK");
    log::info!("rakuyomi server stopped");
    status::OK
}

/// Poll for any new log entries.
#[no_mangle]
pub extern "system" fn Java_git_shin_rakuyomi_1bridge_RakuyomiServer_nativePollLogs<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
) -> jstring {
    let logs = if let Ok(mut queue) = log_queue().lock() {
        if queue.is_empty() {
            return std::ptr::null_mut();
        }
        let all_logs = queue.join(";");
        queue.clear();
        all_logs
    } else {
        return std::ptr::null_mut();
    };

    unowned_env
        .with_env(|env| {
            let jstr = env.new_string(logs)?;
            Ok::<jstring, jni::errors::Error>(jstr.into_raw())
        })
        .resolve::<LogErrorAndDefault>()
}

/// Send response back to Rust.
#[no_mangle]
pub extern "system" fn Java_git_shin_rakuyomi_1bridge_RakuyomiServer_nativeSendNetworkResponse<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    request_id: jlong,
    status_code: jint,
    headers_json: JString<'caller>,
    body: JByteArray<'caller>,
) {
    let headers: std::collections::HashMap<String, String> = unowned_env
        .with_env(|_env| {
            let s = headers_json.to_string();
            Ok::<std::collections::HashMap<String, String>, jni::errors::Error>(
                serde_json::from_str(&s).unwrap_or_default(),
            )
        })
        .resolve::<LogErrorAndDefault>();

    let body_vec = if !body.is_null() {
        unowned_env
            .with_env(|env| Ok::<Vec<u8>, jni::errors::Error>(env.convert_byte_array(body)?))
            .resolve::<LogErrorAndDefault>()
    } else {
        Vec::new()
    };

    if let Ok(mut pending) = net_pending().lock() {
        if let Some(tx) = pending.remove(&(request_id as u64)) {
            let _ = tx.send(Ok(NetworkResponse {
                status_code: status_code as u16,
                headers,
                body: Some(body_vec),
            }));
        }
    }
}

/// Send error back to Rust.
#[no_mangle]
pub extern "system" fn Java_git_shin_rakuyomi_1bridge_RakuyomiServer_nativeSendNetworkError<
    'caller,
>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    request_id: jlong,
    error_msg: JString<'caller>,
) {
    let err = unowned_env
        .with_env(|_env| Ok::<String, jni::errors::Error>(error_msg.to_string()))
        .resolve::<LogErrorAndDefault>();

    if let Ok(mut pending) = net_pending().lock() {
        if let Some(tx) = pending.remove(&(request_id as u64)) {
            let _ = tx.send(Err(err));
        }
    }
}

/// Returns 1 if a server is currently running, 0 otherwise.
#[no_mangle]
pub extern "system" fn Java_git_shin_rakuyomi_1bridge_RakuyomiServer_nativeIsRunning<'caller>(
    _unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
) -> jint {
    match state().try_lock() {
        Ok(mut g) => {
            reap_if_dead(&mut g);

            if g.is_some() {
                1
            } else {
                0
            }
        }
        Err(_) => {
            diag_log_default("nativeIsRunning: lock busy, returning -1");
            -1
        }
    }
}
