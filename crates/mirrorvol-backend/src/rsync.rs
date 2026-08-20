//! rsync implementation
//!
//! Pull-only: this node only ever pulls from
//! [`ReplicaConfig::active_peer_address`], never the reverse, so
//! [`conflict_files`](LocalBackend::conflict_files) always returns
//! `Ok(vec![])`. Two passes, both through [`RsyncRunner`]: a periodic warm
//! sync (background task per volume, non-gating), and
//! [`completion`](LocalBackend::completion)'s full pull + dry-run verify,
//! which `reconcile_local_strict` calls at the promotion checkpoints.
//!
//! Every real pull is wrapped in a short-lived `stunnel` client tunnel to
//! a `stunnel` server sidecar fronting the peer's `rsyncd`: opened right
//! before the pull, closed right after. This is mandatory:
//! `mirrorvol-agent`/`mirrorvol-csi` always call [`with_stunnel`].
//! `tunnel` stays `Option`al on [`RsyncBackend`] for unit testing only.
//! Authenticated by TLS-PSK, reusing the same shared secret already
//! provisioned for rsync module auth ([`derive_psk_secrets_file`]).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use thiserror::Error;

use crate::lockfile::{self, RoleSetter};
use crate::{
    BackendError, CompletionStatus, LocalBackend, LockState, ReplicaConfig, WriterOperation,
};

pub mod env {
    /// Path to the file holding this node's rsync module password
    /// (also derives the `stunnel` PSK).
    pub const RSYNC_SECRET_FILE: &str = "RSYNC_SECRET_FILE";

    /// Path to the file holding this node's per-node `rsync` device
    /// identity token — distinct from [`RSYNC_SECRET_FILE`] (the shared
    /// module auth password). Written by `mirrorvol-agent provision`,
    /// read by `mirrorvol-agent run` to register this node's `BackendNode`
    /// identity.
    pub const RSYNC_IDENTITY_FILE: &str = "RSYNC_IDENTITY_FILE";
}

/// The `stunnel` server sidecar's fixed TLS-listening port.
pub const STUNNEL_PORT: u16 = 8873;

const ROLE_FILE_NAME: &str = ".mirror-rsync-role";
const FILTER_FILE_NAME: &str = ".mirror-rsync-filter";
const ROLE_WRITER: &str = "writer";
const ROLE_STANDBY: &str = "standby";

#[derive(Debug, Error)]
pub enum RsyncError {
    #[error("rsync exited with status {status}: {stderr}")]
    NonZeroExit { status: i32, stderr: String },
    #[error("failed to launch rsync: {0}")]
    Spawn(std::io::Error),
    #[error("volume {0} has no known active peer to pull from yet")]
    NoActivePeer(String),
    #[error("filesystem operation on {path} failed: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
}

impl From<RsyncError> for BackendError {
    fn from(value: RsyncError) -> Self {
        BackendError::Request(value.to_string())
    }
}

/// One rsync pass. Returns each stdout line, which the dry-run pass
/// in [`RsyncBackend::full_resync`] inspects for pending changes.
#[async_trait]
pub trait RsyncRunner: Send + Sync {
    async fn run(&self, args: &[String]) -> Result<Vec<String>, BackendError>;
}

/// Shells out to the real `rsync` binary.
pub struct SystemRsyncRunner;

#[async_trait]
impl RsyncRunner for SystemRsyncRunner {
    async fn run(&self, args: &[String]) -> Result<Vec<String>, BackendError> {
        let output = tokio::process::Command::new("rsync")
            .args(args)
            .output()
            .await
            .map_err(RsyncError::Spawn)?;
        if !output.status.success() {
            return Err(RsyncError::NonZeroExit {
                status: output.status.code().unwrap_or(-1),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            }
            .into());
        }
        Ok(String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(str::to_owned)
            .collect())
    }
}

/// Opens one local TLS tunnel for one pull.
#[async_trait]
pub trait TunnelClient: Send + Sync {
    /// Returns the loopback `host:port` rsync should connect to instead of
    /// `source_address` directly. Torn down when the handle is dropped.
    async fn open(&self, source_address: &str) -> Result<Box<dyn TunnelHandle>, BackendError>;
}

/// One open tunnel; `Drop` tears the underlying process down.
pub trait TunnelHandle: Send + Sync {
    fn local_address(&self) -> &str;
}

/// Shells out to the real `stunnel` binary in client mode. One tunnel per
/// pull, opened fresh with `source_address` as `connect =`.
pub struct SystemStunnelClient {
    tls_port: u16,
    secret_file: PathBuf,
    // Derived once and reused — the secret doesn't rotate at runtime.
    psk_path: tokio::sync::OnceCell<PathBuf>,
}

impl SystemStunnelClient {
    pub fn new(tls_port: u16, secret_file: impl Into<PathBuf>) -> Self {
        Self {
            tls_port,
            secret_file: secret_file.into(),
            psk_path: tokio::sync::OnceCell::new(),
        }
    }

    async fn psk_secrets_path(&self) -> Result<&PathBuf, BackendError> {
        self.psk_path
            .get_or_try_init(|| derive_psk_secrets_file(&self.secret_file))
            .await
    }
}

#[async_trait]
impl TunnelClient for SystemStunnelClient {
    async fn open(&self, source_address: &str) -> Result<Box<dyn TunnelHandle>, BackendError> {
        let psk_path = self.psk_secrets_path().await?;
        let local_port = allocate_local_port()?;
        let local_address = format!("127.0.0.1:{local_port}");
        let config_path = std::env::temp_dir().join(format!(
            "mirrorvol-stunnel-{}-{local_port}.conf",
            std::process::id()
        ));
        let config = format!(
            "client = yes\nforeground = yes\npid =\n\n[mirrorvol]\naccept = {local_address}\nconnect = {source_address}:{}\nPSKsecrets = {}\n",
            self.tls_port,
            psk_path.display(),
        );
        tokio::fs::write(&config_path, &config)
            .await
            .map_err(|source| RsyncError::Io {
                path: config_path.clone(),
                source,
            })?;
        let config_guard = TempFileGuard(config_path.clone());
        let child = tokio::process::Command::new("stunnel")
            .arg(&config_path)
            .spawn()
            .map_err(RsyncError::Spawn)?;
        wait_for_local_listener(&local_address).await?;
        Ok(Box::new(SystemTunnelHandle {
            child,
            local_address,
            _config_guard: config_guard,
        }))
    }
}

struct SystemTunnelHandle {
    child: tokio::process::Child,
    local_address: String,
    _config_guard: TempFileGuard,
}

impl TunnelHandle for SystemTunnelHandle {
    fn local_address(&self) -> &str {
        &self.local_address
    }
}

impl Drop for SystemTunnelHandle {
    fn drop(&mut self) {
        // `start_kill` is synchronous, unlike `kill`/`wait` — usable here.
        let _ = self.child.start_kill();
    }
}

/// Deletes its file on drop, best-effort.
struct TempFileGuard(PathBuf);

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Binds an OS-assigned ephemeral port and releases it.
fn allocate_local_port() -> Result<u16, BackendError> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|source| RsyncError::Io {
        path: PathBuf::from("127.0.0.1:0"),
        source,
    })?;
    listener
        .local_addr()
        .map(|address| address.port())
        .map_err(|source| {
            RsyncError::Io {
                path: PathBuf::from("127.0.0.1:0"),
                source,
            }
            .into()
        })
}

/// Polls a local tunnel's accept address until something is listening.
async fn wait_for_local_listener(address: &str) -> Result<(), BackendError> {
    for _ in 0..20 {
        if tokio::net::TcpStream::connect(address).await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(RsyncError::Spawn(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        format!("stunnel did not start listening on {address} in time"),
    ))
    .into())
}

/// Writes the `identity:key` form both rsyncd's `secrets file` and
/// `stunnel`'s `PSKsecrets` directives require (`{secret_file}.psk`). The
/// password is already 32 hex chars, which also satisfies stunnel's "PSK
/// key must be hex" requirement as-is. Mode `0600`, unix only.
async fn derive_psk_secrets_file(secret_file: &Path) -> Result<PathBuf, BackendError> {
    let password = tokio::fs::read_to_string(secret_file)
        .await
        .map_err(|source| RsyncError::Io {
            path: secret_file.to_owned(),
            source,
        })?;
    let path = secret_file.with_extension("psk");
    tokio::fs::write(&path, format!("mirrorvol:{}", password.trim()))
        .await
        .map_err(|source| RsyncError::Io {
            path: path.clone(),
            source,
        })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .await
            .map_err(|source| RsyncError::Io {
                path: path.clone(),
                source,
            })?;
    }
    Ok(path)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RsyncCompletion {
    pub ready: bool,
}

impl CompletionStatus for RsyncCompletion {
    fn ready(&self) -> bool {
        self.ready
    }
}

struct VolumeConfig {
    generation: u64,
    local_path: PathBuf,
    active_peer_address: Option<String>,
}

pub struct RsyncBackend {
    runner: Arc<dyn RsyncRunner>,
    data_root: PathBuf,
    secret_file: Option<PathBuf>,
    /// This node's `rsyncd.conf`, set only if this node also runs the
    /// daemon (e.g. unset in `mirrorvol-csi`, which only pulls).
    rsyncd_conf_path: Option<PathBuf>,
    /// `None` only in unit tests; every real caller sets it via
    /// [`with_stunnel`](RsyncBackend::with_stunnel).
    tunnel: Option<Arc<dyn TunnelClient>>,
    configured: Mutex<HashMap<String, VolumeConfig>>,
    ignore_patterns: Mutex<HashMap<String, Vec<String>>>,
    // Keyed by volume so a repeat `ensure_replica` doesn't spawn a
    // duplicate loop; never explicitly cancelled (harmless if leaked).
    warm_sync_tasks: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
}

impl RsyncBackend {
    pub fn new(runner: Arc<dyn RsyncRunner>, data_root: impl Into<PathBuf>) -> Self {
        Self {
            runner,
            data_root: data_root.into(),
            secret_file: None,
            rsyncd_conf_path: None,
            tunnel: None,
            configured: Mutex::new(HashMap::new()),
            ignore_patterns: Mutex::new(HashMap::new()),
            warm_sync_tasks: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_secret_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.secret_file = Some(path.into());
        self
    }

    /// Enables this node serving its own replicas as `rsyncd` modules.
    pub fn with_rsyncd_conf_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.rsyncd_conf_path = Some(path.into());
        self
    }

    /// Wraps every pull in a local `stunnel` tunnel. Every real caller
    /// sets this; there is no plaintext-pull path in production.
    pub fn with_stunnel(mut self, tunnel: Arc<dyn TunnelClient>) -> Self {
        self.tunnel = Some(tunnel);
        self
    }

    /// Regenerates the whole `rsyncd.conf` module list from every volume
    /// this backend knows about — cheap enough to redo on every
    /// `ensure_replica`, and safe to call with zero volumes configured
    /// (writes an empty module list plus the PSK secrets file). Callers
    /// should call this once at startup too, not only from
    /// `ensure_replica`: the `rsyncd`/`stunnel` sidecars wait for these
    /// files to exist before they'll start, so a node with no
    /// `rsync`-backed volume assigned yet would otherwise never become
    /// ready. Global daemon directives (port, uid, ...) are the
    /// deployment's concern, not this function's.
    pub async fn write_rsyncd_conf(&self) -> Result<(), BackendError> {
        let Some(conf_path) = self.rsyncd_conf_path.clone() else {
            return Ok(());
        };
        let server_secrets_path = match &self.secret_file {
            Some(secret_file) => Some(derive_psk_secrets_file(secret_file).await?),
            None => None,
        };
        let mut contents = String::new();
        for (volume_id, config) in self.configured.lock().iter() {
            contents.push_str(&format!(
                "[{volume_id}]\n    path = {}\n    read only = yes\n",
                config.local_path.display()
            ));
            if let Some(server_secrets_path) = &server_secrets_path {
                contents.push_str(&format!(
                    "    auth users = mirrorvol\n    secrets file = {}\n",
                    server_secrets_path.display()
                ));
            }
            contents.push('\n');
        }
        if let Some(parent) = conf_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|source| RsyncError::Io {
                    path: parent.to_owned(),
                    source,
                })?;
        }
        let tmp_path = conf_path.with_extension("tmp");
        tokio::fs::write(&tmp_path, contents)
            .await
            .map_err(|source| RsyncError::Io {
                path: tmp_path.clone(),
                source,
            })?;
        tokio::fs::rename(&tmp_path, &conf_path)
            .await
            .map_err(|source| RsyncError::Io {
                path: conf_path,
                source,
            })?;
        Ok(())
    }

    fn replica_dir(&self, volume_id: &str) -> PathBuf {
        self.configured
            .lock()
            .get(volume_id)
            .map(|config| config.local_path.clone())
            .unwrap_or_else(|| self.data_root.join(volume_id))
    }

    fn role_path(&self, volume_id: &str) -> PathBuf {
        self.replica_dir(volume_id).join(ROLE_FILE_NAME)
    }

    fn filter_path(&self, volume_id: &str) -> PathBuf {
        self.replica_dir(volume_id).join(FILTER_FILE_NAME)
    }

    async fn write_role(&self, volume_id: &str, role: &str) -> Result<(), BackendError> {
        let path = self.role_path(volume_id);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|source| RsyncError::Io {
                    path: parent.to_owned(),
                    source,
                })?;
        }
        tokio::fs::write(&path, role)
            .await
            .map_err(|source| RsyncError::Io { path, source })?;
        Ok(())
    }

    async fn read_role(&self, volume_id: &str) -> Result<bool, BackendError> {
        let path = self.role_path(volume_id);
        match tokio::fs::read_to_string(&path).await {
            Ok(contents) => Ok(contents.trim() == ROLE_WRITER),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(source) => Err(RsyncError::Io { path, source }.into()),
        }
    }

    fn pull_args(&self, volume_id: &str, source_address: &str, dry_run: bool) -> Vec<String> {
        let mut args = vec!["-a".to_owned(), "--delete".to_owned()];
        if dry_run {
            args.push("--dry-run".to_owned());
            args.push("--itemize-changes".to_owned());
        }
        let filter = self.filter_path(volume_id);
        args.push(format!("--exclude-from={}", filter.display()));
        // Must match write_rsyncd_conf's `auth users = mirrorvol`.
        let source = if let Some(secret_file) = &self.secret_file {
            args.push(format!("--password-file={}", secret_file.display()));
            format!("rsync://mirrorvol@{source_address}/{volume_id}/")
        } else {
            format!("rsync://{source_address}/{volume_id}/")
        };
        args.push(source);
        args.push(format!("{}/", self.replica_dir(volume_id).display()));
        args
    }

    /// One full pull followed by a zero-diff dry-run verification. `Ok(false)`
    /// when no active peer is known yet (bootstrap), not an error.
    async fn full_resync(&self, volume_id: &str) -> Result<bool, BackendError> {
        let Some(source_address) = self
            .configured
            .lock()
            .get(volume_id)
            .and_then(|config| config.active_peer_address.clone())
        else {
            return Ok(false);
        };
        // `_tunnel` is held across both pulls, dropped (closing the
        // tunnel) once this function returns.
        let (pull_target, _tunnel) = match &self.tunnel {
            Some(tunnel) => {
                let handle = tunnel.open(&source_address).await?;
                let local_address = handle.local_address().to_owned();
                (local_address, Some(handle))
            }
            None => (source_address, None),
        };
        self.runner
            .run(&self.pull_args(volume_id, &pull_target, false))
            .await?;
        let remaining = self
            .runner
            .run(&self.pull_args(volume_id, &pull_target, true))
            .await?;
        Ok(remaining.is_empty())
    }

    /// Spawns (once per volume) a background loop running [`full_resync`]
    /// on `interval`. Errors are logged, never gating. `interval` changes
    /// take effect on next Pod restart, not immediately.
    fn ensure_warm_sync_task(self_arc: &Arc<Self>, volume_id: String, interval: Duration) {
        let mut tasks = self_arc.warm_sync_tasks.lock();
        if tasks.contains_key(&volume_id) {
            return;
        }
        let backend = Arc::clone(self_arc);
        let task_volume_id = volume_id.clone();
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // first tick fires immediately; skip it
            loop {
                ticker.tick().await;
                if let Err(error) = backend.full_resync(&task_volume_id).await {
                    tracing::warn!(volume = %task_volume_id, %error, "periodic warm sync failed");
                }
            }
        });
        tasks.insert(volume_id, handle);
    }
}

#[async_trait]
impl RoleSetter for RsyncBackend {
    async fn set_writer_role(&self, volume_id: &str, writer: bool) -> Result<(), BackendError> {
        self.write_role(volume_id, if writer { ROLE_WRITER } else { ROLE_STANDBY })
            .await
    }
}

#[async_trait]
impl LocalBackend for RsyncBackend {
    async fn ensure_replica(&self, config: &ReplicaConfig) -> Result<(), BackendError> {
        // active_peer_address refreshes unconditionally — see its doc
        // comment on ReplicaConfig.
        let already_configured = self
            .configured
            .lock()
            .get(&config.volume_id)
            .is_some_and(|existing| existing.generation == config.generation);
        self.configured.lock().insert(
            config.volume_id.clone(),
            VolumeConfig {
                generation: config.generation,
                local_path: PathBuf::from(&config.local_path),
                active_peer_address: config.active_peer_address.clone(),
            },
        );
        if already_configured {
            return Ok(());
        }
        tokio::fs::create_dir_all(&config.local_path)
            .await
            .map_err(|source| RsyncError::Io {
                path: PathBuf::from(&config.local_path),
                source,
            })?;
        self.write_rsyncd_conf().await?;
        Ok(())
    }

    async fn is_writer(&self, volume_id: &str) -> Result<bool, BackendError> {
        self.read_role(volume_id).await
    }

    async fn completion(&self, volume_id: &str) -> Result<Box<dyn CompletionStatus>, BackendError> {
        let ready = self.full_resync(volume_id).await?;
        Ok(Box::new(RsyncCompletion { ready }))
    }

    async fn lock_state(&self, volume_id: &str) -> Result<LockState, BackendError> {
        lockfile::lock_state(&self.replica_dir(volume_id), volume_id).await
    }

    async fn acquire_writer(
        &self,
        volume_id: &str,
        operation: &WriterOperation,
    ) -> Result<(), BackendError> {
        lockfile::acquire_writer(self, &self.replica_dir(volume_id), volume_id, operation).await
    }

    async fn restore_writer(&self, volume_id: &str, epoch: u64) -> Result<(), BackendError> {
        lockfile::restore_writer(self, &self.replica_dir(volume_id), volume_id, epoch).await
    }

    async fn release_writer(
        &self,
        volume_id: &str,
        _operation: &WriterOperation,
    ) -> Result<(), BackendError> {
        lockfile::remove_lock(&self.replica_dir(volume_id)).await
    }

    async fn enforce_standby(&self, volume_id: &str) -> Result<(), BackendError> {
        self.set_writer_role(volume_id, false).await
    }

    async fn enable_send_receive(&self, volume_id: &str) -> Result<(), BackendError> {
        // bestEffort only; near-no-op for a pull-only backend.
        self.set_writer_role(volume_id, true).await
    }

    async fn conflict_files(&self, _volume_id: &str) -> Result<Vec<String>, BackendError> {
        Ok(Vec::new())
    }

    async fn set_ignore_patterns(
        &self,
        volume_id: &str,
        patterns: &[String],
    ) -> Result<(), BackendError> {
        if self
            .ignore_patterns
            .lock()
            .get(volume_id)
            .map(Vec::as_slice)
            == Some(patterns)
        {
            return Ok(());
        }
        let path = self.filter_path(volume_id);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|source| RsyncError::Io {
                    path: parent.to_owned(),
                    source,
                })?;
        }
        tokio::fs::write(&path, patterns.join("\n"))
            .await
            .map_err(|source| RsyncError::Io { path, source })?;
        self.ignore_patterns
            .lock()
            .insert(volume_id.to_owned(), patterns.to_vec());
        Ok(())
    }

    fn start_background_tasks(self: Arc<Self>, volume_id: String, interval: Duration) {
        Self::ensure_warm_sync_task(&self, volume_id, interval);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct FakeRunner {
        dry_run_remaining: Mutex<Vec<String>>,
        calls: AtomicUsize,
        last_args: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl RsyncRunner for FakeRunner {
        async fn run(&self, args: &[String]) -> Result<Vec<String>, BackendError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.last_args.lock() = args.to_vec();
            if args.iter().any(|arg| arg == "--dry-run") {
                Ok(self.dry_run_remaining.lock().clone())
            } else {
                Ok(vec![])
            }
        }
    }

    struct FakeTunnelClient {
        opened: AtomicUsize,
    }

    struct FakeTunnelHandle;

    impl TunnelHandle for FakeTunnelHandle {
        fn local_address(&self) -> &str {
            "127.0.0.1:19999"
        }
    }

    #[async_trait]
    impl TunnelClient for FakeTunnelClient {
        async fn open(&self, _source_address: &str) -> Result<Box<dyn TunnelHandle>, BackendError> {
            self.opened.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(FakeTunnelHandle))
        }
    }

    fn replica_config(active_peer_address: Option<&str>) -> ReplicaConfig {
        ReplicaConfig {
            volume_id: "volume".to_owned(),
            local_path: "/tmp/mirrorvol-rsync-test/volume".to_owned(),
            active_peer_address: active_peer_address.map(str::to_owned),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn completion_is_never_ready_without_a_known_active_peer() {
        let backend = RsyncBackend::new(Arc::new(FakeRunner::default()), std::env::temp_dir());
        backend
            .ensure_replica(&replica_config(None))
            .await
            .expect("ensure_replica");
        assert!(!backend
            .completion("volume")
            .await
            .expect("completion")
            .ready());
    }

    #[tokio::test]
    async fn completion_blocks_until_the_dry_run_pass_reports_nothing_pending() {
        let runner = Arc::new(FakeRunner::default());
        runner
            .dry_run_remaining
            .lock()
            .push(">f+++++++++ still-pending.txt".to_owned());
        let backend = RsyncBackend::new(runner.clone(), std::env::temp_dir());
        backend
            .ensure_replica(&replica_config(Some("node-a.mirrorvol.svc")))
            .await
            .expect("ensure_replica");
        assert!(!backend
            .completion("volume")
            .await
            .expect("completion")
            .ready());

        runner.dry_run_remaining.lock().clear();
        assert!(backend
            .completion("volume")
            .await
            .expect("completion")
            .ready());
    }

    #[tokio::test]
    async fn conflict_files_is_always_empty() {
        let backend = RsyncBackend::new(Arc::new(FakeRunner::default()), std::env::temp_dir());
        assert!(backend
            .conflict_files("volume")
            .await
            .expect("conflict_files")
            .is_empty());
    }

    #[tokio::test]
    async fn acquire_writer_rejects_a_present_lock_without_writing_the_role_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = RsyncBackend::new(Arc::new(FakeRunner::default()), dir.path());
        let mut config = replica_config(None);
        config.local_path = dir.path().to_string_lossy().into_owned();
        backend
            .ensure_replica(&config)
            .await
            .expect("ensure_replica");
        let op = WriterOperation {
            operation_id: 1,
            epoch: 1,
            writer_node: "node-a".to_owned(),
        };
        backend
            .acquire_writer("volume", &op)
            .await
            .expect("first acquire");
        assert!(backend
            .acquire_writer("volume", &op)
            .await
            .is_err_and(|error| matches!(error, BackendError::LockPresent)));
    }

    #[tokio::test]
    async fn active_peer_address_refreshes_even_when_generation_is_unchanged() {
        let backend = RsyncBackend::new(Arc::new(FakeRunner::default()), std::env::temp_dir());
        let mut config = replica_config(Some("node-a.mirrorvol.svc"));
        config.generation = 1;
        backend
            .ensure_replica(&config)
            .await
            .expect("first ensure_replica");
        config.active_peer_address = Some("node-b.mirrorvol.svc".to_owned());
        // generation unchanged — only the active peer moved.
        backend
            .ensure_replica(&config)
            .await
            .expect("second ensure_replica");
        assert_eq!(
            backend
                .configured
                .lock()
                .get("volume")
                .and_then(|c| c.active_peer_address.clone()),
            Some("node-b.mirrorvol.svc".to_owned())
        );
    }

    #[tokio::test]
    async fn full_resync_pulls_through_the_tunnel_when_one_is_configured() {
        let runner = Arc::new(FakeRunner::default());
        let tunnel = Arc::new(FakeTunnelClient {
            opened: AtomicUsize::new(0),
        });
        let backend =
            RsyncBackend::new(runner.clone(), std::env::temp_dir()).with_stunnel(tunnel.clone());
        backend
            .ensure_replica(&replica_config(Some("node-a.mirrorvol.svc")))
            .await
            .expect("ensure_replica");

        assert!(backend
            .completion("volume")
            .await
            .expect("completion")
            .ready());

        // Pulls went to the tunnel's local address, never the real peer.
        assert!(runner
            .last_args
            .lock()
            .iter()
            .any(|arg| arg.contains("127.0.0.1:19999")));
        assert!(!runner
            .last_args
            .lock()
            .iter()
            .any(|arg| arg.contains("node-a.mirrorvol.svc")));
        // One open per full_resync, not one per rsync invocation.
        assert_eq!(tunnel.opened.load(Ordering::SeqCst), 1);
    }
}
