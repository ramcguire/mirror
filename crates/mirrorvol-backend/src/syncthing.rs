//! Syncthing implementation of [`LocalBackend`], talking to a sibling
//! Syncthing instance over its local REST API.

use std::collections::HashMap;
use std::path::PathBuf;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    BackendError, CompletionStatus, LocalBackend, LockState, ReplicaConfig, WriterOperation,
};

const LOCK_FILE_NAME: &str = ".mirror-lock";

/// Env var *names* for reaching this node's own sibling Syncthing instance
/// — read by both mirrorvol-agent's reconcile loop and mirrorvol-csi's
/// attach-hook readiness check, so a typo in one place would silently
/// break the contract with the other rather than failing to compile.
pub mod env {
    /// Base URL of the local Syncthing REST API.
    pub const SYNCTHING_BASE_URL: &str = "SYNCTHING_BASE_URL";
    pub const SYNCTHING_BASE_URL_DEFAULT: &str = "http://localhost:8384";

    /// Passed as [`api_key_from_env`](super::api_key_from_env)'s `env_var`
    /// — the API key itself, checked before [`SYNCTHING_API_KEY_FILE`].
    pub const SYNCTHING_API_KEY: &str = "SYNCTHING_API_KEY";

    /// Passed as [`api_key_from_env`](super::api_key_from_env)'s
    /// `file_env_var` — path to the file `mirrorvol-agent provision`
    /// writes the key to (see `mirrorvol-agent`'s `provision.rs`), shared
    /// via an `emptyDir` with the `syncthing`/`mirrorvol-csi` containers.
    pub const SYNCTHING_API_KEY_FILE: &str = "SYNCTHING_API_KEY_FILE";
}

#[derive(Debug, Error)]
pub enum SyncthingError {
    #[error("unknown local folder {0}")]
    NotFound(String),
    #[error("local data root was not configured")]
    NoLocalDataRoot,
    #[error("HTTP request to {url} failed: {source}")]
    Http { url: String, source: reqwest::Error },
    #[error("Syncthing returned {status} from {url}: {body}")]
    Response {
        url: String,
        status: reqwest::StatusCode,
        body: String,
    },
    #[error("filesystem operation on {path} failed: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid writer lock at {path}: {source}")]
    LockDecode {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("neither {env_var} nor {file_env_var} is set")]
    MissingApiKey {
        env_var: String,
        file_env_var: String,
    },
}

impl From<SyncthingError> for BackendError {
    fn from(value: SyncthingError) -> Self {
        BackendError::Request(value.to_string())
    }
}

/// Endpoint for this process's sibling Syncthing instance only.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LocalEndpoint {
    pub base_url: String,
    pub api_key: String,
}

/// Reads the Syncthing API key from `env_var`, or failing that, from the
/// trimmed contents of the file named by `file_env_var`. The file fallback
/// exists because a DaemonSet's shared pod template can't mount a per-node
/// Secret via `secretKeyRef.name` — the key is written to a shared file
/// instead.
pub fn api_key_from_env(env_var: &str, file_env_var: &str) -> Result<String, SyncthingError> {
    if let Ok(value) = std::env::var(env_var) {
        return Ok(value);
    }
    let path = std::env::var(file_env_var).map_err(|_| SyncthingError::MissingApiKey {
        env_var: env_var.to_owned(),
        file_env_var: file_env_var.to_owned(),
    })?;
    std::fs::read_to_string(&path)
        .map(|contents| contents.trim().to_owned())
        .map_err(|source| SyncthingError::Io {
            path: PathBuf::from(path),
            source,
        })
}

pub async fn fetch_device_id(
    http: &reqwest::Client,
    base_url: &str,
    api_key: &str,
) -> Result<String, SyncthingError> {
    #[derive(Deserialize)]
    struct Status {
        #[serde(rename = "myID")]
        my_id: String,
    }
    let url = format!("{base_url}/rest/system/status");
    let response = http
        .get(&url)
        .header("X-API-Key", api_key)
        .send()
        .await
        .map_err(|source| SyncthingError::Http {
            url: url.clone(),
            source,
        })?;
    check_status(response, url.clone())
        .await?
        .json()
        .await
        .map_err(|source| SyncthingError::Http { url, source })
        .map(|status: Status| status.my_id)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WriterLock {
    #[serde(rename = "volumeID")]
    volume_id: String,
    #[serde(rename = "writerNode")]
    writer_node: String,
    epoch: u64,
    #[serde(rename = "operationID")]
    operation_id: u64,
}

/// Which [`conflict_files`](crate::LocalBackend::conflict_files)
/// implementation a [`SyncthingBackend`] uses — kept as two independently
/// testable code paths rather than merged heuristics, so each can be
/// deployed and compared on its own footing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConflictDetection {
    /// Recursive local filesystem walk, pruned to skip Syncthing's own
    /// `.stfolder`/`.stversions` metadata directories, and gated on
    /// `/rest/db/status`'s local index `sequence` number so an unchanged
    /// folder reuses the last scan instead of walking again. Exact — lists
    /// whatever conflict files currently exist on disk — at the cost of a
    /// local directory walk on a cache miss.
    #[default]
    Filesystem,
    /// Syncthing's own `/rest/events` buffer, filtered for `ItemFinished`
    /// events naming a `*.sync-conflict-*` item — no filesystem I/O at all.
    /// Approximate: only sees whatever Syncthing still has buffered, so a
    /// conflict file that predates the buffer's current window won't show
    /// up unless a later event also touched it. Lighter-weight than
    /// [`Filesystem`](Self::Filesystem), not a guaranteed-equivalent
    /// replacement for it.
    SyncthingEvents,
}

pub struct SyncthingBackend {
    http: reqwest::Client,
    endpoint: LocalEndpoint,
    data_root: PathBuf,
    conflict_detection: ConflictDetection,
    // Generation and local path are always written together (`ensure_replica`)
    // and describe the same "last applied config" for a volume, so one map
    // keeps them from ever disagreeing with each other.
    configured: Mutex<HashMap<String, (u64, PathBuf)>>,
    // ConflictDetection::Filesystem's cache: the local index sequence number
    // last scanned against, and what that scan found. A folder whose
    // sequence hasn't moved since is known unchanged without walking again.
    conflict_scan_cache: Mutex<HashMap<String, (i64, Vec<String>)>>,
    // set_ignore_patterns' cache — called unconditionally on every
    // reconcile, so this is what keeps an unchanged pattern list from
    // re-triggering a full Syncthing rescan every tick.
    ignore_patterns: Mutex<HashMap<String, Vec<String>>>,
}

impl SyncthingBackend {
    pub fn new(endpoint: LocalEndpoint, data_root: impl Into<PathBuf>) -> Self {
        Self {
            http: reqwest::Client::new(),
            endpoint,
            data_root: data_root.into(),
            conflict_detection: ConflictDetection::default(),
            configured: Mutex::new(HashMap::new()),
            conflict_scan_cache: Mutex::new(HashMap::new()),
            ignore_patterns: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_conflict_detection(mut self, mode: ConflictDetection) -> Self {
        self.conflict_detection = mode;
        self
    }

    fn folder_url(&self, volume_id: &str) -> String {
        format!("{}/rest/config/folders/{volume_id}", self.endpoint.base_url)
    }

    fn device_url(&self, device_id: &str) -> String {
        format!("{}/rest/config/devices/{device_id}", self.endpoint.base_url)
    }

    /// `folder` is a query parameter here, not a path segment like
    /// `folder_url`'s — this is `/rest/db/...` (the live database), not
    /// `/rest/config/...` (the folder's own config object).
    fn ignores_url(&self, volume_id: &str) -> String {
        format!(
            "{}/rest/db/ignores?folder={volume_id}",
            self.endpoint.base_url
        )
    }

    /// Registers a peer device if this instance doesn't already know it.
    /// Referencing an unregistered device ID in a folder's `devices` list
    /// isn't enough on its own — Syncthing only actually syncs with devices
    /// present in its global device list. A `GET` first (rather than an
    /// unconditional `PUT`) so an already-known device's other config (e.g.
    /// pause state, an operator-set name) isn't clobbered every reconcile.
    async fn ensure_device_registered(
        &self,
        device_id: &str,
        address: Option<&str>,
    ) -> Result<(), BackendError> {
        let url = self.device_url(device_id);
        let response = self
            .http
            .get(&url)
            .header("X-API-Key", &self.endpoint.api_key)
            .send()
            .await
            .map_err(|source| SyncthingError::Http {
                url: url.clone(),
                source,
            })?;
        if response.status() != reqwest::StatusCode::NOT_FOUND {
            check_status(response, url)
                .await
                .map_err(BackendError::from)?;
            return Ok(());
        }
        // A missing address falls back to Syncthing's own discovery,
        // `["dynamic"]` (global discovery server + LAN multicast). An
        // explicit address is preferred whenever one is known: a small,
        // fixed candidate set doesn't need discovery, and it isn't
        // reliable on every network.
        let addresses = address.map_or_else(
            || vec!["dynamic".to_owned()],
            |address| vec![address.to_owned()],
        );
        let response = self
            .http
            .put(&url)
            .header("X-API-Key", &self.endpoint.api_key)
            .json(&serde_json::json!({ "deviceID": device_id, "name": device_id, "addresses": addresses }))
            .send()
            .await
            .map_err(|source| SyncthingError::Http {
                url: url.clone(),
                source,
            })?;
        check_status(response, url)
            .await
            .map_err(BackendError::from)?;
        Ok(())
    }

    fn replica_dir(&self, volume_id: &str) -> PathBuf {
        self.configured
            .lock()
            .get(volume_id)
            .map(|(_generation, path)| path.clone())
            .unwrap_or_else(|| self.data_root.join(volume_id))
    }

    fn lock_path(&self, volume_id: &str) -> PathBuf {
        self.replica_dir(volume_id).join(LOCK_FILE_NAME)
    }

    async fn patch_folder_type(
        &self,
        volume_id: &str,
        folder_type: &str,
    ) -> Result<(), BackendError> {
        let url = self.folder_url(volume_id);
        let response = self
            .http
            .patch(&url)
            .header("X-API-Key", &self.endpoint.api_key)
            .json(&serde_json::json!({ "type": folder_type }))
            .send()
            .await
            .map_err(|source| SyncthingError::Http {
                url: url.clone(),
                source,
            })?;
        check_status(response, url)
            .await
            .map_err(BackendError::from)?;
        Ok(())
    }

    /// This folder's local index sequence number — increases on every local
    /// change Syncthing's scanner observes (including a conflict file
    /// appearing or being deleted), stays put otherwise. The cheap "did
    /// anything change" check before paying for a directory walk.
    async fn folder_sequence(&self, volume_id: &str) -> Result<i64, BackendError> {
        #[derive(Deserialize)]
        struct Status {
            sequence: i64,
        }
        let url = format!(
            "{}/rest/db/status?folder={volume_id}",
            self.endpoint.base_url
        );
        let response = self
            .http
            .get(&url)
            .header("X-API-Key", &self.endpoint.api_key)
            .send()
            .await
            .map_err(|source| SyncthingError::Http {
                url: url.clone(),
                source,
            })?;
        check_status(response, url.clone())
            .await
            .map_err(BackendError::from)?
            .json::<Status>()
            .await
            .map_err(|source| SyncthingError::Http { url, source }.into())
            .map(|status| status.sequence)
    }

    /// Walks `replica_dir`, pruned to skip `.stfolder`/`.stversions`
    /// (never where a live conflict file lives, and `.stversions` can
    /// accumulate without bound), reusing the last scan outright when
    /// `folder_sequence` hasn't moved. A `folder_sequence` failure just
    /// skips the cache rather than failing the whole call — caching is an
    /// optimization here, not a correctness dependency.
    async fn conflict_files_via_filesystem(
        &self,
        volume_id: &str,
    ) -> Result<Vec<String>, BackendError> {
        if let Ok(sequence) = self.folder_sequence(volume_id).await {
            if let Some((cached_sequence, cached_files)) =
                self.conflict_scan_cache.lock().get(volume_id)
            {
                if *cached_sequence == sequence {
                    return Ok(cached_files.clone());
                }
            }
            let files = self.scan_conflict_files(volume_id).await?;
            self.conflict_scan_cache
                .lock()
                .insert(volume_id.to_owned(), (sequence, files.clone()));
            return Ok(files);
        }
        self.scan_conflict_files(volume_id).await
    }

    async fn scan_conflict_files(&self, volume_id: &str) -> Result<Vec<String>, BackendError> {
        let dir = self.replica_dir(volume_id);
        // walkdir is a synchronous, blocking directory walk — offloaded so
        // it never blocks this process's async runtime. Errors below (an
        // unreadable subdirectory, a symlink loop) are skipped, not
        // propagated — a partial conflict listing is still useful signal,
        // and this must never itself become a reason to fail a reconcile.
        tokio::task::spawn_blocking(move || {
            if !dir.exists() {
                return Vec::new();
            }
            walkdir::WalkDir::new(&dir)
                .into_iter()
                .filter_entry(|entry| {
                    entry.file_type().is_file()
                        || !matches!(
                            entry.file_name().to_str(),
                            Some(".stfolder" | ".stversions")
                        )
                })
                .filter_map(|entry| entry.ok())
                .filter(|entry| {
                    entry.file_type().is_file()
                        && entry
                            .file_name()
                            .to_str()
                            .is_some_and(|name| name.contains(".sync-conflict-"))
                })
                .map(|entry| {
                    entry
                        .path()
                        .strip_prefix(&dir)
                        .unwrap_or(entry.path())
                        .to_string_lossy()
                        .into_owned()
                })
                .collect()
        })
        .await
        .map_err(|error| BackendError::Request(format!("conflict_files scan panicked: {error}")))
    }

    /// No filesystem I/O — reads whatever `ItemFinished` events Syncthing
    /// still has buffered for this folder, keeping each conflict-named
    /// item's most recent action (`update` vs `delete`) and reporting only
    /// the ones not last deleted.
    async fn conflict_files_via_events(
        &self,
        volume_id: &str,
    ) -> Result<Vec<String>, BackendError> {
        #[derive(Deserialize)]
        struct Event {
            #[serde(rename = "type")]
            type_: String,
            data: EventData,
        }
        #[derive(Deserialize, Default)]
        struct EventData {
            folder: Option<String>,
            item: Option<String>,
            action: Option<String>,
        }
        let url = format!("{}/rest/events?events=ItemFinished", self.endpoint.base_url);
        let response = self
            .http
            .get(&url)
            .header("X-API-Key", &self.endpoint.api_key)
            .send()
            .await
            .map_err(|source| SyncthingError::Http {
                url: url.clone(),
                source,
            })?;
        let events: Vec<Event> = check_status(response, url.clone())
            .await
            .map_err(BackendError::from)?
            .json()
            .await
            .map_err(|source| SyncthingError::Http { url, source })?;

        // Folded in event order, so a later event for the same item
        // (typically its own eventual delete once the conflict is resolved)
        // overwrites an earlier one — last known action wins.
        let mut last_action: HashMap<String, String> = HashMap::new();
        for event in events {
            if event.type_ != "ItemFinished" || event.data.folder.as_deref() != Some(volume_id) {
                continue;
            }
            let Some(item) = event.data.item else {
                continue;
            };
            if !item.contains(".sync-conflict-") {
                continue;
            }
            last_action.insert(item, event.data.action.unwrap_or_default());
        }
        Ok(last_action
            .into_iter()
            .filter(|(_, action)| action != "delete")
            .map(|(item, _)| item)
            .collect())
    }
}

#[async_trait]
impl LocalBackend for SyncthingBackend {
    type Completion = SyncthingCompletion;

    async fn ensure_replica(&self, config: &ReplicaConfig) -> Result<(), BackendError> {
        if self
            .configured
            .lock()
            .get(&config.volume_id)
            .is_some_and(|(generation, _path)| *generation == config.generation)
        {
            return Ok(());
        }

        for device_id in &config.peer_device_ids {
            self.ensure_device_registered(
                device_id,
                config.peer_addresses.get(device_id).map(String::as_str),
            )
            .await?;
        }

        let url = self.folder_url(&config.volume_id);
        let response = self
            .http
            .get(&url)
            .header("X-API-Key", &self.endpoint.api_key)
            .send()
            .await
            .map_err(|source| SyncthingError::Http {
                url: url.clone(),
                source,
            })?;
        let devices: Vec<_> = config
            .peer_device_ids
            .iter()
            .map(|device_id| serde_json::json!({ "deviceID": device_id }))
            .collect();
        let response = if response.status() == reqwest::StatusCode::NOT_FOUND {
            self.http
                .put(&url)
                .header("X-API-Key", &self.endpoint.api_key)
                .json(&serde_json::json!({
                    "id": config.volume_id,
                    "path": config.local_path,
                    "type": "receiveonly",
                    "devices": devices,
                }))
                .send()
                .await
        } else {
            check_status(response, url.clone())
                .await
                .map_err(BackendError::from)?;
            self.http
                .patch(&url)
                .header("X-API-Key", &self.endpoint.api_key)
                .json(&serde_json::json!({
                    "path": config.local_path,
                    "devices": devices,
                }))
                .send()
                .await
        }
        .map_err(|source| SyncthingError::Http {
            url: url.clone(),
            source,
        })?;
        check_status(response, url)
            .await
            .map_err(BackendError::from)?;
        self.configured.lock().insert(
            config.volume_id.clone(),
            (config.generation, PathBuf::from(&config.local_path)),
        );
        Ok(())
    }

    async fn is_writer(&self, volume_id: &str) -> Result<bool, BackendError> {
        #[derive(Deserialize)]
        struct Folder {
            #[serde(rename = "type")]
            folder_type: String,
        }
        let url = self.folder_url(volume_id);
        let response = self
            .http
            .get(&url)
            .header("X-API-Key", &self.endpoint.api_key)
            .send()
            .await
            .map_err(|source| SyncthingError::Http {
                url: url.clone(),
                source,
            })?;
        let folder: Folder = check_status(response, url.clone())
            .await
            .map_err(BackendError::from)?
            .json()
            .await
            .map_err(|source| SyncthingError::Http { url, source })?;
        Ok(folder.folder_type == "sendreceive")
    }

    async fn completion(&self, volume_id: &str) -> Result<Self::Completion, BackendError> {
        // Omitting `device` asks Syncthing for this local replica's completion.
        let url = format!(
            "{}/rest/db/completion?folder={volume_id}",
            self.endpoint.base_url
        );
        let response = self
            .http
            .get(&url)
            .header("X-API-Key", &self.endpoint.api_key)
            .send()
            .await
            .map_err(|source| SyncthingError::Http {
                url: url.clone(),
                source,
            })?;
        check_status(response, url.clone())
            .await
            .map_err(BackendError::from)?
            .json()
            .await
            .map_err(|source| SyncthingError::Http { url, source }.into())
    }

    async fn lock_state(&self, volume_id: &str) -> Result<LockState, BackendError> {
        let path = self.lock_path(volume_id);
        match tokio::fs::read(&path).await {
            Ok(contents) => {
                let lock = serde_json::from_slice::<WriterLock>(&contents)
                    .map_err(|source| SyncthingError::LockDecode { path, source })?;
                if lock.volume_id != volume_id {
                    return Err(BackendError::Request(
                        "writer lock belongs to another volume".to_owned(),
                    ));
                }
                Ok(LockState::Present {
                    epoch: Some(lock.epoch),
                })
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(LockState::Absent),
            Err(source) => Err(SyncthingError::Io { path, source }.into()),
        }
    }

    async fn acquire_writer(
        &self,
        volume_id: &str,
        operation: &WriterOperation,
    ) -> Result<(), BackendError> {
        if !matches!(self.lock_state(volume_id).await?, LockState::Absent) {
            return Err(BackendError::LockPresent);
        }
        self.patch_folder_type(volume_id, "sendreceive").await?;
        let path = self.lock_path(volume_id);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|source| SyncthingError::Io {
                    path: parent.to_owned(),
                    source,
                })?;
        }
        let lock = WriterLock {
            volume_id: volume_id.to_owned(),
            writer_node: operation.writer_node.clone(),
            epoch: operation.epoch,
            operation_id: operation.operation_id,
        };
        let contents = serde_json::to_vec(&lock).expect("WriterLock serialization is infallible");
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(mut file) => {
                use tokio::io::AsyncWriteExt;
                file.write_all(&contents)
                    .await
                    .map_err(|source| SyncthingError::Io { path, source })?;
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                self.enforce_standby(volume_id).await?;
                Err(BackendError::LockPresent)
            }
            Err(source) => {
                self.enforce_standby(volume_id).await?;
                Err(SyncthingError::Io { path, source }.into())
            }
        }
    }

    async fn restore_writer(&self, volume_id: &str, epoch: u64) -> Result<(), BackendError> {
        if !matches!(
            self.lock_state(volume_id).await?,
            LockState::Present {
                epoch: Some(lock_epoch)
            } if lock_epoch == epoch
        ) {
            return Err(BackendError::LockPresent);
        }
        self.patch_folder_type(volume_id, "sendreceive").await
    }

    async fn release_writer(
        &self,
        volume_id: &str,
        _operation: &WriterOperation,
    ) -> Result<(), BackendError> {
        let path = self.lock_path(volume_id);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(SyncthingError::Io { path, source }.into()),
        }
    }

    async fn enforce_standby(&self, volume_id: &str) -> Result<(), BackendError> {
        self.patch_folder_type(volume_id, "receiveonly").await
    }

    async fn enable_send_receive(&self, volume_id: &str) -> Result<(), BackendError> {
        self.patch_folder_type(volume_id, "sendreceive").await
    }

    async fn conflict_files(&self, volume_id: &str) -> Result<Vec<String>, BackendError> {
        match self.conflict_detection {
            ConflictDetection::Filesystem => self.conflict_files_via_filesystem(volume_id).await,
            ConflictDetection::SyncthingEvents => self.conflict_files_via_events(volume_id).await,
        }
    }

    /// `POST /rest/db/ignores` rewrites `.stignore` atomically and triggers
    /// an immediate rescan — preferred over writing the file directly,
    /// which would only take effect on Syncthing's own next scan interval
    /// and gives no validation error back for a malformed pattern. `POST`,
    /// not `PUT`: `/rest/db/ignores` is a live-database endpoint, and those
    /// reject `PUT` with 405.
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
        let url = self.ignores_url(volume_id);
        let response = self
            .http
            .post(&url)
            .header("X-API-Key", &self.endpoint.api_key)
            .json(&serde_json::json!({ "ignore": patterns }))
            .send()
            .await
            .map_err(|source| SyncthingError::Http {
                url: url.clone(),
                source,
            })?;
        check_status(response, url)
            .await
            .map_err(BackendError::from)?;
        self.ignore_patterns
            .lock()
            .insert(volume_id.to_owned(), patterns.to_vec());
        Ok(())
    }
}

async fn check_status(
    response: reqwest::Response,
    url: String,
) -> Result<reqwest::Response, SyncthingError> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let body = response.text().await.unwrap_or_default();
    Err(SyncthingError::Response { url, status, body })
}

#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize)]
pub struct SyncthingCompletion {
    pub completion: f64,
    #[serde(rename = "needItems")]
    pub need_items: u64,
    #[serde(rename = "needDeletes")]
    pub need_deletes: u64,
}

impl CompletionStatus for SyncthingCompletion {
    fn ready(&self) -> bool {
        self.completion >= 100.0 && self.need_items == 0 && self.need_deletes == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn completion_requires_delete_queue_to_be_empty() {
        assert!(!SyncthingCompletion {
            completion: 100.0,
            need_items: 0,
            need_deletes: 1,
        }
        .ready());
    }

    #[tokio::test]
    async fn completion_queries_the_local_folder_without_a_device_parameter() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/rest/db/completion"))
            .and(query_param("folder", "volume"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "completion": 100.0,
                "needItems": 0,
                "needDeletes": 0
            })))
            .expect(1)
            .mount(&server)
            .await;
        let backend = SyncthingBackend::new(
            LocalEndpoint {
                base_url: server.uri(),
                api_key: "key".to_owned(),
            },
            tempfile::tempdir().expect("tempdir").path(),
        );
        assert!(backend
            .completion("volume")
            .await
            .expect("completion")
            .ready());
    }

    #[tokio::test]
    async fn ensure_replica_registers_unknown_peer_devices() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/rest/config/devices/PEER1"))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/rest/config/devices/PEER1"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/rest/config/folders/volume"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/rest/config/folders/volume"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let backend = SyncthingBackend::new(
            LocalEndpoint {
                base_url: server.uri(),
                api_key: "key".to_owned(),
            },
            tempfile::tempdir().expect("tempdir").path(),
        );
        backend
            .ensure_replica(&ReplicaConfig {
                volume_id: "volume".to_owned(),
                local_path: "/data/volume".to_owned(),
                peer_device_ids: vec!["PEER1".to_owned()],
                peer_addresses: std::collections::BTreeMap::new(),
                generation: 1,
                ignore_patterns: vec![],
            })
            .await
            .expect("ensure_replica");
    }

    #[tokio::test]
    async fn ensure_replica_registers_a_known_address_instead_of_leaving_it_dynamic() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/rest/config/devices/PEER1"))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/rest/config/devices/PEER1"))
            .and(wiremock::matchers::body_partial_json(serde_json::json!({
                "addresses": ["tcp://node-b:22000"]
            })))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/rest/config/folders/volume"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/rest/config/folders/volume"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let backend = SyncthingBackend::new(
            LocalEndpoint {
                base_url: server.uri(),
                api_key: "key".to_owned(),
            },
            tempfile::tempdir().expect("tempdir").path(),
        );
        backend
            .ensure_replica(&ReplicaConfig {
                volume_id: "volume".to_owned(),
                local_path: "/data/volume".to_owned(),
                peer_device_ids: vec!["PEER1".to_owned()],
                peer_addresses: std::collections::BTreeMap::from([(
                    "PEER1".to_owned(),
                    "tcp://node-b:22000".to_owned(),
                )]),
                generation: 1,
                ignore_patterns: vec![],
            })
            .await
            .expect("ensure_replica");
        // wiremock's `.expect(1)` on the addresses-matching PUT mock above
        // already fails the test if that exact body was never sent — the
        // real assertion here is implicit in the mock server's own
        // verification on drop.
    }

    #[tokio::test]
    async fn enable_send_receive_patches_the_folder_unconditionally() {
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/rest/config/folders/volume"))
            .and(wiremock::matchers::body_partial_json(
                serde_json::json!({ "type": "sendreceive" }),
            ))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let backend = SyncthingBackend::new(
            LocalEndpoint {
                base_url: server.uri(),
                api_key: "key".to_owned(),
            },
            tempfile::tempdir().expect("tempdir").path(),
        );
        backend
            .enable_send_receive("volume")
            .await
            .expect("enable_send_receive");
    }

    #[tokio::test]
    async fn set_ignore_patterns_puts_the_pattern_list_to_the_db_ignores_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/rest/db/ignores"))
            .and(wiremock::matchers::query_param("folder", "volume"))
            .and(wiremock::matchers::body_partial_json(serde_json::json!({
                "ignore": ["*.tmp", "cache/"]
            })))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let backend = SyncthingBackend::new(
            LocalEndpoint {
                base_url: server.uri(),
                api_key: "key".to_owned(),
            },
            tempfile::tempdir().expect("tempdir").path(),
        );
        backend
            .set_ignore_patterns("volume", &["*.tmp".to_owned(), "cache/".to_owned()])
            .await
            .expect("set_ignore_patterns");
    }

    #[tokio::test]
    async fn conflict_files_finds_planted_conflicts_and_ignores_ordinary_files() {
        let root = tempfile::tempdir().expect("tempdir");
        let replica = root.path().join("volume");
        std::fs::create_dir_all(replica.join("sub")).expect("mkdir");
        std::fs::write(replica.join("plain.txt"), b"data").expect("write");
        std::fs::write(
            replica
                .join("sub")
                .join("file.sync-conflict-20260101-120000-NODE.txt"),
            b"conflict",
        )
        .expect("write");
        let backend = SyncthingBackend::new(
            LocalEndpoint {
                base_url: "http://unused".to_owned(),
                api_key: "key".to_owned(),
            },
            root.path(),
        );
        let conflicts = backend.conflict_files("volume").await.expect("scan");
        assert_eq!(conflicts.len(), 1);
        assert!(conflicts[0].contains("sync-conflict"));
    }

    #[tokio::test]
    async fn conflict_files_is_empty_for_a_replica_never_configured_locally() {
        let backend = SyncthingBackend::new(
            LocalEndpoint {
                base_url: "http://unused".to_owned(),
                api_key: "key".to_owned(),
            },
            tempfile::tempdir().expect("tempdir").path(),
        );
        assert!(backend
            .conflict_files("never-seen")
            .await
            .expect("scan")
            .is_empty());
    }

    #[tokio::test]
    async fn conflict_files_prunes_syncthing_metadata_directories() {
        let root = tempfile::tempdir().expect("tempdir");
        let replica = root.path().join("volume");
        // A conflict-named file planted inside .stversions (Syncthing's own
        // version-history trash can) must never surface — it isn't a live
        // conflict, and this directory can otherwise grow without bound.
        std::fs::create_dir_all(replica.join(".stversions")).expect("mkdir");
        std::fs::write(
            replica
                .join(".stversions")
                .join("old.sync-conflict-20250101-000000-NODE.txt"),
            b"stale",
        )
        .expect("write");
        std::fs::create_dir_all(replica.join(".stfolder")).expect("mkdir");
        std::fs::write(
            replica
                .join(".stfolder")
                .join("also.sync-conflict-20250101-000000-NODE.txt"),
            b"stale",
        )
        .expect("write");
        std::fs::write(
            replica.join("live.sync-conflict-20260101-120000-NODE.txt"),
            b"live",
        )
        .expect("write");
        // No mock server mounted — folder_sequence fails and
        // conflict_files_via_filesystem falls back to an uncached scan,
        // which is all this test needs.
        let backend = SyncthingBackend::new(
            LocalEndpoint {
                base_url: "http://unused".to_owned(),
                api_key: "key".to_owned(),
            },
            root.path(),
        );
        let conflicts = backend.conflict_files("volume").await.expect("scan");
        assert_eq!(
            conflicts,
            vec!["live.sync-conflict-20260101-120000-NODE.txt".to_owned()]
        );
    }

    #[tokio::test]
    async fn conflict_files_reuses_the_cached_scan_while_the_sequence_is_unchanged() {
        let root = tempfile::tempdir().expect("tempdir");
        let replica = root.path().join("volume");
        std::fs::create_dir_all(&replica).expect("mkdir");
        std::fs::write(
            replica.join("a.sync-conflict-20260101-120000-NODE.txt"),
            b"conflict",
        )
        .expect("write");

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/rest/db/status"))
            .and(query_param("folder", "volume"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "sequence": 42 })),
            )
            .mount(&server)
            .await;
        let backend = SyncthingBackend::new(
            LocalEndpoint {
                base_url: server.uri(),
                api_key: "key".to_owned(),
            },
            root.path(),
        );

        let first = backend.conflict_files("volume").await.expect("first scan");
        assert_eq!(first.len(), 1);

        // The conflict is resolved on disk, but the mocked sequence number
        // hasn't moved — the cached (now stale) result must still come back
        // rather than triggering a fresh walk.
        std::fs::remove_file(replica.join("a.sync-conflict-20260101-120000-NODE.txt"))
            .expect("remove");
        let second = backend.conflict_files("volume").await.expect("cached scan");
        assert_eq!(
            second, first,
            "unchanged sequence must reuse the cached scan"
        );
    }

    #[tokio::test]
    async fn conflict_files_via_events_ignores_other_folders_and_deleted_items() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/rest/events"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {
                    "id": 1,
                    "type": "ItemFinished",
                    "data": { "folder": "volume", "item": "a.sync-conflict-1", "action": "update" }
                },
                {
                    "id": 2,
                    "type": "ItemFinished",
                    "data": { "folder": "other-volume", "item": "b.sync-conflict-1", "action": "update" }
                },
                {
                    "id": 3,
                    "type": "ItemFinished",
                    "data": { "folder": "volume", "item": "c.sync-conflict-1", "action": "update" }
                },
                {
                    "id": 4,
                    "type": "ItemFinished",
                    "data": { "folder": "volume", "item": "c.sync-conflict-1", "action": "delete" }
                },
                {
                    "id": 5,
                    "type": "ItemFinished",
                    "data": { "folder": "volume", "item": "plain.txt", "action": "update" }
                }
            ])))
            .mount(&server)
            .await;
        let backend = SyncthingBackend::new(
            LocalEndpoint {
                base_url: server.uri(),
                api_key: "key".to_owned(),
            },
            tempfile::tempdir().expect("tempdir").path(),
        )
        .with_conflict_detection(ConflictDetection::SyncthingEvents);

        let conflicts = backend.conflict_files("volume").await.expect("events scan");
        // a.sync-conflict-1: only-ever-updated, same folder -> included.
        // b.sync-conflict-1: right name, wrong folder -> excluded.
        // c.sync-conflict-1: updated then deleted -> excluded.
        // plain.txt: not a conflict-shaped name at all -> excluded.
        assert_eq!(conflicts, vec!["a.sync-conflict-1".to_owned()]);
    }

    #[tokio::test]
    async fn ensure_replica_does_not_re_register_a_known_peer_device() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/rest/config/devices/PEER1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "deviceID": "PEER1",
                "name": "PEER1"
            })))
            .expect(1)
            .mount(&server)
            .await;
        // No PUT mock for the device endpoint at all — an unexpected PUT
        // there 404s against wiremock's default "no matching mock" response,
        // which `check_status` turns into an error and fails the test.
        Mock::given(method("GET"))
            .and(path("/rest/config/folders/volume"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/rest/config/folders/volume"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let backend = SyncthingBackend::new(
            LocalEndpoint {
                base_url: server.uri(),
                api_key: "key".to_owned(),
            },
            tempfile::tempdir().expect("tempdir").path(),
        );
        backend
            .ensure_replica(&ReplicaConfig {
                volume_id: "volume".to_owned(),
                local_path: "/data/volume".to_owned(),
                peer_device_ids: vec!["PEER1".to_owned()],
                peer_addresses: std::collections::BTreeMap::new(),
                generation: 1,
                ignore_patterns: vec![],
            })
            .await
            .expect("ensure_replica");
    }
}
