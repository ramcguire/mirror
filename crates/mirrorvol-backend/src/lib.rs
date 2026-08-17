//! Contract for a node-local replication backend, plus [`FakeBackend`] for
//! testing the controller and agent without a real one. Each backend is its
//! own submodule implementing [`LocalBackend`]; adding a backend means
//! adding a module, not touching this one.

pub mod syncthing;

use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BackendError {
    #[error("backend request failed: {0}")]
    Request(String),
    #[error("volume {0} was not configured locally")]
    NotFound(String),
    #[error("writer lock already exists")]
    LockPresent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockState {
    Present { epoch: Option<u64> },
    Absent,
}

pub trait CompletionStatus: Send + Sync {
    fn ready(&self) -> bool;
}

/// Declarative configuration for this agent's local replica only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaConfig {
    pub volume_id: String,
    pub local_path: String,
    pub peer_device_ids: Vec<String>,
    /// Keyed by device ID. A peer with no entry here falls back to the
    /// backend's own peer discovery, which a small fixed candidate set
    /// doesn't need and can't rely on across every network.
    pub peer_addresses: BTreeMap<String, String>,
    pub generation: u64,
    /// Gitignore-style patterns this replica should never sync.
    pub ignore_patterns: Vec<String>,
}

/// Immutable controller operation passed to one local backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterOperation {
    pub operation_id: u64,
    pub epoch: u64,
    pub writer_node: String,
}

/// Backend API deliberately has no remote-node parameter. A process holding
/// this trait cannot administer another node's backend instance.
#[async_trait]
pub trait LocalBackend: Send + Sync {
    type Completion: CompletionStatus + std::fmt::Debug;

    async fn ensure_replica(&self, config: &ReplicaConfig) -> Result<(), BackendError>;
    async fn is_writer(&self, volume_id: &str) -> Result<bool, BackendError>;
    async fn completion(&self, volume_id: &str) -> Result<Self::Completion, BackendError>;
    async fn lock_state(&self, volume_id: &str) -> Result<LockState, BackendError>;

    /// Enables only this node's writer role and atomically records the epoch.
    async fn acquire_writer(
        &self,
        volume_id: &str,
        operation: &WriterOperation,
    ) -> Result<(), BackendError>;

    /// Restores the committed local writer after a fail-closed backend restart.
    async fn restore_writer(&self, volume_id: &str, epoch: u64) -> Result<(), BackendError>;

    /// Removes this node's writer lock while it can still propagate the delete.
    async fn release_writer(
        &self,
        volume_id: &str,
        operation: &WriterOperation,
    ) -> Result<(), BackendError>;

    /// Makes only this node's replica non-writable.
    async fn enforce_standby(&self, volume_id: &str) -> Result<(), BackendError>;

    /// `bestEffort` only: unconditional `Send & Receive`, no lock/epoch
    /// semantics — unlike [`acquire_writer`](Self::acquire_writer), never
    /// rejected by a present lock (there is none) and safe to call on a
    /// node that's already writable.
    async fn enable_send_receive(&self, volume_id: &str) -> Result<(), BackendError>;

    /// `bestEffort` only: unresolved conflict file paths for this node's
    /// own local replica.
    async fn conflict_files(&self, volume_id: &str) -> Result<Vec<String>, BackendError>;

    /// Sets this node's local ignore-pattern list for `volume_id` —
    /// gitignore-style patterns this node's own backend should never sync
    /// in either direction (Syncthing's `.stignore`, for example).
    async fn set_ignore_patterns(
        &self,
        volume_id: &str,
        patterns: &[String],
    ) -> Result<(), BackendError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FakeCompletion {
    pub ready: bool,
}

impl CompletionStatus for FakeCompletion {
    fn ready(&self) -> bool {
        self.ready
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Call {
    EnsureReplica {
        volume_id: String,
    },
    AcquireWriter {
        volume_id: String,
        epoch: u64,
    },
    RestoreWriter {
        volume_id: String,
        epoch: u64,
    },
    ReleaseWriter {
        volume_id: String,
        operation_id: u64,
    },
    EnforceStandby {
        volume_id: String,
    },
    EnableSendReceive {
        volume_id: String,
    },
    SetIgnorePatterns {
        volume_id: String,
        patterns: Vec<String>,
    },
}

#[derive(Debug, Default)]
pub struct FakeBackend {
    completion: Mutex<HashMap<String, FakeCompletion>>,
    lock_state: Mutex<HashMap<String, LockState>>,
    writer_state: Mutex<HashMap<String, bool>>,
    conflict_files: Mutex<HashMap<String, Vec<String>>>,
    calls: Mutex<Vec<Call>>,
}

impl FakeBackend {
    pub fn set_completion(&self, volume_id: &str, ready: bool) {
        self.completion
            .lock()
            .insert(volume_id.to_owned(), FakeCompletion { ready });
    }

    pub fn set_lock_state(&self, volume_id: &str, state: LockState) {
        self.lock_state.lock().insert(volume_id.to_owned(), state);
    }

    pub fn set_writer(&self, volume_id: &str, writer: bool) {
        self.writer_state
            .lock()
            .insert(volume_id.to_owned(), writer);
    }

    pub fn set_conflict_files(&self, volume_id: &str, files: Vec<String>) {
        self.conflict_files
            .lock()
            .insert(volume_id.to_owned(), files);
    }

    pub fn calls(&self) -> Vec<Call> {
        self.calls.lock().clone()
    }
}

#[async_trait]
impl LocalBackend for FakeBackend {
    type Completion = FakeCompletion;

    async fn ensure_replica(&self, config: &ReplicaConfig) -> Result<(), BackendError> {
        self.calls.lock().push(Call::EnsureReplica {
            volume_id: config.volume_id.clone(),
        });
        self.writer_state
            .lock()
            .entry(config.volume_id.clone())
            .or_insert(false);
        Ok(())
    }

    async fn is_writer(&self, volume_id: &str) -> Result<bool, BackendError> {
        Ok(self
            .writer_state
            .lock()
            .get(volume_id)
            .copied()
            .unwrap_or(false))
    }

    async fn completion(&self, volume_id: &str) -> Result<Self::Completion, BackendError> {
        Ok(self
            .completion
            .lock()
            .get(volume_id)
            .copied()
            .unwrap_or(FakeCompletion { ready: false }))
    }

    async fn lock_state(&self, volume_id: &str) -> Result<LockState, BackendError> {
        Ok(self
            .lock_state
            .lock()
            .get(volume_id)
            .copied()
            .unwrap_or(LockState::Absent))
    }

    async fn acquire_writer(
        &self,
        volume_id: &str,
        operation: &WriterOperation,
    ) -> Result<(), BackendError> {
        if !matches!(self.lock_state(volume_id).await?, LockState::Absent) {
            return Err(BackendError::LockPresent);
        }
        self.calls.lock().push(Call::AcquireWriter {
            volume_id: volume_id.to_owned(),
            epoch: operation.epoch,
        });
        self.writer_state.lock().insert(volume_id.to_owned(), true);
        self.lock_state.lock().insert(
            volume_id.to_owned(),
            LockState::Present {
                epoch: Some(operation.epoch),
            },
        );
        Ok(())
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
        self.calls.lock().push(Call::RestoreWriter {
            volume_id: volume_id.to_owned(),
            epoch,
        });
        self.writer_state.lock().insert(volume_id.to_owned(), true);
        Ok(())
    }

    async fn release_writer(
        &self,
        volume_id: &str,
        operation: &WriterOperation,
    ) -> Result<(), BackendError> {
        self.calls.lock().push(Call::ReleaseWriter {
            volume_id: volume_id.to_owned(),
            operation_id: operation.operation_id,
        });
        self.lock_state
            .lock()
            .insert(volume_id.to_owned(), LockState::Absent);
        Ok(())
    }

    async fn enforce_standby(&self, volume_id: &str) -> Result<(), BackendError> {
        self.calls.lock().push(Call::EnforceStandby {
            volume_id: volume_id.to_owned(),
        });
        self.writer_state.lock().insert(volume_id.to_owned(), false);
        Ok(())
    }

    async fn enable_send_receive(&self, volume_id: &str) -> Result<(), BackendError> {
        self.calls.lock().push(Call::EnableSendReceive {
            volume_id: volume_id.to_owned(),
        });
        self.writer_state.lock().insert(volume_id.to_owned(), true);
        Ok(())
    }

    async fn conflict_files(&self, volume_id: &str) -> Result<Vec<String>, BackendError> {
        Ok(self
            .conflict_files
            .lock()
            .get(volume_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn set_ignore_patterns(
        &self,
        volume_id: &str,
        patterns: &[String],
    ) -> Result<(), BackendError> {
        self.calls.lock().push(Call::SetIgnorePatterns {
            volume_id: volume_id.to_owned(),
            patterns: patterns.to_vec(),
        });
        Ok(())
    }
}
