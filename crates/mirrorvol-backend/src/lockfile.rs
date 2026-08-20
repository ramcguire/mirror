//! Shared writer lock-file mechanics for backends that store their writer
//! lock as a file inside the local replica root (`.mirror-lock`) — used by
//! both `syncthing` and `rsync` today. Purely local filesystem I/O beyond
//! the one thing a backend supplies through [`RoleSetter`]: no knowledge of
//! either backend's own daemon/protocol otherwise.
//!
//! [`acquire_writer`]/[`restore_writer`] own the *ordering* around a role
//! flip and a lock write (e.g. "flip the role before creating the lock,
//! roll back to standby if creation loses the race") — that ordering used
//! to be hand-copied into each backend's own `LocalBackend::acquire_writer`/
//! `restore_writer`, which meant it could silently drift between them. It
//! lives here once instead, parameterized on [`RoleSetter`] so it still
//! knows nothing about how either backend actually flips its role.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{BackendError, LockState, WriterOperation};

pub const LOCK_FILE_NAME: &str = ".mirror-lock";

#[derive(Debug, Error)]
pub enum LockFileError {
    #[error("filesystem operation on {path} failed: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid writer lock at {path}: {source}")]
    Decode {
        path: PathBuf,
        source: serde_json::Error,
    },
}

impl From<LockFileError> for BackendError {
    fn from(value: LockFileError) -> Self {
        BackendError::Request(value.to_string())
    }
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

pub fn lock_path(replica_dir: &Path) -> PathBuf {
    replica_dir.join(LOCK_FILE_NAME)
}

/// Reads the lock file at `replica_dir`, if any. Rejects a lock that names
/// a different `volume_id` than expected — a sign this replica path was
/// reused across volumes.
pub async fn lock_state(replica_dir: &Path, volume_id: &str) -> Result<LockState, BackendError> {
    let path = lock_path(replica_dir);
    match tokio::fs::read(&path).await {
        Ok(contents) => {
            let lock = serde_json::from_slice::<WriterLock>(&contents)
                .map_err(|source| LockFileError::Decode { path, source })?;
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
        Err(source) => Err(LockFileError::Io { path, source }.into()),
    }
}

/// Atomically creates the lock file (create-exclusive) — the moment this
/// succeeds, this node holds the writer lock. Returns
/// [`BackendError::LockPresent`] if another writer already holds it,
/// without touching the file.
pub async fn create_lock(
    replica_dir: &Path,
    volume_id: &str,
    operation: &WriterOperation,
) -> Result<(), BackendError> {
    let path = lock_path(replica_dir);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|source| LockFileError::Io {
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
                .map_err(|source| LockFileError::Io { path, source })?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            Err(BackendError::LockPresent)
        }
        Err(source) => Err(LockFileError::Io { path, source }.into()),
    }
}

/// Removes the lock file. Idempotent — a missing file is not an error.
pub async fn remove_lock(replica_dir: &Path) -> Result<(), BackendError> {
    let path = lock_path(replica_dir);
    match tokio::fs::remove_file(&path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(LockFileError::Io { path, source }.into()),
    }
}

/// A backend's own way of flipping its local writer role — `syncthing`'s
/// folder type PATCH, `rsync`'s role file write. The only thing
/// [`acquire_writer`]/[`restore_writer`] below need from a backend, so
/// they can own the *ordering* around a role flip and a lock write without
/// knowing how either backend actually performs one.
#[async_trait]
pub trait RoleSetter: Send + Sync {
    /// `true` sets this node writer, `false` sets it standby.
    async fn set_writer_role(&self, volume_id: &str, writer: bool) -> Result<(), BackendError>;
}

/// The shared "acquire" protocol both backends' `LocalBackend::acquire_writer`
/// delegate to: re-check the lock is absent, flip the role, create the
/// lock, roll back to standby if lock creation loses the race. This is the
/// part that has to stay in lockstep across backends — see this module's
/// own doc comment — so it lives here once instead of being hand-copied.
pub async fn acquire_writer<R: RoleSetter + ?Sized>(
    role: &R,
    replica_dir: &Path,
    volume_id: &str,
    operation: &WriterOperation,
) -> Result<(), BackendError> {
    if !matches!(lock_state(replica_dir, volume_id).await?, LockState::Absent) {
        return Err(BackendError::LockPresent);
    }
    role.set_writer_role(volume_id, true).await?;
    match create_lock(replica_dir, volume_id, operation).await {
        Ok(()) => Ok(()),
        Err(error) => {
            role.set_writer_role(volume_id, false).await?;
            Err(error)
        }
    }
}

/// The shared "restore" protocol both backends' `LocalBackend::restore_writer`
/// delegate to: re-check the lock matches the expected epoch, flip the role
/// — no lock write, since this only runs when this node is already the
/// committed writer restarting after a fail-closed init.
pub async fn restore_writer<R: RoleSetter + ?Sized>(
    role: &R,
    replica_dir: &Path,
    volume_id: &str,
    epoch: u64,
) -> Result<(), BackendError> {
    if !matches!(
        lock_state(replica_dir, volume_id).await?,
        LockState::Present {
            epoch: Some(lock_epoch)
        } if lock_epoch == epoch
    ) {
        return Err(BackendError::LockPresent);
    }
    role.set_writer_role(volume_id, true).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;

    #[derive(Default)]
    struct FakeRoleSetter {
        calls: Mutex<Vec<bool>>,
    }

    impl FakeRoleSetter {
        fn calls(&self) -> Vec<bool> {
            self.calls.lock().clone()
        }
    }

    #[async_trait]
    impl RoleSetter for FakeRoleSetter {
        async fn set_writer_role(
            &self,
            _volume_id: &str,
            writer: bool,
        ) -> Result<(), BackendError> {
            self.calls.lock().push(writer);
            Ok(())
        }
    }

    fn operation() -> WriterOperation {
        WriterOperation {
            operation_id: 1,
            epoch: 1,
            writer_node: "node-a".to_owned(),
        }
    }

    #[tokio::test]
    async fn acquire_writer_flips_the_role_before_creating_the_lock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let role = FakeRoleSetter::default();
        acquire_writer(&role, dir.path(), "volume", &operation())
            .await
            .expect("acquire");
        assert_eq!(role.calls(), vec![true]);
        assert!(matches!(
            lock_state(dir.path(), "volume").await.expect("lock_state"),
            LockState::Present { epoch: Some(1) }
        ));
    }

    #[tokio::test]
    async fn acquire_writer_rejects_a_present_lock_without_flipping_the_role() {
        let dir = tempfile::tempdir().expect("tempdir");
        create_lock(dir.path(), "volume", &operation())
            .await
            .expect("seed lock");
        let role = FakeRoleSetter::default();
        let result = acquire_writer(&role, dir.path(), "volume", &operation()).await;
        assert!(matches!(result, Err(BackendError::LockPresent)));
        assert!(role.calls().is_empty());
    }

    /// Plants a foreign lock the instant it's asked to become writer —
    /// simulating another process's `acquire_writer` landing between this
    /// call's own `lock_state` pre-check and its `create_lock`, the exact
    /// crash/race window [`acquire_writer`]'s own doc comment describes.
    struct RacingRoleSetter<'a> {
        dir: &'a Path,
        calls: Mutex<Vec<bool>>,
    }

    #[async_trait]
    impl RoleSetter for RacingRoleSetter<'_> {
        async fn set_writer_role(&self, volume_id: &str, writer: bool) -> Result<(), BackendError> {
            self.calls.lock().push(writer);
            if writer {
                create_lock(self.dir, volume_id, &operation()).await.ok();
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn acquire_writer_rolls_back_to_standby_when_lock_creation_loses_the_race() {
        let dir = tempfile::tempdir().expect("tempdir");
        let role = RacingRoleSetter {
            dir: dir.path(),
            calls: Mutex::new(Vec::new()),
        };
        let result = acquire_writer(&role, dir.path(), "volume", &operation()).await;
        assert!(result.is_err());
        // Both role flips happened: writer (racing the lock), then standby
        // (the rollback) once create_lock discovered the race.
        assert_eq!(role.calls.lock().clone(), vec![true, false]);
    }

    #[tokio::test]
    async fn restore_writer_flips_the_role_back_to_writer_when_the_lock_matches() {
        let dir = tempfile::tempdir().expect("tempdir");
        create_lock(dir.path(), "volume", &operation())
            .await
            .expect("seed lock");
        let role = FakeRoleSetter::default();
        restore_writer(&role, dir.path(), "volume", 1)
            .await
            .expect("restore");
        assert_eq!(role.calls(), vec![true]);
    }

    #[tokio::test]
    async fn restore_writer_rejects_a_mismatched_epoch_without_flipping_the_role() {
        let dir = tempfile::tempdir().expect("tempdir");
        create_lock(dir.path(), "volume", &operation())
            .await
            .expect("seed lock");
        let role = FakeRoleSetter::default();
        let result = restore_writer(&role, dir.path(), "volume", 2).await;
        assert!(matches!(result, Err(BackendError::LockPresent)));
        assert!(role.calls().is_empty());
    }

    #[tokio::test]
    async fn restore_writer_rejects_an_absent_lock_without_flipping_the_role() {
        let dir = tempfile::tempdir().expect("tempdir");
        let role = FakeRoleSetter::default();
        let result = restore_writer(&role, dir.path(), "volume", 1).await;
        assert!(matches!(result, Err(BackendError::LockPresent)));
        assert!(role.calls().is_empty());
    }
}
