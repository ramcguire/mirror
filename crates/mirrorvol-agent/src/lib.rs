//! Per-node reconciliation for one local backend instance —
//! [`reconcile_local`], the only thing allowed to call a [`LocalBackend`],
//! and only ever against its own node.

use mirrorvol_api::{
    phase, role, Consistency, MirroredVolumeStatus, NodeSyncStatus, PromotionOperation,
};
use mirrorvol_backend::{
    CompletionStatus, LocalBackend, LockState, ReplicaConfig, WriterOperation,
};

fn writer_operation(operation: &PromotionOperation, node: &str) -> WriterOperation {
    WriterOperation {
        operation_id: operation.id,
        epoch: operation.epoch,
        writer_node: node.to_owned(),
    }
}

/// Bumps `BackendNode.status.identityGeneration` only on a genuine device
/// identity change. `existing` is this node's previously-registered
/// `(device_id, identity_generation)`. Unset -> `1`; unchanged device_id ->
/// steady; changed device_id -> `existing + 1`. `mirrorvol_controller::decide`
/// reacts to the resulting value changing.
pub fn next_identity_generation(existing: Option<(&str, u64)>, new_device_id: &str) -> u64 {
    match existing {
        None => 1,
        Some((old_device_id, generation)) if old_device_id == new_device_id => generation,
        Some((_, generation)) => generation + 1,
    }
}

/// Reconciles one node's local replica. `source_quiesced` must be true only
/// after the caller has proved both workload termination and local mount
/// teardown — ignored in `bestEffort`, which never drains or quiesces
/// anything. Passing `false` is fail-closed: the source lock remains
/// present. `pull_only` is `bestEffort`-only (see
/// [`StorageSpec::pull_only`](mirrorvol_api::StorageSpec::pull_only)) —
/// read only when `consistency` is [`Consistency::BestEffort`], ignored
/// for [`Consistency::Strict`], which is why it's its own parameter rather
/// than bundled into `consistency` itself: the two are independent fields
/// on the wire (`spec.consistency`, `spec.storage.pullOnly`), and keeping
/// them independent here means nothing has to re-pack them into a bespoke
/// sum type just to call this.
pub async fn reconcile_local<B: LocalBackend>(
    node: &str,
    replica: &ReplicaConfig,
    status: &MirroredVolumeStatus,
    consistency: Consistency,
    pull_only: bool,
    source_quiesced: bool,
    writer_pod_uids: Vec<String>,
    backend: &B,
) -> Result<NodeSyncStatus, String> {
    backend
        .ensure_replica(replica)
        .await
        .map_err(|error| error.to_string())?;
    backend
        .set_ignore_patterns(&replica.volume_id, &replica.ignore_patterns)
        .await
        .map_err(|error| error.to_string())?;
    match consistency {
        Consistency::Strict => {
            reconcile_local_strict(
                node,
                replica,
                status,
                source_quiesced,
                writer_pod_uids,
                backend,
            )
            .await
        }
        Consistency::BestEffort => {
            reconcile_local_best_effort(node, replica, status, pull_only, writer_pod_uids, backend)
                .await
        }
    }
}

async fn reconcile_local_strict<B: LocalBackend>(
    node: &str,
    replica: &ReplicaConfig,
    status: &MirroredVolumeStatus,
    source_quiesced: bool,
    writer_pod_uids: Vec<String>,
    backend: &B,
) -> Result<NodeSyncStatus, String> {
    // These two fail-closed re-assertions depend only on status.active/
    // grant/is_writer, never a local lock_state/completion read, and run
    // before the operation-scoped block below — that block's reads can
    // fail, and a `?` bailing out must never also skip these.
    if status
        .active
        .as_ref()
        .is_some_and(|active| active.node == node)
        && status.grant.is_none()
        && !backend
            .is_writer(&replica.volume_id)
            .await
            .map_err(|error| error.to_string())?
    {
        let active = status.active.as_ref().expect("checked above");
        backend
            .restore_writer(&replica.volume_id, active.epoch)
            .await
            .map_err(|error| error.to_string())?;
    }

    // A node not named as the committed writer and without a current writer
    // grant must fail closed after restart or stale local configuration.
    if status
        .active
        .as_ref()
        .is_none_or(|active| active.node != node)
        && !status
            .grant
            .as_ref()
            .is_some_and(|grant| grant.target == node)
        && backend
            .is_writer(&replica.volume_id)
            .await
            .map_err(|error| error.to_string())?
    {
        backend
            .enforce_standby(&replica.volume_id)
            .await
            .map_err(|error| error.to_string())?;
    }

    // Fetched at most once per reconcile, so a value read inside the
    // `operation` branch stays valid for the final status.
    let mut completion: Option<B::Completion> = None;

    let operation = status.operation.as_ref();
    let current_operation_id = operation.map(|operation| operation.id);
    // Progress evidence, not safety authority — once proven true for the
    // in-flight operation these stay true for its lifetime rather than
    // reverting to None once the controller advances past the phase that
    // computed them. Only carries forward if it still names the operation
    // in flight; a new operation id drops it.
    let previous = status.nodes.get(node);
    let carry_forward = |value: Option<u64>| value.filter(|id| Some(*id) == current_operation_id);
    let mut released_operation = previous.and_then(|entry| carry_forward(entry.released_operation));
    let mut release_observed_operation =
        previous.and_then(|entry| carry_forward(entry.release_observed_operation));
    let mut quiesced_operation = previous.and_then(|entry| carry_forward(entry.quiesced_operation));

    if let Some(operation) = operation {
        let writer_operation = writer_operation(operation, node);
        if operation.source.as_deref() == Some(node)
            && operation.phase == phase::AWAITING_RELEASE
            && source_quiesced
        {
            quiesced_operation = Some(operation.id);
            if backend
                .is_writer(&replica.volume_id)
                .await
                .map_err(|error| error.to_string())?
            {
                backend
                    .release_writer(&replica.volume_id, &writer_operation)
                    .await
                    .map_err(|error| error.to_string())?;
            }
            released_operation = Some(operation.id);
        }

        if operation.source.as_deref() == Some(node) && operation.phase == phase::ENFORCING_STANDBY
        {
            backend
                .enforce_standby(&replica.volume_id)
                .await
                .map_err(|error| error.to_string())?;
        }

        let completion = completion.insert(
            backend
                .completion(&replica.volume_id)
                .await
                .map_err(|error| error.to_string())?,
        );
        let lock_state = backend
            .lock_state(&replica.volume_id)
            .await
            .map_err(|error| error.to_string())?;

        if operation.target == node
            && operation.phase == phase::AWAITING_RELEASE
            && status
                .nodes
                .get(operation.source.as_deref().unwrap_or_default())
                .is_some_and(|source| source.released_operation == Some(operation.id))
            && completion.ready()
            && matches!(lock_state, LockState::Absent)
        {
            release_observed_operation = Some(operation.id);
        }

        if status.grant.as_ref().is_some_and(|grant| {
            grant.operation_id == operation.id
                && grant.target == node
                && grant.epoch == operation.epoch
        }) && !backend
            .is_writer(&replica.volume_id)
            .await
            .map_err(|error| error.to_string())?
        {
            // Live and local, not the target's previous CRD report. Only
            // reachable before this node has actually acquired the writer
            // role. Once it has, its lock reads back Present on every
            // later reconcile, so this must not re-run then.
            if !completion.ready() || !matches!(lock_state, LockState::Absent) {
                return Err(
                    "writer grant rejected: local replica is not ready or lock is present".into(),
                );
            }
            backend
                .acquire_writer(&replica.volume_id, &writer_operation)
                .await
                .map_err(|error| error.to_string())?;
        }
    }

    let completion = match completion {
        Some(completion) => completion,
        None => backend
            .completion(&replica.volume_id)
            .await
            .map_err(|error| error.to_string())?,
    };
    let lock_state = backend
        .lock_state(&replica.volume_id)
        .await
        .map_err(|error| error.to_string())?;
    let is_writer = backend
        .is_writer(&replica.volume_id)
        .await
        .map_err(|error| error.to_string())?;

    let (lock_absent, lock_epoch) = match lock_state {
        LockState::Absent => (true, None),
        LockState::Present { epoch } => (false, epoch),
    };
    Ok(NodeSyncStatus {
        role: if is_writer {
            role::WRITER
        } else {
            role::STANDBY
        }
        .to_owned(),
        epoch: is_writer
            .then(|| {
                status.operation.as_ref().map_or_else(
                    || status.active.as_ref().map(|active| active.epoch),
                    |operation| Some(operation.epoch),
                )
            })
            .flatten(),
        ready: completion.ready(),
        lock_absent,
        lock_epoch,
        quiesced_operation,
        released_operation,
        release_observed_operation,
        writer_pod_uids,
        conflict_files: Vec::new(),
        detail: serde_json::Value::Null,
    })
}

/// `bestEffort`'s reconcile: no lock/epoch/drain handshake — every
/// folder defaults to `Send & Receive` unless `pull_only` says otherwise,
/// and conflicts are surfaced, never gated on.
async fn reconcile_local_best_effort<B: LocalBackend>(
    node: &str,
    replica: &ReplicaConfig,
    status: &MirroredVolumeStatus,
    pull_only: bool,
    writer_pod_uids: Vec<String>,
    backend: &B,
) -> Result<NodeSyncStatus, String> {
    let is_active = status
        .active
        .as_ref()
        .is_some_and(|active| active.node == node);
    if !pull_only || is_active {
        // With pull_only off (the default), every node — not just the
        // active one — keeps re-asserting Send & Receive on every
        // reconcile. That's what restores it within one tick after the
        // force-standby initContainer's blanket rewrite on every Pod
        // restart, without needing that initContainer to know about
        // per-volume consistency at all.
        backend
            .enable_send_receive(&replica.volume_id)
            .await
            .map_err(|error| error.to_string())?;
    } else if status
        .active
        .as_ref()
        .and_then(|active| status.nodes.get(&active.node))
        .is_some_and(|active_node| active_node.ready)
    {
        // pull_only hygiene: downgrade a non-active node only once the
        // active replica is already known-healthy — never a gate, just
        // tidiness, and left alone (whatever it already is) until then.
        backend
            .enforce_standby(&replica.volume_id)
            .await
            .map_err(|error| error.to_string())?;
    }

    let completion = backend
        .completion(&replica.volume_id)
        .await
        .map_err(|error| error.to_string())?;
    let is_writer = backend
        .is_writer(&replica.volume_id)
        .await
        .map_err(|error| error.to_string())?;
    let conflict_files = backend
        .conflict_files(&replica.volume_id)
        .await
        .map_err(|error| error.to_string())?;

    Ok(NodeSyncStatus {
        role: if is_writer {
            role::WRITER
        } else {
            role::STANDBY
        }
        .to_owned(),
        epoch: None,
        ready: completion.ready(),
        lock_absent: true,
        lock_epoch: None,
        quiesced_operation: None,
        released_operation: None,
        release_observed_operation: None,
        writer_pod_uids,
        conflict_files,
        detail: serde_json::Value::Null,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mirrorvol_api::{phase, ActiveWriter, NodeSyncStatus, PromotionGrant};
    use mirrorvol_backend::{BackendError, Call, FakeBackend, FakeCompletion};
    use std::collections::BTreeMap;

    /// Wraps `FakeBackend` but always fails `lock_state` — for proving that a
    /// failure there doesn't skip the fail-closed re-assertions that don't
    /// depend on it.
    #[derive(Default)]
    struct FailingLockBackend {
        inner: FakeBackend,
    }

    #[async_trait::async_trait]
    impl LocalBackend for FailingLockBackend {
        type Completion = FakeCompletion;

        async fn ensure_replica(&self, config: &ReplicaConfig) -> Result<(), BackendError> {
            self.inner.ensure_replica(config).await
        }
        async fn is_writer(&self, volume_id: &str) -> Result<bool, BackendError> {
            self.inner.is_writer(volume_id).await
        }
        async fn completion(&self, volume_id: &str) -> Result<Self::Completion, BackendError> {
            self.inner.completion(volume_id).await
        }
        async fn lock_state(&self, _volume_id: &str) -> Result<LockState, BackendError> {
            Err(BackendError::Request("lock file unreadable".into()))
        }
        async fn acquire_writer(
            &self,
            volume_id: &str,
            operation: &WriterOperation,
        ) -> Result<(), BackendError> {
            self.inner.acquire_writer(volume_id, operation).await
        }
        async fn restore_writer(&self, volume_id: &str, epoch: u64) -> Result<(), BackendError> {
            self.inner.restore_writer(volume_id, epoch).await
        }
        async fn release_writer(
            &self,
            volume_id: &str,
            operation: &WriterOperation,
        ) -> Result<(), BackendError> {
            self.inner.release_writer(volume_id, operation).await
        }
        async fn enforce_standby(&self, volume_id: &str) -> Result<(), BackendError> {
            self.inner.enforce_standby(volume_id).await
        }
        async fn enable_send_receive(&self, volume_id: &str) -> Result<(), BackendError> {
            self.inner.enable_send_receive(volume_id).await
        }
        async fn conflict_files(&self, volume_id: &str) -> Result<Vec<String>, BackendError> {
            self.inner.conflict_files(volume_id).await
        }
        async fn set_ignore_patterns(
            &self,
            volume_id: &str,
            patterns: &[String],
        ) -> Result<(), BackendError> {
            self.inner.set_ignore_patterns(volume_id, patterns).await
        }
    }

    fn replica() -> ReplicaConfig {
        ReplicaConfig {
            volume_id: "volume".to_owned(),
            local_path: "/data/volume".to_owned(),
            peer_device_ids: vec![],
            peer_addresses: Default::default(),
            generation: 1,
            ignore_patterns: vec![],
        }
    }

    fn operation(phase_name: &str) -> PromotionOperation {
        PromotionOperation {
            id: 2,
            source: Some("node-a".to_owned()),
            target: "node-b".to_owned(),
            epoch: 2,
            phase: phase_name.to_owned(),
            started_at: "0".to_owned(),
            deadline: "9999999999".to_owned(),
        }
    }

    #[tokio::test]
    async fn writer_grant_rechecks_local_lock_and_completion() {
        let backend = FakeBackend::default();
        backend.set_completion("volume", true);
        backend.set_lock_state("volume", LockState::Present { epoch: Some(1) });
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            operation: Some(operation(phase::GRANTING)),
            grant: Some(PromotionGrant {
                operation_id: 2,
                target: "node-b".to_owned(),
                epoch: 2,
            }),
            ..Default::default()
        };
        assert!(reconcile_local(
            "node-b",
            &replica(),
            &status,
            Consistency::Strict,
            false, // pull_only: unused for Strict
            false,
            vec![],
            &backend
        )
        .await
        .is_err());
        assert!(!backend
            .calls()
            .iter()
            .any(|call| matches!(call, Call::AcquireWriter { .. })));
    }

    #[tokio::test]
    async fn a_granted_writer_is_not_re_rejected_after_it_already_acquired() {
        // A later reconcile of the same still-outstanding grant must not
        // reject the node's own already-successful lock as if it were
        // unsafe.
        let backend = FakeBackend::default();
        backend.set_completion("volume", true);
        backend.set_lock_state("volume", LockState::Absent);
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            operation: Some(operation(phase::GRANTING)),
            grant: Some(PromotionGrant {
                operation_id: 2,
                target: "node-b".to_owned(),
                epoch: 2,
            }),
            ..Default::default()
        };
        reconcile_local(
            "node-b",
            &replica(),
            &status,
            Consistency::Strict,
            false, // pull_only: unused for Strict
            false,
            vec![],
            &backend,
        )
        .await
        .expect("first reconcile of a fresh grant acquires the writer role");
        assert!(backend
            .calls()
            .iter()
            .any(|call| matches!(call, Call::AcquireWriter { .. })));

        // What acquire_writer's own effect looks like on the next tick's
        // fresh local reads — same as a real SyncthingBackend would report
        // after successfully creating its own lock file.
        backend.set_lock_state("volume", LockState::Present { epoch: Some(2) });
        backend.set_writer("volume", true);

        let second = reconcile_local(
            "node-b",
            &replica(),
            &status,
            Consistency::Strict,
            false, // pull_only: unused for Strict
            false,
            vec![],
            &backend,
        )
        .await
        .expect("a later reconcile of the same already-fulfilled grant must not re-reject it");
        assert_eq!(second.role, role::WRITER);
    }

    #[tokio::test]
    async fn source_releases_only_after_quiescence() {
        let backend = FakeBackend::default();
        backend.set_completion("volume", true);
        backend.set_writer("volume", true);
        backend.set_lock_state("volume", LockState::Present { epoch: Some(1) });
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            operation: Some(operation(phase::AWAITING_RELEASE)),
            ..Default::default()
        };
        let waiting = reconcile_local(
            "node-a",
            &replica(),
            &status,
            Consistency::Strict,
            false, // pull_only: unused for Strict
            false,
            vec![],
            &backend,
        )
        .await
        .expect("reconcile");
        assert!(waiting.released_operation.is_none());
        let released = reconcile_local(
            "node-a",
            &replica(),
            &status,
            Consistency::Strict,
            false, // pull_only: unused for Strict
            true,
            vec![],
            &backend,
        )
        .await
        .expect("reconcile");
        assert_eq!(released.released_operation, Some(2));
    }

    #[tokio::test]
    async fn fail_closed_reassertion_survives_a_lock_read_failure() {
        let backend = FailingLockBackend::default();
        backend.inner.set_writer("volume", true);
        // Neither source nor target of the in-flight operation, but any
        // operation being in flight is enough to hit the fallible
        // `lock_state` read further down `reconcile_local`.
        let status = MirroredVolumeStatus {
            operation: Some(operation(phase::AWAITING_RELEASE)),
            ..Default::default()
        };
        let result = reconcile_local(
            "node-c",
            &replica(),
            &status,
            Consistency::Strict,
            false, // pull_only: unused for Strict
            false,
            vec![],
            &backend,
        )
        .await;
        assert!(result.is_err());
        assert!(backend
            .inner
            .calls()
            .iter()
            .any(|call| matches!(call, Call::EnforceStandby { .. })));
    }

    #[tokio::test]
    async fn release_observed_progress_survives_a_phase_advance() {
        let backend = FakeBackend::default();
        let mut nodes = BTreeMap::new();
        nodes.insert(
            "node-b".to_owned(),
            NodeSyncStatus {
                release_observed_operation: Some(2),
                ..Default::default()
            },
        );
        let status = MirroredVolumeStatus {
            operation: Some(operation(phase::ENFORCING_STANDBY)),
            nodes,
            ..Default::default()
        };
        let result = reconcile_local(
            "node-b",
            &replica(),
            &status,
            Consistency::Strict,
            false, // pull_only: unused for Strict
            false,
            vec![],
            &backend,
        )
        .await
        .expect("reconcile");
        assert_eq!(result.release_observed_operation, Some(2));
    }

    #[tokio::test]
    async fn stale_progress_does_not_carry_into_a_new_operation() {
        let backend = FakeBackend::default();
        let mut nodes = BTreeMap::new();
        nodes.insert(
            "node-b".to_owned(),
            NodeSyncStatus {
                release_observed_operation: Some(2),
                ..Default::default()
            },
        );
        let mut new_operation = operation(phase::AWAITING_RELEASE);
        new_operation.id = 3;
        let status = MirroredVolumeStatus {
            operation: Some(new_operation),
            nodes,
            ..Default::default()
        };
        let result = reconcile_local(
            "node-b",
            &replica(),
            &status,
            Consistency::Strict,
            false, // pull_only: unused for Strict
            false,
            vec![],
            &backend,
        )
        .await
        .expect("reconcile");
        assert_eq!(result.release_observed_operation, None);
    }

    #[test]
    fn identity_generation_starts_at_one_on_first_registration() {
        assert_eq!(next_identity_generation(None, "DEVICE-A"), 1);
    }

    #[test]
    fn identity_generation_does_not_bump_on_an_unchanged_device_id() {
        assert_eq!(
            next_identity_generation(Some(("DEVICE-A", 3)), "DEVICE-A"),
            3
        );
    }

    #[test]
    fn identity_generation_bumps_on_a_genuine_device_id_change() {
        assert_eq!(
            next_identity_generation(Some(("DEVICE-A", 3)), "DEVICE-B"),
            4
        );
    }

    #[tokio::test]
    async fn best_effort_enables_send_receive_on_every_node_when_pull_only_is_off() {
        let backend = FakeBackend::default();
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 0,
            }),
            ..Default::default()
        };
        // node-b is not the active node, yet pull_only is off — every
        // candidate stays Send & Receive regardless of which one is active.
        reconcile_local(
            "node-b",
            &replica(),
            &status,
            Consistency::BestEffort,
            false,
            false,
            vec![],
            &backend,
        )
        .await
        .expect("reconcile");
        assert!(backend
            .calls()
            .iter()
            .any(|call| matches!(call, Call::EnableSendReceive { .. })));
    }

    #[tokio::test]
    async fn best_effort_pull_only_downgrades_a_non_active_node_once_active_is_ready() {
        let backend = FakeBackend::default();
        let mut nodes = BTreeMap::new();
        nodes.insert(
            "node-a".to_owned(),
            NodeSyncStatus {
                ready: true,
                ..Default::default()
            },
        );
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 0,
            }),
            nodes,
            ..Default::default()
        };
        reconcile_local(
            "node-b",
            &replica(),
            &status,
            Consistency::BestEffort,
            true,
            false,
            vec![],
            &backend,
        )
        .await
        .expect("reconcile");
        assert!(backend
            .calls()
            .iter()
            .any(|call| matches!(call, Call::EnforceStandby { .. })));
        assert!(!backend
            .calls()
            .iter()
            .any(|call| matches!(call, Call::EnableSendReceive { .. })));
    }

    #[tokio::test]
    async fn best_effort_pull_only_leaves_a_non_active_node_alone_until_active_is_ready() {
        let backend = FakeBackend::default();
        // No status.nodes entry for node-a at all yet — its readiness is
        // unknown, not confirmed false.
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 0,
            }),
            ..Default::default()
        };
        reconcile_local(
            "node-b",
            &replica(),
            &status,
            Consistency::BestEffort,
            true,
            false,
            vec![],
            &backend,
        )
        .await
        .expect("reconcile");
        // ensure_replica still runs (that's shared, mode-independent) —
        // only the folder-role calls are what must stay untouched.
        assert!(!backend.calls().iter().any(|call| matches!(
            call,
            Call::EnableSendReceive { .. } | Call::EnforceStandby { .. }
        )));
    }

    #[tokio::test]
    async fn ignore_patterns_are_pushed_to_the_backend_on_every_reconcile() {
        let backend = FakeBackend::default();
        let mut replica = replica();
        replica.ignore_patterns = vec!["*.tmp".to_owned(), "cache/".to_owned()];
        let status = MirroredVolumeStatus::default();
        reconcile_local(
            "node-a",
            &replica,
            &status,
            Consistency::BestEffort,
            false,
            false,
            vec![],
            &backend,
        )
        .await
        .expect("reconcile");
        assert!(backend.calls().iter().any(|call| matches!(
            call,
            Call::SetIgnorePatterns { volume_id, patterns }
                if volume_id == "volume" && patterns == &replica.ignore_patterns
        )));
    }

    #[tokio::test]
    async fn best_effort_surfaces_backend_conflict_files_in_node_status() {
        let backend = FakeBackend::default();
        backend.set_conflict_files("volume", vec!["a.sync-conflict-1".to_owned()]);
        let status = MirroredVolumeStatus::default();
        let result = reconcile_local(
            "node-a",
            &replica(),
            &status,
            Consistency::BestEffort,
            false,
            false,
            vec![],
            &backend,
        )
        .await
        .expect("reconcile");
        assert_eq!(result.conflict_files, vec!["a.sync-conflict-1".to_owned()]);
    }
}
