//! Per-node reconciliation for one local backend instance —
//! [`reconcile_local`], the only thing allowed to call a [`LocalBackend`],
//! and only ever against its own node.

use std::time::Duration;

use mirrorvol_api::{
    phase, role, Consistency, MirroredVolume, MirroredVolumeStatus, NodeSyncStatus,
    PromotionOperation,
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

/// Everything [`reconcile_node`] reads from the cluster on `main.rs`'s
/// behalf, kept out of [`reconcile_local`] so that stays a pure function
/// over trait objects with no `kube::Client` in its signature. One method
/// per distinct read, same shape as `mirrorvol-controller`'s
/// `ClusterReader` — the sequencing between these three, not any one read
/// alone, is what [`reconcile_node`] exists to cover.
#[async_trait::async_trait]
pub trait NodeReader: Send + Sync {
    /// This node's own `ReplicaConfig` for `mv`: peer device set/addresses
    /// from every candidate's `BackendNode`, this node's real replica path,
    /// and (`rsync`-only) the current active peer to pull from.
    async fn replica_config(&self, mv: &MirroredVolume) -> Result<ReplicaConfig, String>;

    /// UIDs of every Pod on this node currently running `mv`'s workload.
    async fn local_writer_pod_uids(&self, mv: &MirroredVolume) -> Result<Vec<String>, String>;

    /// Whether this node is `status.operation`'s source, has fully drained
    /// (its previously observed writer Pods are gone from `current_pods`),
    /// and every one of those Pods' host cgroup/kubelet record also proves
    /// gone. Infallible by design — fails closed (`false`, lock stays
    /// present) rather than surfacing a request error the way the other two
    /// reads do, since a `false` here already means "keep the source
    /// locked," the same conservative outcome an error should produce.
    async fn source_quiesced(&self, status: &MirroredVolumeStatus, current_pods: &[String])
        -> bool;
}

/// Selects the writer-agreement value [`reconcile_node`] relays to
/// `mirrorvol-csi` for `node` — pure, so it's testable without a real
/// reconcile. A missing entry (this node has no
/// [`node_writer_agreement`](MirroredVolumeStatus::node_writer_agreement)
/// entry yet — `bestEffort`, or `strict` before this agent's first
/// reconcile of this volume) defaults to `Converged` (no veto): a relay
/// this agent hasn't populated yet must never itself block an otherwise-
/// healthy attach, since the live local check in `mirrorvol-csi` remains
/// the authoritative gate either way.
pub fn writer_agreement_signal_value<'a>(status: &'a MirroredVolumeStatus, node: &str) -> &'a str {
    status
        .node_writer_agreement
        .get(node)
        .map_or(mirrorvol_api::writer_agreement::CONVERGED, |entry| {
            entry.state.as_str()
        })
}

/// Everything `main.rs` needs to turn one [`reconcile_node`] call into
/// signal-file writes and a status patch: the three cross-process signal
/// values (`None` only when this node isn't a candidate at all — see
/// `requeue_after`), the node's own sync status to patch (`None` on a
/// `NodeReader` failure or a [`reconcile_local`] failure — nothing was
/// decided yet), and how long to wait before the next reconcile.
pub struct NodeReconcileOutcome {
    /// This volume's resolved `spec.consistency`, always known once this
    /// node is a candidate — needs no read.
    pub consistency_signal: Option<String>,
    /// `rsync`-only in practice; empty string for every other backend, same
    /// fallback [`mirrorvol_api::ReplicaConfig::active_peer_address`]'s own
    /// caller already used. Only known once [`NodeReader::replica_config`]
    /// succeeds.
    pub active_peer_signal: Option<String>,
    /// Always known once this node is a candidate — needs no read.
    pub writer_agreement_signal: Option<String>,
    pub node_status: Option<NodeSyncStatus>,
    /// `None` means "wait for the next relevant change"
    /// (`Action::await_change()` in `main.rs`) — this node isn't a
    /// candidate for this volume at all. `Some` is the ordinary
    /// requeue-after duration.
    pub requeue_after: Option<Duration>,
    /// The `NodeReader`/[`reconcile_local`] error that caused this
    /// reconcile to stop early, if any — carried through so `main.rs` can
    /// still log it with context, since `reconcile_node` itself stays free
    /// of logging (same convention `mirrorvol-controller`'s `reconcile`
    /// follows).
    pub error: Option<String>,
}

/// The whole per-reconcile sequence `main.rs::reconcile()` used to hold
/// directly: the candidate guard, the three `NodeReader`-gated reads, then
/// [`reconcile_local`] — one function a test can now drive end to end.
/// `backend` is resolved by the caller (a synchronous registry lookup, not
/// a read this trait needs to cover) — `None` reports this node has no
/// backend configured for `mv.spec.backend` at all, the same defensive case
/// `main.rs` always treated as a 15s-retry rather than an error, since
/// admission already requires every candidate to have a matching
/// `BackendNode` before a volume is acted on.
pub async fn reconcile_node<R: NodeReader>(
    node: &str,
    mv: &MirroredVolume,
    status: &MirroredVolumeStatus,
    reader: &R,
    backend: Option<&dyn LocalBackend>,
) -> NodeReconcileOutcome {
    if !mv
        .spec
        .candidate_nodes
        .iter()
        .any(|candidate| candidate == node)
    {
        return NodeReconcileOutcome {
            consistency_signal: None,
            active_peer_signal: None,
            writer_agreement_signal: None,
            node_status: None,
            requeue_after: None,
            error: None,
        };
    }

    // Neither needs a read: consistency comes straight from `mv.spec`,
    // writer-agreement straight from `status` — both already in hand.
    // Emitted whenever this node is a candidate, independent of whether the
    // reads below succeed (a deliberate simplification over the ordering
    // `main.rs` used to have incidentally, where the writer-agreement
    // signal win was gated on `replica_config` succeeding despite not
    // needing its result).
    let consistency_signal = Some(mv.spec.consistency.as_str().to_owned());
    let writer_agreement_signal = Some(writer_agreement_signal_value(status, node).to_owned());

    let config = match reader.replica_config(mv).await {
        Ok(config) => config,
        Err(error) => {
            return NodeReconcileOutcome {
                consistency_signal,
                active_peer_signal: None,
                writer_agreement_signal,
                node_status: None,
                requeue_after: Some(Duration::from_secs(15)),
                error: Some(error),
            };
        }
    };
    let active_peer_signal = Some(config.active_peer_address.clone().unwrap_or_default());

    let writer_pod_uids = match reader.local_writer_pod_uids(mv).await {
        Ok(pods) => pods,
        Err(error) => {
            return NodeReconcileOutcome {
                consistency_signal,
                active_peer_signal,
                writer_agreement_signal,
                node_status: None,
                requeue_after: Some(Duration::from_secs(15)),
                error: Some(error),
            };
        }
    };

    let Some(backend) = backend else {
        return NodeReconcileOutcome {
            consistency_signal,
            active_peer_signal,
            writer_agreement_signal,
            node_status: None,
            requeue_after: Some(Duration::from_secs(15)),
            error: Some(format!(
                "this node's agent has no backend configured for {}",
                mv.spec.backend
            )),
        };
    };

    let quiesced = reader.source_quiesced(status, &writer_pod_uids).await;
    let node_status = match reconcile_local(
        node,
        &config,
        status,
        mv.spec.consistency,
        quiesced,
        writer_pod_uids,
        backend,
    )
    .await
    {
        Ok(node_status) => node_status,
        Err(error) => {
            return NodeReconcileOutcome {
                consistency_signal,
                active_peer_signal,
                writer_agreement_signal,
                node_status: None,
                requeue_after: Some(Duration::from_secs(5)),
                error: Some(error),
            };
        }
    };

    NodeReconcileOutcome {
        consistency_signal,
        active_peer_signal,
        writer_agreement_signal,
        node_status: Some(node_status),
        requeue_after: Some(Duration::from_secs(5)),
        error: None,
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
/// present.
pub async fn reconcile_local<B: LocalBackend + ?Sized>(
    node: &str,
    replica: &ReplicaConfig,
    status: &MirroredVolumeStatus,
    consistency: Consistency,
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
            reconcile_local_best_effort(node, replica, status, writer_pod_uids, backend).await
        }
    }
}

async fn reconcile_local_strict<B: LocalBackend + ?Sized>(
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
    let mut completion: Option<Box<dyn CompletionStatus>> = None;

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
        }) {
            // Live and local, not the target's previous CRD report.
            let is_writer = backend
                .is_writer(&replica.volume_id)
                .await
                .map_err(|error| error.to_string())?;
            let lock_matches_epoch =
                matches!(lock_state, LockState::Present { epoch: Some(e) } if e == operation.epoch);
            // Not yet converged: role and lock disagree with what this
            // grant authorizes. Covers both the ordinary first-acquisition
            // case and the crash-recovery case (role already flipped by a
            // previous attempt, but the process died before the lock was
            // created) — `acquire_writer`'s own internal lock_state check
            // makes it safe to call in either case. Once converged, this
            // whole branch is a no-op on every later reconcile.
            if !(is_writer && lock_matches_epoch) {
                match lock_state {
                    LockState::Absent => {
                        if !completion.ready() {
                            return Err("writer grant rejected: local replica is not ready".into());
                        }
                        backend
                            .acquire_writer(&replica.volume_id, &writer_operation)
                            .await
                            .map_err(|error| error.to_string())?;
                    }
                    LockState::Present {
                        epoch: Some(lock_epoch),
                    } if lock_epoch == operation.epoch => {
                        // The lock already matches this epoch but role
                        // hasn't caught up — restore rather than
                        // re-acquire, since a lock for this epoch already
                        // exists and acquire_writer would reject it.
                        backend
                            .restore_writer(&replica.volume_id, operation.epoch)
                            .await
                            .map_err(|error| error.to_string())?;
                    }
                    LockState::Present { .. } => {
                        // A lock for a *different* epoch is present — a
                        // genuine conflict, not a retry case.
                        return Err(
                            "writer grant rejected: a lock for a different epoch is present".into(),
                        );
                    }
                }
            }
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
/// folder is `Send & Receive`; every other candidate downgrades to
/// `Receive Only` once the active replica is known-healthy — unconditional
/// hygiene, not opt-in, so `status.writerAgreement` actually converges to
/// `Converged` on its own rather than staying `Pending` indefinitely (this
/// is what replaced the old `pullOnly` flag). Conflicts are surfaced,
/// never gated on.
async fn reconcile_local_best_effort<B: LocalBackend + ?Sized>(
    node: &str,
    replica: &ReplicaConfig,
    status: &MirroredVolumeStatus,
    writer_pod_uids: Vec<String>,
    backend: &B,
) -> Result<NodeSyncStatus, String> {
    let is_active = status
        .active
        .as_ref()
        .is_some_and(|active| active.node == node);
    if is_active {
        // Re-asserted on every reconcile — what restores it within one
        // tick after the force-standby initContainer's blanket rewrite on
        // every Pod restart, without that initContainer needing to know
        // about per-volume consistency at all.
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
        // Downgrade a non-active node only once the active replica is
        // already known-healthy — never a gate, just tidiness, and left
        // alone (whatever it already is) until then.
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
    use mirrorvol_api::{
        phase, ActiveWriter, MirroredVolumeSpec, NodeSyncStatus, PromotionGrant, StorageSpec,
        WorkloadRef,
    };
    use mirrorvol_backend::{BackendError, Call, FakeBackend};
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
        async fn ensure_replica(&self, config: &ReplicaConfig) -> Result<(), BackendError> {
            self.inner.ensure_replica(config).await
        }
        async fn is_writer(&self, volume_id: &str) -> Result<bool, BackendError> {
            self.inner.is_writer(volume_id).await
        }
        async fn completion(
            &self,
            volume_id: &str,
        ) -> Result<Box<dyn CompletionStatus>, BackendError> {
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
            active_peer_address: None,
        }
    }

    fn mv(candidate_nodes: &[&str]) -> MirroredVolume {
        MirroredVolume::new(
            "volume",
            MirroredVolumeSpec {
                workload: WorkloadRef {
                    kind: "Deployment".to_owned(),
                    name: "app".to_owned(),
                    volume_name: "data".to_owned(),
                    mount_path: "/data".to_owned(),
                },
                storage: StorageSpec {
                    storage_class_name: "local-path".to_owned(),
                    replica_path_template: "/data/{claim}".to_owned(),
                    claim_template: serde_json::json!({}),
                    ignore_patterns: vec![],
                    warm_sync_interval_seconds: 300,
                    writer_agreement_timeout_seconds: 1800,
                },
                candidate_nodes: candidate_nodes
                    .iter()
                    .map(|node| (*node).to_owned())
                    .collect(),
                desired_active_node: candidate_nodes.first().unwrap_or(&"node-a").to_string(),
                operation_timeout_seconds: 900,
                backend: mirrorvol_api::backend::SYNCTHING.to_owned(),
                consistency: Consistency::Strict,
            },
        )
    }

    /// Configurable `NodeReader` stub — each method returns whatever was
    /// last set via its `fail_*`/`set_*` method, defaulting to values that
    /// let a candidate node proceed all the way to [`reconcile_local`]. A
    /// `calls` log is also kept, for the rare test that cares about
    /// short-circuiting itself rather than only the final
    /// [`NodeReconcileOutcome`].
    struct FakeNodeReader {
        replica_config: std::sync::Mutex<Result<ReplicaConfig, String>>,
        local_writer_pod_uids: std::sync::Mutex<Result<Vec<String>, String>>,
        source_quiesced: std::sync::Mutex<bool>,
        calls: std::sync::Mutex<Vec<&'static str>>,
    }

    impl Default for FakeNodeReader {
        fn default() -> Self {
            Self {
                replica_config: std::sync::Mutex::new(Ok(replica())),
                local_writer_pod_uids: std::sync::Mutex::new(Ok(Vec::new())),
                source_quiesced: std::sync::Mutex::new(false),
                calls: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl FakeNodeReader {
        fn fail_replica_config(&self) {
            *self.replica_config.lock().unwrap() =
                Err("replica configuration unavailable".to_owned());
        }
        fn fail_local_writer_pod_uids(&self) {
            *self.local_writer_pod_uids.lock().unwrap() =
                Err("writer pod observation unavailable".to_owned());
        }
        fn calls(&self) -> Vec<&'static str> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl NodeReader for FakeNodeReader {
        async fn replica_config(&self, _mv: &MirroredVolume) -> Result<ReplicaConfig, String> {
            self.calls.lock().unwrap().push("replica_config");
            self.replica_config.lock().unwrap().clone()
        }

        async fn local_writer_pod_uids(&self, _mv: &MirroredVolume) -> Result<Vec<String>, String> {
            self.calls.lock().unwrap().push("local_writer_pod_uids");
            self.local_writer_pod_uids.lock().unwrap().clone()
        }

        async fn source_quiesced(
            &self,
            _status: &MirroredVolumeStatus,
            _current_pods: &[String],
        ) -> bool {
            self.calls.lock().unwrap().push("source_quiesced");
            *self.source_quiesced.lock().unwrap()
        }
    }

    fn agreement_entry(state: &str) -> mirrorvol_api::WriterAgreementEntry {
        mirrorvol_api::WriterAgreementEntry {
            state: state.to_owned(),
            changed_at: k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(chrono::Utc::now()),
        }
    }

    #[test]
    fn writer_agreement_signal_value_passes_through_a_present_entry() {
        for state in [
            mirrorvol_api::writer_agreement::CONVERGED,
            mirrorvol_api::writer_agreement::PENDING,
            mirrorvol_api::writer_agreement::CONTRADICTION,
        ] {
            let status = MirroredVolumeStatus {
                node_writer_agreement: BTreeMap::from([(
                    "node-a".to_owned(),
                    agreement_entry(state),
                )]),
                ..Default::default()
            };
            assert_eq!(writer_agreement_signal_value(&status, "node-a"), state);
        }
    }

    #[test]
    fn writer_agreement_signal_value_defaults_to_converged_when_the_node_has_no_entry() {
        let status = MirroredVolumeStatus {
            node_writer_agreement: BTreeMap::from([(
                "node-b".to_owned(),
                agreement_entry(mirrorvol_api::writer_agreement::CONTRADICTION),
            )]),
            ..Default::default()
        };
        // node-a has no entry (bestEffort never populates this map at all,
        // or strict before this agent's first reconcile) — must default to
        // Converged (no veto), not inherit another node's entry or a more
        // alarming default.
        assert_eq!(
            writer_agreement_signal_value(&status, "node-a"),
            mirrorvol_api::writer_agreement::CONVERGED
        );
    }

    #[tokio::test]
    async fn reconcile_node_waits_for_change_when_this_node_is_not_a_candidate() {
        let reader = FakeNodeReader::default();
        let backend = FakeBackend::default();
        let outcome = reconcile_node(
            "node-c",
            &mv(&["node-a", "node-b"]),
            &MirroredVolumeStatus::default(),
            &reader,
            Some(&backend),
        )
        .await;
        assert!(outcome.consistency_signal.is_none());
        assert!(outcome.active_peer_signal.is_none());
        assert!(outcome.writer_agreement_signal.is_none());
        assert!(outcome.node_status.is_none());
        assert!(outcome.requeue_after.is_none());
        assert!(reader.calls().is_empty());
    }

    #[tokio::test]
    async fn reconcile_node_still_emits_consistency_and_writer_agreement_signals_when_replica_config_fails(
    ) {
        let reader = FakeNodeReader::default();
        reader.fail_replica_config();
        let backend = FakeBackend::default();
        let outcome = reconcile_node(
            "node-a",
            &mv(&["node-a", "node-b"]),
            &MirroredVolumeStatus::default(),
            &reader,
            Some(&backend),
        )
        .await;
        // Neither value needs `replica_config`'s result — see reconcile_node's
        // own doc comment on why they're no longer incidentally gated on it.
        assert!(outcome.consistency_signal.is_some());
        assert!(outcome.writer_agreement_signal.is_some());
        assert!(outcome.active_peer_signal.is_none());
        assert!(outcome.node_status.is_none());
        assert_eq!(outcome.requeue_after, Some(Duration::from_secs(15)));
        assert_eq!(
            outcome.error.as_deref(),
            Some("replica configuration unavailable")
        );
        assert_eq!(reader.calls(), vec!["replica_config"]);
    }

    #[tokio::test]
    async fn reconcile_node_keeps_the_active_peer_signal_when_local_writer_pod_uids_fails() {
        let reader = FakeNodeReader::default();
        reader.fail_local_writer_pod_uids();
        let backend = FakeBackend::default();
        let outcome = reconcile_node(
            "node-a",
            &mv(&["node-a", "node-b"]),
            &MirroredVolumeStatus::default(),
            &reader,
            Some(&backend),
        )
        .await;
        // Already known from the successful replica_config read above this
        // one in the sequence.
        assert!(outcome.active_peer_signal.is_some());
        assert!(outcome.node_status.is_none());
        assert_eq!(outcome.requeue_after, Some(Duration::from_secs(15)));
        assert_eq!(
            reader.calls(),
            vec!["replica_config", "local_writer_pod_uids"]
        );
    }

    #[tokio::test]
    async fn reconcile_node_reports_a_missing_backend_after_both_reads_succeed() {
        let reader = FakeNodeReader::default();
        let outcome = reconcile_node(
            "node-a",
            &mv(&["node-a", "node-b"]),
            &MirroredVolumeStatus::default(),
            &reader,
            None,
        )
        .await;
        assert!(outcome.node_status.is_none());
        assert_eq!(outcome.requeue_after, Some(Duration::from_secs(15)));
        assert!(outcome
            .error
            .as_deref()
            .is_some_and(|error| error.contains("no backend configured")));
        // Both reads still ran — the backend check comes after them,
        // matching the original ordering.
        assert_eq!(
            reader.calls(),
            vec!["replica_config", "local_writer_pod_uids"]
        );
    }

    #[tokio::test]
    async fn reconcile_node_reaches_reconcile_local_and_requeues_at_5s() {
        let reader = FakeNodeReader::default();
        let backend = FakeBackend::default();
        let outcome = reconcile_node(
            "node-a",
            &mv(&["node-a", "node-b"]),
            &MirroredVolumeStatus::default(),
            &reader,
            Some(&backend),
        )
        .await;
        assert!(outcome.node_status.is_some());
        assert_eq!(outcome.requeue_after, Some(Duration::from_secs(5)));
        assert!(outcome.error.is_none());
        assert_eq!(
            reader.calls(),
            vec!["replica_config", "local_writer_pod_uids", "source_quiesced"]
        );
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
    async fn a_grant_retries_lock_creation_after_a_crash_between_role_flip_and_lock() {
        // The crash window this design closes: a previous attempt already
        // flipped the backend role to writer (is_writer() == true) but the
        // process died before creating the lock file (lock_state() ==
        // Absent). The old gate (`!is_writer()`) would treat this as
        // already done and never retry; the fix must still call
        // acquire_writer, since acquire_writer's own internal check makes
        // that safe regardless of the role's current state.
        let backend = FakeBackend::default();
        backend.set_completion("volume", true);
        backend.set_lock_state("volume", LockState::Absent);
        backend.set_writer("volume", true);
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
            false,
            vec![],
            &backend,
        )
        .await
        .expect("reconcile retries lock creation");
        assert!(backend
            .calls()
            .iter()
            .any(|call| matches!(call, Call::AcquireWriter { .. })));
    }

    #[tokio::test]
    async fn a_grant_rejects_a_lock_present_for_a_foreign_epoch() {
        let backend = FakeBackend::default();
        backend.set_completion("volume", true);
        // A lock exists, but for a different epoch than this grant — a
        // genuine conflict, not a retry case.
        backend.set_lock_state("volume", LockState::Present { epoch: Some(99) });
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
        let result = reconcile_local(
            "node-b",
            &replica(),
            &status,
            Consistency::Strict,
            false,
            vec![],
            &backend,
        )
        .await;
        assert!(result.is_err());
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
    async fn best_effort_active_node_always_enables_send_receive() {
        let backend = FakeBackend::default();
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 0,
            }),
            ..Default::default()
        };
        reconcile_local(
            "node-a",
            &replica(),
            &status,
            Consistency::BestEffort,
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
    async fn best_effort_downgrades_a_non_active_node_once_active_is_ready() {
        // Unconditional now — no pullOnly opt-in needed for this to happen.
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
    async fn best_effort_leaves_a_non_active_node_alone_until_active_is_ready() {
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
            vec![],
            &backend,
        )
        .await
        .expect("reconcile");
        assert_eq!(result.conflict_files, vec!["a.sync-conflict-1".to_owned()]);
    }
}
