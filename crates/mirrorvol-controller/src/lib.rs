//! Controller-side policy for immutable promotion operations.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
use mirrorvol_api::{
    phase, role, writer_agreement, ActiveWriter, Consistency, MirroredVolumeSpec,
    MirroredVolumeStatus, PromotionGrant, PromotionOperation,
};

#[async_trait::async_trait]
pub trait AppScaler: Send + Sync {
    async fn scale(&self, deployment: &str, replicas: u32) -> Result<(), String>;
    async fn is_scaled_to(&self, deployment: &str, replicas: u32) -> Result<bool, String>;
    async fn set_node_selector(&self, deployment: &str, node: &str) -> Result<(), String>;
    async fn set_volume_claim(
        &self,
        deployment: &str,
        volume_name: &str,
        claim_name: &str,
    ) -> Result<(), String>;
}

/// Everything [`reconcile`] reads from the cluster on `main.rs`'s behalf,
/// kept out of [`decide`] so that stays a pure function. One method per
/// distinct read, same shape as [`AppScaler`] — the point isn't hiding
/// these calls behind one bundled "gather" method, it's letting a test see
/// (and short-circuit on) each one individually, since the *sequencing*
/// between them — not any one read itself — is what [`reconcile`] exists to
/// cover.
#[async_trait::async_trait]
pub trait ClusterReader: Send + Sync {
    /// Every candidate node in `spec` with a `BackendNode` advertising
    /// `spec.backend`, plus each node's `status.identityGeneration`.
    async fn backend_status_for_candidates(
        &self,
        spec: &MirroredVolumeSpec,
    ) -> Result<(BTreeSet<String>, BTreeMap<String, u64>), String>;

    /// True if some other `MirroredVolume` in this namespace already names
    /// the same `(workload.name, workload.volumeName)` pair as `spec`.
    async fn owned_by_another_volume(
        &self,
        self_name: &str,
        spec: &MirroredVolumeSpec,
    ) -> Result<bool, String>;

    /// The target Deployment's *configured* (`spec.replicas`) replica
    /// count — only read before this volume has ever adopted a writer (see
    /// [`AdmissionContext::pre_adoption_replicas`]).
    async fn deployment_desired_replicas(&self, deployment: &str) -> Result<u32, String>;

    /// Applies one replica PVC per candidate node from
    /// `spec.storage.claimTemplate`. A write, but grouped here rather than
    /// left direct in `main.rs`: its failure gates continuation exactly
    /// like the reads above do, and that's the boundary this trait actually
    /// draws — "does this call's outcome affect the sequencing," not
    /// "is it technically a read."
    async fn ensure_replica_claims(
        &self,
        volume_id: &str,
        spec: &MirroredVolumeSpec,
    ) -> Result<(), String>;
}

/// Everything `main.rs` needs to turn one [`reconcile`] call into
/// Kubernetes patches: a status to apply (`None` on a `ClusterReader`
/// failure — nothing was decided yet, so there's nothing to patch), which
/// annotation keys (if any) to clear, and how long to wait before the next
/// reconcile. `requeue_after` is computed here rather than in `main.rs` so
/// the three tiers this picks between (1s operator-override, 15s read
/// failure, deadline-aware normal path) live next to the logic that
/// chooses between them instead of being split across two files.
pub struct ReconcileOutcome {
    pub status: Option<MirroredVolumeStatus>,
    pub clear_annotations: Vec<String>,
    pub requeue_after: Duration,
    /// The `ClusterReader` error that caused this reconcile to stop early,
    /// if any — carried through so `main.rs` can still log it with context,
    /// since `reconcile` itself stays free of logging (same convention
    /// [`decide`] already follows).
    pub error: Option<String>,
}

fn read_failure(error: String) -> ReconcileOutcome {
    ReconcileOutcome {
        status: None,
        clear_annotations: Vec::new(),
        requeue_after: Duration::from_secs(15),
        error: Some(error),
    }
}

/// Detects a matching
/// [`RECOVER_DEGRADED_OPERATION_ANNOTATION`](mirrorvol_api::naming::RECOVER_DEGRADED_OPERATION_ANNOTATION)
/// override on a `Degraded` volume. Returns the `Recovered` condition's
/// message when it applies; `None` otherwise. Pure — [`reconcile`] performs
/// the actual status/annotation changes.
pub fn degraded_recovery_message(
    annotations: &BTreeMap<String, String>,
    status: &MirroredVolumeStatus,
) -> Option<String> {
    let operation = status.operation.as_ref()?;
    if operation.phase != phase::DEGRADED {
        return None;
    }
    let requested =
        annotations.get(mirrorvol_api::naming::RECOVER_DEGRADED_OPERATION_ANNOTATION)?;
    if requested.parse::<u64>() != Ok(operation.id) {
        return None;
    }
    let reason = annotations
        .get(mirrorvol_api::naming::RECOVERY_REASON_ANNOTATION)
        .map_or("no reason given", String::as_str);
    Some(format!(
        "operator override cleared degraded operation {} (target {}) after externally confirming the prior writer can never run again: {reason}",
        operation.id, operation.target
    ))
}

/// The whole per-reconcile sequence `main.rs::reconcile()` used to hold
/// directly: both operator-override checks, the four `ClusterReader`-gated
/// steps, then [`decide`] — one function a test can now drive end to end,
/// instead of only its pure fragments (`degraded_recovery_message`,
/// `identity_reenrollment`, `decide` itself) in isolation. `main.rs`
/// becomes thin wiring: build the real `ClusterReader`/`AppScaler`, call
/// this, turn the [`ReconcileOutcome`] into patches.
pub async fn reconcile<R: ClusterReader, S: AppScaler>(
    name: &str,
    generation: Option<i64>,
    annotations: &BTreeMap<String, String>,
    spec: &MirroredVolumeSpec,
    status: &MirroredVolumeStatus,
    reader: &R,
    scaler: &S,
) -> ReconcileOutcome {
    if let Some(message) = degraded_recovery_message(annotations, status) {
        let mut conditions: Vec<Condition> = status
            .conditions
            .iter()
            .filter(|condition| condition.type_ != "Recovered")
            .cloned()
            .collect();
        conditions.push(Condition {
            type_: "Recovered".to_owned(),
            status: "True".to_owned(),
            reason: "OperatorOverride".to_owned(),
            message,
            observed_generation: generation,
            last_transition_time: Time(chrono::Utc::now()),
        });
        return ReconcileOutcome {
            status: Some(MirroredVolumeStatus {
                active: None,
                operation: None,
                grant: None,
                conditions,
                ..status.clone()
            }),
            clear_annotations: vec![
                mirrorvol_api::naming::RECOVER_DEGRADED_OPERATION_ANNOTATION.to_owned(),
                mirrorvol_api::naming::RECOVERY_REASON_ANNOTATION.to_owned(),
            ],
            requeue_after: Duration::from_secs(1),
            error: None,
        };
    }

    if let Err(error) = reader.ensure_replica_claims(name, spec).await {
        return read_failure(error);
    }
    let (backend_available, identity_generations) =
        match reader.backend_status_for_candidates(spec).await {
            Ok(result) => result,
            Err(error) => return read_failure(error),
        };

    if let Some((node, new_generation, message)) = identity_reenrollment(
        annotations,
        &status.node_identity_generations,
        &identity_generations,
    ) {
        let mut conditions: Vec<Condition> = status
            .conditions
            .iter()
            .filter(|condition| condition.type_ != "IdentityReenrolled")
            .cloned()
            .collect();
        conditions.push(Condition {
            type_: "IdentityReenrolled".to_owned(),
            status: "True".to_owned(),
            reason: "OperatorOverride".to_owned(),
            message,
            observed_generation: generation,
            last_transition_time: Time(chrono::Utc::now()),
        });
        let mut node_identity_generations = status.node_identity_generations.clone();
        node_identity_generations.insert(node, new_generation);
        return ReconcileOutcome {
            status: Some(MirroredVolumeStatus {
                node_identity_generations,
                conditions,
                ..status.clone()
            }),
            clear_annotations: vec![
                mirrorvol_api::naming::REENROLL_NODE_IDENTITY_ANNOTATION.to_owned(),
                mirrorvol_api::naming::RECOVERY_REASON_ANNOTATION.to_owned(),
            ],
            requeue_after: Duration::from_secs(1),
            error: None,
        };
    }

    let owned_by_another = match reader.owned_by_another_volume(name, spec).await {
        Ok(result) => result,
        Err(error) => return read_failure(error),
    };
    // Only read (and only enforce) before this volume has ever adopted a
    // writer — see AdmissionContext::pre_adoption_replicas.
    let pre_adoption_replicas = if status.active.is_none() {
        match reader
            .deployment_desired_replicas(&spec.workload.name)
            .await
        {
            Ok(replicas) => Some(replicas),
            Err(error) => return read_failure(error),
        }
    } else {
        None
    };
    let admission = AdmissionContext {
        backend_available: &backend_available,
        owned_by_another,
        pre_adoption_replicas,
    };

    let next = decide(
        name,
        spec,
        status,
        &admission,
        &identity_generations,
        scaler,
    )
    .await;
    let requeue_after = next
        .operation
        .as_ref()
        .map(|operation| Duration::from_secs(seconds_until_deadline(operation).max(1)))
        .unwrap_or_else(|| Duration::from_secs(5));
    ReconcileOutcome {
        status: Some(next),
        clear_annotations: Vec::new(),
        requeue_after,
        error: None,
    }
}

/// Re-exported for `main.rs` — see [`mirrorvol_api::naming::replica_claim_name`].
pub fn replica_claim_name(volume_id: &str, node: &str) -> String {
    mirrorvol_api::naming::replica_claim_name(volume_id, node)
}

/// Points the workload's volume claim and node selector at `target` and
/// scales it back up — the one sequence both consistency modes end a move
/// with, whether that's `strict`'s [`phase::GRANTING`] phase (only after
/// every safety check has passed) or `bestEffort`'s immediate move (no
/// checks at all beyond [`admission_rejection`]). Shared so the two paths
/// can't drift apart. Its `Result` is deliberately still fallible — unlike
/// [`decide`]/`decide_strict`/`decide_best_effort`, this is the one place
/// that actually calls out over the network; callers turn a failure here
/// into a `ReconcileBlocked`/`ScalerError` condition rather than losing it.
async fn promote_workload<S: AppScaler>(
    scaler: &S,
    spec: &MirroredVolumeSpec,
    volume_id: &str,
    target: &str,
) -> Result<(), String> {
    scaler
        .set_volume_claim(
            &spec.workload.name,
            &spec.workload.volume_name,
            &replica_claim_name(volume_id, target),
        )
        .await?;
    scaler
        .set_node_selector(&spec.workload.name, target)
        .await?;
    scaler.scale(&spec.workload.name, 1).await?;
    Ok(())
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn deadline_expired(operation: &PromotionOperation) -> bool {
    operation
        .deadline
        .parse::<u64>()
        .is_ok_and(|deadline| now_seconds() >= deadline)
}

/// How long until `operation`'s deadline, for `main.rs` to requeue at
/// exactly that point instead of ticking on a blind fixed interval in the
/// meantime.
/// `0` (requeue immediately) both when the deadline has already passed and
/// when it's unparseable — the same fail-fast reading `deadline_expired`
/// already gives a malformed deadline.
pub fn seconds_until_deadline(operation: &PromotionOperation) -> u64 {
    operation
        .deadline
        .parse::<u64>()
        .unwrap_or(0)
        .saturating_sub(now_seconds())
}

fn standby_confirmed(status: &MirroredVolumeStatus, node: &str) -> bool {
    status
        .nodes
        .get(node)
        .is_some_and(|state| state.role == role::STANDBY)
}

fn all_other_candidates_standby(
    spec: &MirroredVolumeSpec,
    status: &MirroredVolumeStatus,
    target: &str,
) -> bool {
    spec.candidate_nodes
        .iter()
        .filter(|node| node.as_str() != target)
        .all(|node| standby_confirmed(status, node))
}

/// Candidate nodes whose currently-observed identity generation
/// contradicts the controller's recorded baseline. A node absent from
/// `observed`, or with no baseline yet in `baseline`, is never a
/// contradiction — establishing a baseline isn't contradicting one.
fn identity_contradictions(
    spec: &MirroredVolumeSpec,
    baseline: &BTreeMap<String, u64>,
    observed: &BTreeMap<String, u64>,
) -> Vec<String> {
    spec.candidate_nodes
        .iter()
        .filter(|node| {
            observed.get(node.as_str()).is_some_and(|current| {
                baseline
                    .get(node.as_str())
                    .is_some_and(|acknowledged| acknowledged != current)
            })
        })
        .cloned()
        .collect()
}

fn identity_changed_condition(nodes: &[String]) -> Condition {
    Condition {
        type_: "IdentityChanged".to_owned(),
        status: "True".to_owned(),
        reason: "BackendIdentityContradiction".to_owned(),
        message: format!(
            "backend identity changed unexpectedly on: {} — operator re-enrollment required",
            nodes.join(", ")
        ),
        observed_generation: None,
        last_transition_time: Time(chrono::Utc::now()),
    }
}

/// Detects a matching
/// [`REENROLL_NODE_IDENTITY_ANNOTATION`](mirrorvol_api::naming::REENROLL_NODE_IDENTITY_ANNOTATION)
/// override (see that constant's own doc comment) — the idle-state
/// equivalent of `main.rs`'s `degraded_recovery_message`, since an idle
/// contradiction has no `operation.id` to key an override on the way the
/// in-flight case does. Returns the node and generation to re-baseline
/// plus the `IdentityReenrolled` condition message, or `None` if nothing
/// applies. Pure — the caller performs the actual status patch.
/// `baseline`/`observed` are
/// [`node_identity_generations`](mirrorvol_api::MirroredVolumeStatus::node_identity_generations)/
/// the freshly-read [`BackendNode`](mirrorvol_api::BackendNode)
/// generations, same inputs `identity_contradictions` takes.
pub fn identity_reenrollment(
    annotations: &BTreeMap<String, String>,
    baseline: &BTreeMap<String, u64>,
    observed: &BTreeMap<String, u64>,
) -> Option<(String, u64, String)> {
    let requested = annotations.get(mirrorvol_api::naming::REENROLL_NODE_IDENTITY_ANNOTATION)?;
    let (node, generation) = mirrorvol_api::naming::parse_reenroll_node_identity(requested)?;
    // Must name the node's *current* observed generation exactly — a
    // mismatch here means the annotation is stale (the node moved on to a
    // newer generation since it was written) or premature, not a real ack
    // of what's actually running today.
    if observed.get(node) != Some(&generation) {
        return None;
    }
    // Nothing to clear if this node isn't actually contradicting its
    // recorded baseline
    if baseline.get(node) == Some(&generation) {
        return None;
    }
    let reason = annotations
        .get(mirrorvol_api::naming::RECOVERY_REASON_ANNOTATION)
        .map_or("no reason given", String::as_str);
    let message = format!(
        "operator re-enrolled {node}'s new backend identity (generation {generation}) after externally confirming it's trusted: {reason}"
    );
    Some((node.to_owned(), generation, message))
}

/// Replace-if-different, remove-if-`None`, never blindly push: shared by
/// every condition this module maintains across repeated reconciles of the
/// same still-true state, so `status.conditions` never grows without bound
/// and a condition already matching `fresh` keeps its original
/// `lastTransitionTime`.
fn upsert_condition(conditions: &mut Vec<Condition>, type_: &str, fresh: Option<Condition>) {
    match fresh {
        None => conditions.retain(|condition| condition.type_ != type_),
        Some(fresh) => {
            let unchanged = conditions.iter().any(|condition| {
                condition.type_ == fresh.type_ && condition.message == fresh.message
            });
            if !unchanged {
                conditions.retain(|condition| condition.type_ != type_);
                conditions.push(fresh);
            }
        }
    }
}

fn sync_conflict_condition(nodes: &[(String, Vec<String>)]) -> Condition {
    let detail = nodes
        .iter()
        .map(|(node, files)| format!("{node}: {}", files.join(", ")))
        .collect::<Vec<_>>()
        .join("; ");
    Condition {
        type_: "Degraded".to_owned(),
        status: "True".to_owned(),
        reason: "SyncConflict".to_owned(),
        message: format!("unresolved sync conflicts — {detail}"),
        observed_generation: None,
        last_transition_time: Time(chrono::Utc::now()),
    }
}

/// Whether a candidate's self-reported backend role and its own local
/// writer lock agree. Owns the whole concept: the verdict type, computing
/// it in both consistency modes, and interpreting what it means for a
/// caller ([`confirmed`]) —
/// grouped behind one seam instead of touring five-plus separate functions
/// to hold the invariant in your head. Callers only ever need the three
/// `pub(super)` entry points below; everything else here is a private
/// internal seam with its own tests.
///
/// Deliberately does **not** own turning a verdict into a `Condition`
/// (`writer_lock_mismatch_condition`/`writer_agreement_stale_condition`,
/// both just outside this module) — those follow the same convention every
/// other condition in this file does (`identity_changed_condition`,
/// `sync_conflict_condition`, ...): a small function living next to
/// `decide_strict`/`decide_best_effort`, not folded into whatever computed
/// the thing it describes.
mod agreement {
    use std::collections::BTreeMap;

    use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
    use mirrorvol_api::{
        role, writer_agreement, MirroredVolumeSpec, MirroredVolumeStatus, NodeSyncStatus,
        WriterAgreementEntry,
    };

    use super::phase;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum WriterAgreement {
        /// Role and lock agree with what's expected right now (or, for a
        /// standby, there's nothing to disagree about).
        Converged,
        /// Role and lock disagree, but a live grant currently authorizes
        /// this node to be converging toward writer — expected,
        /// self-healing.
        Pending,
        /// `strict` only: role and lock disagree with nothing authorizing
        /// it. Never expected to resolve on its own.
        Contradiction,
    }

    impl WriterAgreement {
        fn as_str(self) -> &'static str {
            match self {
                WriterAgreement::Converged => writer_agreement::CONVERGED,
                WriterAgreement::Pending => writer_agreement::PENDING,
                WriterAgreement::Contradiction => writer_agreement::CONTRADICTION,
            }
        }
    }

    /// The epoch that would legitimately make `node` a writer right now,
    /// from data already in `status` — `status.active` if it's the
    /// committed writer, or the live grant's epoch if it's the target of
    /// one.
    fn expected_writer_epoch(status: &MirroredVolumeStatus, node: &str) -> Option<u64> {
        if let Some(active) = status.active.as_ref().filter(|active| active.node == node) {
            return Some(active.epoch);
        }
        if let (Some(operation), Some(grant)) = (status.operation.as_ref(), status.grant.as_ref()) {
            if operation.target == node
                && grant.operation_id == operation.id
                && grant.target == node
                && grant.epoch == operation.epoch
            {
                return Some(operation.epoch);
            }
        }
        None
    }

    /// Whether a live operation currently explains `node`'s role/lock
    /// disagreeing with `expected_writer_epoch` as an expected, self-healing
    /// transition rather than a genuine contradiction. Both cases are bounded
    /// to the exact phases `reconcile_local_strict` actually expects the
    /// disagreement in — not "anywhere in the operation" — so a transition
    /// that overstays those phases reads as `Contradiction` immediately,
    /// rather than waiting on the full `operation.deadline` to notice:
    ///
    /// - `node` is the operation's *target* with a matching live grant, during
    ///   `Granting` — the only phase a grant ever exists in (mid-acquire).
    /// - `node` is the operation's *source*, during `AwaitingRelease` **or**
    ///   `EnforcingStandby` — the window where its role can legitimately
    ///   still read `Writer` with its lock already gone. `release_writer`
    ///   runs once, right as the phase enters `AwaitingRelease`; but
    ///   `enforce_standby` (the call that actually flips role to `Standby`)
    ///   only runs once the controller has *already* advanced the phase to
    ///   `EnforcingStandby` — a real async gap against a live cluster: the
    ///   agent's own reconcile cadence means at least one real reconcile tick
    ///   can land with the phase already `EnforcingStandby` but
    ///   `enforce_standby` not yet having taken effect. Excluding that phase
    ///   (an earlier, tighter version of this function did) treated that
    ///   ordinary, expected gap as a genuine contradiction and forced every
    ///   real promotion straight to `Degraded` within seconds — bounded the
    ///   same way `AwaitingRelease` already was: `standby_confirmed` gates
    ///   the phase's own advancement out of `EnforcingStandby`, and
    ///   `operation.deadline` remains the backstop for a source that's
    ///   still stuck there once that's overstayed for real.
    fn in_flight_transition(status: &MirroredVolumeStatus, node: &str) -> bool {
        status.operation.as_ref().is_some_and(|operation| {
            (matches!(
                operation.phase.as_str(),
                phase::AWAITING_RELEASE | phase::ENFORCING_STANDBY
            ) && operation.source.as_deref() == Some(node))
                || (operation.phase == phase::GRANTING
                    && operation.target == node
                    && status.grant.as_ref().is_some_and(|grant| {
                        grant.operation_id == operation.id
                            && grant.target == node
                            && grant.epoch == operation.epoch
                    }))
        })
    }

    /// `strict` only. A standby is always `Converged`, whatever its own
    /// (possibly stale, possibly synced-from-a-past-epoch) copy of the
    /// lock file shows — the dangerous half of the pair is specifically
    /// "claims writer without a matching lock." A node with no legitimate
    /// claim at all (`expected_epoch: None`) reporting itself writer, with
    /// no in-flight operation to explain it, is always `Contradiction`,
    /// never `Pending`.
    fn verdict(
        node_state: &NodeSyncStatus,
        expected_epoch: Option<u64>,
        in_flight_transition: bool,
    ) -> WriterAgreement {
        let is_writer = node_state.role == role::WRITER;
        let lock_matches = expected_epoch.is_some() && node_state.lock_epoch == expected_epoch;
        match (is_writer, lock_matches, in_flight_transition) {
            (false, _, _) | (true, true, _) => WriterAgreement::Converged,
            (true, false, true) => WriterAgreement::Pending,
            (true, false, false) => WriterAgreement::Contradiction,
        }
    }

    /// Replace-if-state-changed, same "don't grow/reset without cause"
    /// discipline as [`super::upsert_condition`] — `changed_at` is only
    /// overwritten when `state` actually differs from what's already
    /// recorded, so `now - changed_at` reflects genuine time-in-state.
    fn upsert(
        existing: Option<&WriterAgreementEntry>,
        state: WriterAgreement,
    ) -> WriterAgreementEntry {
        let changed_at = existing
            .filter(|entry| entry.state == state.as_str())
            .map(|entry| entry.changed_at.clone())
            .unwrap_or_else(|| Time(chrono::Utc::now()));
        WriterAgreementEntry {
            state: state.as_str().to_owned(),
            changed_at,
        }
    }

    /// `strict`: every candidate's writer-agreement verdict, plus which
    /// ones are `Contradiction` (the only state that needs a caller
    /// response — `Pending` is expected and self-healing, `Converged`
    /// needs nothing).
    pub(super) fn strict(
        spec: &MirroredVolumeSpec,
        status: &MirroredVolumeStatus,
    ) -> (BTreeMap<String, WriterAgreementEntry>, Vec<String>) {
        let default_state = NodeSyncStatus::default();
        let mut updated = BTreeMap::new();
        let mut contradictions = Vec::new();
        for node in &spec.candidate_nodes {
            let node_state = status.nodes.get(node).unwrap_or(&default_state);
            let expected_epoch = expected_writer_epoch(status, node);
            let transitioning = in_flight_transition(status, node);
            let state = verdict(node_state, expected_epoch, transitioning);
            if state == WriterAgreement::Contradiction {
                contradictions.push(node.clone());
            }
            updated.insert(
                node.clone(),
                upsert(status.node_writer_agreement.get(node), state),
            );
        }
        (updated, contradictions)
    }

    /// `bestEffort`: a volume-wide count, not per-candidate — no single
    /// agent can see another node's `is_writer()`, so this can only be
    /// computed here, by aggregating what every candidate's own agent
    /// already reports into `status.nodes[*].role`. There's no lock to
    /// disagree with in this mode, so this can never produce
    /// `Contradiction` — the underlying verdict has no branch that returns
    /// it. Folds in the same upsert bookkeeping `strict` above already
    /// does internally, so the caller gets a finished entry in one call
    /// rather than two.
    pub(super) fn best_effort(
        spec: &MirroredVolumeSpec,
        status: &MirroredVolumeStatus,
    ) -> WriterAgreementEntry {
        let writers = spec
            .candidate_nodes
            .iter()
            .filter(|node| {
                status
                    .nodes
                    .get(node.as_str())
                    .is_some_and(|state| state.role == role::WRITER)
            })
            .count();
        let state = if writers <= 1 {
            WriterAgreement::Converged
        } else {
            WriterAgreement::Pending
        };
        upsert(status.writer_agreement.as_ref(), state)
    }

    /// A target is only confirmed once its own writer-agreement verdict
    /// reads `Converged` — i.e. its self-reported role *and* its local
    /// lock both back this epoch, not just the role alone. The epoch
    /// itself doesn't need a separate parameter here: `node_writer_agreement`
    /// was computed against the current live grant, so `Converged` already
    /// means "matches the epoch this grant authorizes." Replaces a
    /// narrower `role`+self-reported-`epoch`-only check that a backend
    /// crash between flipping its role and creating its lock could satisfy
    /// without the lock ever existing.
    pub(super) fn confirmed(status: &MirroredVolumeStatus, node: &str) -> bool {
        status
            .node_writer_agreement
            .get(node)
            .is_some_and(|entry| entry.state == writer_agreement::CONVERGED)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn node_state(role_value: &str, lock_epoch: Option<u64>) -> NodeSyncStatus {
            NodeSyncStatus {
                role: role_value.to_owned(),
                lock_epoch,
                ..Default::default()
            }
        }

        #[test]
        fn a_standby_is_always_converged_regardless_of_its_own_lock_copy() {
            for lock_epoch in [None, Some(1), Some(99)] {
                assert_eq!(
                    verdict(&node_state(role::STANDBY, lock_epoch), Some(1), false),
                    WriterAgreement::Converged
                );
            }
        }

        #[test]
        fn a_writer_with_a_matching_lock_is_converged() {
            assert_eq!(
                verdict(&node_state(role::WRITER, Some(1)), Some(1), false),
                WriterAgreement::Converged
            );
        }

        #[test]
        fn a_writer_with_no_legitimate_claim_and_no_in_flight_transition_is_a_contradiction() {
            assert_eq!(
                verdict(&node_state(role::WRITER, None), None, false),
                WriterAgreement::Contradiction
            );
        }

        #[test]
        fn a_writer_with_a_mismatched_lock_but_an_explaining_in_flight_transition_is_pending() {
            assert_eq!(
                verdict(&node_state(role::WRITER, Some(1)), Some(2), true),
                WriterAgreement::Pending
            );
        }

        #[test]
        fn a_writer_with_a_mismatched_lock_and_no_explaining_transition_is_a_contradiction() {
            assert_eq!(
                verdict(&node_state(role::WRITER, Some(1)), Some(2), false),
                WriterAgreement::Contradiction
            );
        }
    }
}

fn writer_lock_mismatch_condition(nodes: &[String]) -> Condition {
    Condition {
        type_: "WriterLockMismatch".to_owned(),
        status: "True".to_owned(),
        reason: "LocalLockDisagreesWithRole".to_owned(),
        message: format!(
            "backend role and local writer lock disagree on: {} — operator investigation required",
            nodes.join(", ")
        ),
        observed_generation: None,
        last_transition_time: Time(chrono::Utc::now()),
    }
}

fn seconds_since(time: &Time) -> u64 {
    now_seconds().saturating_sub(u64::try_from(time.0.timestamp()).unwrap_or(0))
}

fn writer_agreement_stale_condition(timeout_seconds: u32) -> Condition {
    Condition {
        type_: "WriterAgreementStale".to_owned(),
        status: "True".to_owned(),
        reason: "MultipleWriters".to_owned(),
        message: format!(
            "more than one candidate has been write-capable for over {timeout_seconds}s — expected briefly after a move, not indefinitely; check for an unhealthy or unreachable candidate"
        ),
        observed_generation: None,
        last_transition_time: Time(chrono::Utc::now()),
    }
}

/// A rejection from [`admission_rejection`] (`type: AdmissionBlocked`) or
/// from the parts of `decide_strict`/`decide_best_effort` that need to
/// withhold a step for a reason other than an identity contradiction or a
/// sync conflict.
fn blocked_condition(type_: &str, reason: &str, message: String) -> Condition {
    Condition {
        type_: type_.to_owned(),
        status: "True".to_owned(),
        reason: reason.to_owned(),
        message,
        observed_generation: None,
        last_transition_time: Time(chrono::Utc::now()),
    }
}

/// Everything `admission_rejection` needs beyond `spec` itself — all of
/// it read elsewhere (`main.rs`) so [`decide`] stays a pure function.
/// Grouped into one struct so a future admission rule adds a field here
/// for consistency.
pub struct AdmissionContext<'a> {
    /// Nodes with a [`BackendNode`](mirrorvol_api::BackendNode)
    /// advertising [`spec.backend`](mirrorvol_api::MirroredVolumeSpec::backend).
    pub backend_available: &'a BTreeSet<String>,
    /// True if some other `MirroredVolume` in this namespace already names
    /// the same `(workload.name, workload.volumeName)` pair.
    pub owned_by_another: bool,
    /// The target Deployment's *configured* (`spec.replicas`, not
    /// `status.replicas`) replica count — checked only before this volume
    /// has ever adopted a writer (`status.active` still `None`). `None`
    /// once adopted: from that point on the controller owns the workload's
    /// replica count itself, so re-checking it on every reconcile would
    /// reject the controller's in-flight drain/grant transitions.
    pub pre_adoption_replicas: Option<u32>,
}

/// The one admission check that fails, as a ready-to-upsert
/// `AdmissionBlocked` condition or `None` if the spec and cluster state
/// pass every check. See [`AdmissionContext`] for the cluster-state inputs.
fn admission_rejection(
    spec: &MirroredVolumeSpec,
    admission: &AdmissionContext<'_>,
) -> Option<Condition> {
    if spec.workload.kind != "Deployment" {
        return Some(blocked_condition(
            "AdmissionBlocked",
            "UnsupportedWorkloadKind",
            "only Deployment workloads are supported".to_owned(),
        ));
    }
    if spec.candidate_nodes.len() < 2 {
        return Some(blocked_condition(
            "AdmissionBlocked",
            "InsufficientCandidates",
            "a mirrored volume requires at least two candidate nodes".to_owned(),
        ));
    }
    if !spec
        .candidate_nodes
        .iter()
        .any(|node| node == &spec.desired_active_node)
    {
        return Some(blocked_condition(
            "AdmissionBlocked",
            "DesiredNotCandidate",
            "desiredActiveNode is not a candidate node".to_owned(),
        ));
    }
    let mut unique = spec.candidate_nodes.clone();
    unique.sort();
    unique.dedup();
    if unique.len() != spec.candidate_nodes.len() {
        return Some(blocked_condition(
            "AdmissionBlocked",
            "DuplicateCandidates",
            "candidateNodes must be unique".to_owned(),
        ));
    }
    // Every candidate needs its own agent actually running spec.backend.
    // `backend_available` is a reduction of the real BackendNode list,
    // so this stays a pure function, no I/O.
    if let Some(node) = spec
        .candidate_nodes
        .iter()
        .find(|node| !admission.backend_available.contains(node.as_str()))
    {
        return Some(blocked_condition(
            "AdmissionBlocked",
            "BackendUnavailable",
            format!(
                "candidate node {node} has no BackendNode advertising backend {:?}",
                spec.backend
            ),
        ));
    }
    if admission.owned_by_another {
        return Some(blocked_condition(
            "AdmissionBlocked",
            "OwnedByAnotherVolume",
            format!(
                "another MirroredVolume already manages {}/{}",
                spec.workload.name, spec.workload.volume_name
            ),
        ));
    }
    if let Some(replicas) = admission.pre_adoption_replicas {
        // > 1, not != 1: zero is a legitimate pre-adoption state (e.g. a
        // freshly-authored Deployment nobody has started yet).
        if replicas > 1 {
            return Some(blocked_condition(
                "AdmissionBlocked",
                "PreAdoptionReplicaMismatch",
                format!(
                    "workload {} must not already have more than one replica before it can be adopted (found {replicas})",
                    spec.workload.name
                ),
            ));
        }
    }
    None
}

/// Records each candidate's first-sighted identity generation as its
/// baseline; never overwrites one already recorded, so a later change is
/// what [`identity_contradictions`] detects.
fn record_identity_baseline(
    spec: &MirroredVolumeSpec,
    identity_generations: &BTreeMap<String, u64>,
    next: &mut MirroredVolumeStatus,
) {
    for node in &spec.candidate_nodes {
        if let Some(&current) = identity_generations.get(node) {
            next.node_identity_generations
                .entry(node.clone())
                .or_insert(current);
        }
    }
}

/// Advances only controller-owned status. Node status is read as progress
/// evidence and is intentionally returned unchanged. Dispatches on
/// [`spec.consistency`](mirrorvol_api::MirroredVolumeSpec::consistency).
///
/// Infallible: every rejection this module can produce (admission,
/// an operation-target mismatch, an unrecognized phase, a backend
/// identity contradiction, an [`AppScaler`] I/O failure) is part of
/// the returned [`MirroredVolumeStatus`] as a condition instead of
/// an `Err` the caller could discard.
pub async fn decide<S: AppScaler>(
    volume_id: &str,
    spec: &MirroredVolumeSpec,
    status: &MirroredVolumeStatus,
    admission: &AdmissionContext<'_>,
    identity_generations: &BTreeMap<String, u64>,
    scaler: &S,
) -> MirroredVolumeStatus {
    let mut next = status.clone();

    if let Some(rejection) = admission_rejection(spec, admission) {
        upsert_condition(&mut next.conditions, "AdmissionBlocked", Some(rejection));
        return next;
    }
    upsert_condition(&mut next.conditions, "AdmissionBlocked", None);

    record_identity_baseline(spec, identity_generations, &mut next);

    // Computed once and surfaced unconditionally, auto-clearing the
    // moment nothing contradicts anymore (same as `SyncConflict`), so an
    // operator can see a contradiction via `kubectl get` in every state
    // regardless of consistency mode or whether anything is even trying to
    // move right now.
    let contradictions =
        identity_contradictions(spec, &next.node_identity_generations, identity_generations);
    upsert_condition(
        &mut next.conditions,
        "IdentityChanged",
        (!contradictions.is_empty()).then(|| identity_changed_condition(&contradictions)),
    );

    match spec.consistency {
        Consistency::Strict => decide_strict(volume_id, spec, next, &contradictions, scaler).await,
        Consistency::BestEffort => {
            decide_best_effort(volume_id, spec, next, &contradictions, scaler).await
        }
    }
}

async fn decide_strict<S: AppScaler>(
    volume_id: &str,
    spec: &MirroredVolumeSpec,
    mut next: MirroredVolumeStatus,
    contradictions: &[String],
    scaler: &S,
) -> MirroredVolumeStatus {
    // Cleared unconditionally up front; only the specific branches below
    // that actually withhold a step re-set it. That means a step that
    // stops being blocked (target realigned, a recognized phase, a scaler
    // call that now succeeds) always clears this the very next tick.
    upsert_condition(&mut next.conditions, "ReconcileBlocked", None);

    // Computed once from the incoming status (before anything below
    // mutates active/operation/grant), surfaced unconditionally — same
    // "visible via kubectl get in every state" treatment IdentityChanged
    // already gets.
    let (node_writer_agreement, agreement_contradictions) = agreement::strict(spec, &next);
    next.node_writer_agreement = node_writer_agreement;
    upsert_condition(
        &mut next.conditions,
        "WriterLockMismatch",
        (!agreement_contradictions.is_empty())
            .then(|| writer_lock_mismatch_condition(&agreement_contradictions)),
    );

    if let Some(operation) = &next.operation {
        if operation.target != spec.desired_active_node {
            upsert_condition(
                &mut next.conditions,
                "ReconcileBlocked",
                Some(blocked_condition(
                    "ReconcileBlocked",
                    "OperationTargetMismatch",
                    format!(
                        "operation {} targets {}; reject desired target {} until it resolves",
                        operation.id, operation.target, spec.desired_active_node
                    ),
                )),
            );
            return next;
        }
        if deadline_expired(operation)
            || !contradictions.is_empty()
            || !agreement_contradictions.is_empty()
        {
            let operation = next.operation.as_mut().expect("checked above");
            operation.phase = phase::DEGRADED.to_owned();
            next.grant = None;
            return next;
        }
    } else if next
        .active
        .as_ref()
        .is_some_and(|active| active.node == spec.desired_active_node)
    {
        return next;
    } else {
        if !contradictions.is_empty() || !agreement_contradictions.is_empty() {
            // Not starting a new operation: an idle rejection has no
            // operation to force into Degraded, but the IdentityChanged/
            // WriterLockMismatch conditions set above still need to reach
            // status; see `identity_reenrollment`.
            return next;
        }
        if let Err(error) = scaler.scale(&spec.workload.name, 0).await {
            upsert_condition(
                &mut next.conditions,
                "ReconcileBlocked",
                Some(blocked_condition("ReconcileBlocked", "ScalerError", error)),
            );
            return next;
        }
        let current_epoch = next.active.as_ref().map_or(0, |active| active.epoch);
        let now = now_seconds();
        next.operation = Some(PromotionOperation {
            id: current_epoch + 1,
            source: next.active.as_ref().map(|active| active.node.clone()),
            target: spec.desired_active_node.clone(),
            epoch: current_epoch + 1,
            phase: phase::DRAINING.to_owned(),
            started_at: now.to_string(),
            deadline: (now + spec.operation_timeout_seconds).to_string(),
        });
        return next;
    }

    let operation = next.operation.clone().expect("operation established above");
    match operation.phase.as_str() {
        phase::DRAINING => match scaler.is_scaled_to(&spec.workload.name, 0).await {
            Ok(true) => {
                next.operation.as_mut().expect("operation exists").phase =
                    phase::AWAITING_RELEASE.to_owned();
            }
            Ok(false) => {}
            Err(error) => {
                upsert_condition(
                    &mut next.conditions,
                    "ReconcileBlocked",
                    Some(blocked_condition("ReconcileBlocked", "ScalerError", error)),
                );
            }
        },
        phase::AWAITING_RELEASE => {
            let target_ready = next
                .nodes
                .get(&operation.target)
                .is_some_and(|target| target.ready);
            let ready_for_source_standby = match operation.source.as_deref() {
                Some(source) => {
                    next.nodes
                        .get(source)
                        .is_some_and(|state| state.released_operation == Some(operation.id))
                        && next.nodes.get(&operation.target).is_some_and(|state| {
                            state.release_observed_operation == Some(operation.id) && state.ready
                        })
                }
                None => {
                    target_ready
                        && next
                            .nodes
                            .get(&operation.target)
                            .is_some_and(|state| state.lock_absent)
                }
            };
            if ready_for_source_standby {
                next.operation.as_mut().expect("operation exists").phase =
                    phase::ENFORCING_STANDBY.to_owned();
            }
        }
        phase::ENFORCING_STANDBY => {
            if operation
                .source
                .as_deref()
                .is_none_or(|source| standby_confirmed(&next, source))
                && all_other_candidates_standby(spec, &next, &operation.target)
            {
                next.operation.as_mut().expect("operation exists").phase =
                    phase::GRANTING.to_owned();
                next.grant = Some(PromotionGrant {
                    operation_id: operation.id,
                    target: operation.target.clone(),
                    epoch: operation.epoch,
                });
            }
        }
        phase::GRANTING => {
            if agreement::confirmed(&next, &operation.target)
                && all_other_candidates_standby(spec, &next, &operation.target)
            {
                match promote_workload(scaler, spec, volume_id, &operation.target).await {
                    Ok(()) => {
                        next.active = Some(ActiveWriter {
                            node: operation.target,
                            epoch: operation.epoch,
                        });
                        next.operation = None;
                        next.grant = None;
                    }
                    Err(error) => {
                        upsert_condition(
                            &mut next.conditions,
                            "ReconcileBlocked",
                            Some(blocked_condition("ReconcileBlocked", "ScalerError", error)),
                        );
                    }
                }
            }
        }
        phase::DEGRADED => {}
        unknown => {
            upsert_condition(
                &mut next.conditions,
                "ReconcileBlocked",
                Some(blocked_condition(
                    "ReconcileBlocked",
                    "UnknownOperationPhase",
                    format!("unknown operation phase {unknown}"),
                )),
            );
        }
    }
    next
}

/// Candidates with a nonempty
/// [`conflict_files`](mirrorvol_api::NodeSyncStatus::conflict_files) in
/// `status.nodes[node]`.
fn candidates_with_conflicts(
    spec: &MirroredVolumeSpec,
    status: &MirroredVolumeStatus,
) -> Vec<(String, Vec<String>)> {
    spec.candidate_nodes
        .iter()
        .filter_map(|node| {
            let files = &status.nodes.get(node)?.conflict_files;
            (!files.is_empty()).then(|| (node.clone(), files.clone()))
        })
        .collect()
}

/// `bestEffort`: no operation/lock/epoch/drain handshake. Moves the
/// workload directly to `desired_active_node` and surfaces (never gates on)
/// Syncthing conflict files as a `Degraded`/`SyncConflict` condition.
async fn decide_best_effort<S: AppScaler>(
    volume_id: &str,
    spec: &MirroredVolumeSpec,
    mut next: MirroredVolumeStatus,
    contradictions: &[String],
    scaler: &S,
) -> MirroredVolumeStatus {
    upsert_condition(&mut next.conditions, "ReconcileBlocked", None);

    let conflicts = candidates_with_conflicts(spec, &next);
    upsert_condition(
        &mut next.conditions,
        "Degraded",
        (!conflicts.is_empty()).then(|| sync_conflict_condition(&conflicts)),
    );

    // Computed unconditionally, same "always visible" treatment as
    // strict's node_writer_agreement.
    // `Pending` itself is expected and non-gating; only a `Pending` that
    // has overstayed writerAgreementTimeoutSeconds gets a condition.
    let agreement_entry = agreement::best_effort(spec, &next);
    let stale_pending = agreement_entry.state == writer_agreement::PENDING
        && seconds_since(&agreement_entry.changed_at)
            >= u64::from(spec.storage.writer_agreement_timeout_seconds);
    next.writer_agreement = Some(agreement_entry);
    upsert_condition(
        &mut next.conditions,
        "WriterAgreementStale",
        stale_pending.then(|| {
            writer_agreement_stale_condition(spec.storage.writer_agreement_timeout_seconds)
        }),
    );

    // A backend identity contradiction is about backend trust, not write
    // concurrency. It blocks a move in both consistency modes, the same
    // way `decide_strict`'s idle-rejection does.
    if !contradictions.is_empty() {
        return next;
    }

    if next
        .active
        .as_ref()
        .is_none_or(|active| active.node != spec.desired_active_node)
    {
        match promote_workload(scaler, spec, volume_id, &spec.desired_active_node).await {
            Ok(()) => {
                next.active = Some(ActiveWriter {
                    node: spec.desired_active_node.clone(),
                    // Not a promotion epoch, just a visible move counter, since
                    // there's no operation history to look at in this mode.
                    epoch: next.active.as_ref().map_or(1, |active| active.epoch + 1),
                });
            }
            Err(error) => {
                upsert_condition(
                    &mut next.conditions,
                    "ReconcileBlocked",
                    Some(blocked_condition("ReconcileBlocked", "ScalerError", error)),
                );
            }
        }
    }
    next
}

#[cfg(test)]
mod tests {
    use super::*;
    use mirrorvol_api::{NodeSyncStatus, StorageSpec, WorkloadRef};

    struct FakeScaler;

    #[async_trait::async_trait]
    impl AppScaler for FakeScaler {
        async fn scale(&self, _: &str, _: u32) -> Result<(), String> {
            Ok(())
        }
        async fn is_scaled_to(&self, _: &str, _: u32) -> Result<bool, String> {
            Ok(true)
        }
        async fn set_node_selector(&self, _: &str, _: &str) -> Result<(), String> {
            Ok(())
        }
        async fn set_volume_claim(&self, _: &str, _: &str, _: &str) -> Result<(), String> {
            Ok(())
        }
    }

    /// Every `AppScaler` call fails.
    struct FailingScaler;

    #[async_trait::async_trait]
    impl AppScaler for FailingScaler {
        async fn scale(&self, _: &str, _: u32) -> Result<(), String> {
            Err("scale unavailable".to_owned())
        }
        async fn is_scaled_to(&self, _: &str, _: u32) -> Result<bool, String> {
            Err("scale unavailable".to_owned())
        }
        async fn set_node_selector(&self, _: &str, _: &str) -> Result<(), String> {
            Err("scale unavailable".to_owned())
        }
        async fn set_volume_claim(&self, _: &str, _: &str, _: &str) -> Result<(), String> {
            Err("scale unavailable".to_owned())
        }
    }

    /// Configurable `ClusterReader` stub — each method returns whatever was
    /// last set via its `fail_*`/`set_*` method, defaulting to values that
    /// let a volume with `spec`'s two ordinary candidates proceed all the
    /// way to `decide()`. A `calls` log is also kept, for the rare test
    /// that cares about short-circuiting itself rather than only the final
    /// [`ReconcileOutcome`] — most tests below only need the outcome.
    type BackendStatus = Result<(BTreeSet<String>, BTreeMap<String, u64>), String>;

    struct FakeClusterReader {
        backend_status: std::sync::Mutex<BackendStatus>,
        owned_by_another: std::sync::Mutex<Result<bool, String>>,
        deployment_desired_replicas: std::sync::Mutex<Result<u32, String>>,
        ensure_replica_claims: std::sync::Mutex<Result<(), String>>,
        calls: std::sync::Mutex<Vec<&'static str>>,
    }

    impl Default for FakeClusterReader {
        fn default() -> Self {
            Self {
                backend_status: std::sync::Mutex::new(Ok((
                    BTreeSet::from(["node-a".to_owned(), "node-b".to_owned()]),
                    BTreeMap::new(),
                ))),
                owned_by_another: std::sync::Mutex::new(Ok(false)),
                deployment_desired_replicas: std::sync::Mutex::new(Ok(1)),
                ensure_replica_claims: std::sync::Mutex::new(Ok(())),
                calls: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl FakeClusterReader {
        fn fail_ensure_replica_claims(&self) {
            *self.ensure_replica_claims.lock().unwrap() = Err("claims unavailable".to_owned());
        }
        fn fail_backend_status(&self) {
            *self.backend_status.lock().unwrap() = Err("backend status unavailable".to_owned());
        }
        fn fail_owned_by_another(&self) {
            *self.owned_by_another.lock().unwrap() = Err("sibling lookup unavailable".to_owned());
        }
        fn fail_deployment_desired_replicas(&self) {
            *self.deployment_desired_replicas.lock().unwrap() =
                Err("deployment unavailable".to_owned());
        }
        fn set_backend_status(&self, generations: BTreeMap<String, u64>) {
            *self.backend_status.lock().unwrap() = Ok((
                BTreeSet::from(["node-a".to_owned(), "node-b".to_owned()]),
                generations,
            ));
        }
        fn calls(&self) -> Vec<&'static str> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl ClusterReader for FakeClusterReader {
        async fn backend_status_for_candidates(
            &self,
            _spec: &MirroredVolumeSpec,
        ) -> Result<(BTreeSet<String>, BTreeMap<String, u64>), String> {
            self.calls
                .lock()
                .unwrap()
                .push("backend_status_for_candidates");
            self.backend_status.lock().unwrap().clone()
        }

        async fn owned_by_another_volume(
            &self,
            _self_name: &str,
            _spec: &MirroredVolumeSpec,
        ) -> Result<bool, String> {
            self.calls.lock().unwrap().push("owned_by_another_volume");
            self.owned_by_another.lock().unwrap().clone()
        }

        async fn deployment_desired_replicas(&self, _deployment: &str) -> Result<u32, String> {
            self.calls
                .lock()
                .unwrap()
                .push("deployment_desired_replicas");
            self.deployment_desired_replicas.lock().unwrap().clone()
        }

        async fn ensure_replica_claims(
            &self,
            _volume_id: &str,
            _spec: &MirroredVolumeSpec,
        ) -> Result<(), String> {
            self.calls.lock().unwrap().push("ensure_replica_claims");
            self.ensure_replica_claims.lock().unwrap().clone()
        }
    }

    fn annotations(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    fn degraded_status(id: u64) -> MirroredVolumeStatus {
        MirroredVolumeStatus {
            operation: Some(PromotionOperation {
                id,
                source: Some("node-a".to_owned()),
                target: "node-b".to_owned(),
                epoch: id,
                phase: phase::DEGRADED.to_owned(),
                started_at: "0".to_owned(),
                deadline: "0".to_owned(),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn no_recovery_when_not_degraded() {
        let status = MirroredVolumeStatus::default();
        let annotations = annotations(&[(
            mirrorvol_api::naming::RECOVER_DEGRADED_OPERATION_ANNOTATION,
            "1",
        )]);
        assert_eq!(degraded_recovery_message(&annotations, &status), None);
    }

    #[test]
    fn no_recovery_without_a_matching_annotation() {
        let status = degraded_status(2);
        assert_eq!(
            degraded_recovery_message(&Default::default(), &status),
            None
        );
    }

    #[test]
    fn no_recovery_when_the_annotation_names_a_different_stale_operation() {
        let status = degraded_status(2);
        let annotations = annotations(&[(
            mirrorvol_api::naming::RECOVER_DEGRADED_OPERATION_ANNOTATION,
            "1",
        )]);
        assert_eq!(degraded_recovery_message(&annotations, &status), None);
    }

    #[test]
    fn recovers_when_the_annotation_names_the_exact_stuck_operation() {
        let status = degraded_status(2);
        let annotations = annotations(&[
            (
                mirrorvol_api::naming::RECOVER_DEGRADED_OPERATION_ANNOTATION,
                "2",
            ),
            (
                mirrorvol_api::naming::RECOVERY_REASON_ANNOTATION,
                "fenced node-a via IPMI power-off",
            ),
        ]);
        let message =
            degraded_recovery_message(&annotations, &status).expect("matching operation id");
        assert!(message.contains("operation 2"));
        assert!(message.contains("node-b"));
        assert!(message.contains("fenced node-a via IPMI power-off"));
    }

    #[test]
    fn recovers_with_a_placeholder_reason_when_none_was_given() {
        let status = degraded_status(2);
        let annotations = annotations(&[(
            mirrorvol_api::naming::RECOVER_DEGRADED_OPERATION_ANNOTATION,
            "2",
        )]);
        let message =
            degraded_recovery_message(&annotations, &status).expect("matching operation id");
        assert!(message.contains("no reason given"));
    }

    #[tokio::test]
    async fn reconcile_takes_the_recovery_override_without_reading_the_cluster_at_all() {
        let status = degraded_status(2);
        let annotations = annotations(&[(
            mirrorvol_api::naming::RECOVER_DEGRADED_OPERATION_ANNOTATION,
            "2",
        )]);
        let reader = FakeClusterReader::default();
        let outcome = reconcile(
            "vol",
            Some(1),
            &annotations,
            &spec("node-a"),
            &status,
            &reader,
            &FakeScaler,
        )
        .await;
        let next = outcome.status.expect("override always produces a status");
        assert!(next.active.is_none());
        assert!(next.operation.is_none());
        assert!(next.grant.is_none());
        assert!(next.conditions.iter().any(|c| c.type_ == "Recovered"));
        assert_eq!(
            outcome.clear_annotations,
            vec![
                mirrorvol_api::naming::RECOVER_DEGRADED_OPERATION_ANNOTATION.to_owned(),
                mirrorvol_api::naming::RECOVERY_REASON_ANNOTATION.to_owned(),
            ]
        );
        assert_eq!(outcome.requeue_after, Duration::from_secs(1));
        // The override short-circuits before any ClusterReader call — a
        // read failure elsewhere must never block clearing a stuck
        // operation an operator has already externally confirmed safe.
        assert!(reader.calls().is_empty());
    }

    #[tokio::test]
    async fn reconcile_takes_the_reenrollment_override_after_reading_identity_generations() {
        let status = MirroredVolumeStatus {
            node_identity_generations: BTreeMap::from([("node-a".to_owned(), 1)]),
            ..Default::default()
        };
        let annotations = annotations(&[(
            mirrorvol_api::naming::REENROLL_NODE_IDENTITY_ANNOTATION,
            "node-a@2",
        )]);
        let reader = FakeClusterReader::default();
        reader.set_backend_status(BTreeMap::from([("node-a".to_owned(), 2)]));
        let outcome = reconcile(
            "vol",
            Some(1),
            &annotations,
            &spec("node-a"),
            &status,
            &reader,
            &FakeScaler,
        )
        .await;
        let next = outcome.status.expect("override always produces a status");
        assert_eq!(next.node_identity_generations.get("node-a"), Some(&2));
        assert!(next
            .conditions
            .iter()
            .any(|c| c.type_ == "IdentityReenrolled"));
        assert_eq!(outcome.requeue_after, Duration::from_secs(1));
        // Needs this node's freshly observed generation first — unlike the
        // recovery override, this one can't be evaluated from annotations
        // alone.
        assert_eq!(
            reader.calls(),
            vec!["ensure_replica_claims", "backend_status_for_candidates"]
        );
    }

    #[tokio::test]
    async fn reconcile_stops_at_the_first_read_that_fails() {
        let reader = FakeClusterReader::default();
        reader.fail_ensure_replica_claims();
        let outcome = reconcile(
            "vol",
            None,
            &BTreeMap::new(),
            &spec("node-a"),
            &MirroredVolumeStatus::default(),
            &reader,
            &FakeScaler,
        )
        .await;
        assert!(outcome.status.is_none());
        assert!(outcome.clear_annotations.is_empty());
        assert_eq!(outcome.requeue_after, Duration::from_secs(15));
        assert_eq!(outcome.error.as_deref(), Some("claims unavailable"));
        // Never reaches the second read.
        assert_eq!(reader.calls(), vec!["ensure_replica_claims"]);
    }

    #[tokio::test]
    async fn reconcile_surfaces_a_backend_status_read_failure() {
        let reader = FakeClusterReader::default();
        reader.fail_backend_status();
        let outcome = reconcile(
            "vol",
            None,
            &BTreeMap::new(),
            &spec("node-a"),
            &MirroredVolumeStatus::default(),
            &reader,
            &FakeScaler,
        )
        .await;
        assert!(outcome.status.is_none());
        assert_eq!(outcome.requeue_after, Duration::from_secs(15));
    }

    #[tokio::test]
    async fn reconcile_surfaces_an_owned_by_another_read_failure() {
        let reader = FakeClusterReader::default();
        reader.fail_owned_by_another();
        let outcome = reconcile(
            "vol",
            None,
            &BTreeMap::new(),
            &spec("node-a"),
            &MirroredVolumeStatus::default(),
            &reader,
            &FakeScaler,
        )
        .await;
        assert!(outcome.status.is_none());
        assert_eq!(outcome.requeue_after, Duration::from_secs(15));
    }

    #[tokio::test]
    async fn reconcile_surfaces_a_deployment_replicas_read_failure_only_before_adoption() {
        let reader = FakeClusterReader::default();
        reader.fail_deployment_desired_replicas();
        // status.active is None here — pre-adoption, so this read actually
        // runs. See AdmissionContext::pre_adoption_replicas.
        let outcome = reconcile(
            "vol",
            None,
            &BTreeMap::new(),
            &spec("node-a"),
            &MirroredVolumeStatus::default(),
            &reader,
            &FakeScaler,
        )
        .await;
        assert!(outcome.status.is_none());
        assert_eq!(outcome.requeue_after, Duration::from_secs(15));

        // Once adopted, the same failing read is never called at all.
        let reader = FakeClusterReader::default();
        reader.fail_deployment_desired_replicas();
        let adopted = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            ..Default::default()
        };
        let outcome = reconcile(
            "vol",
            None,
            &BTreeMap::new(),
            &spec("node-a"),
            &adopted,
            &reader,
            &FakeScaler,
        )
        .await;
        assert!(outcome.status.is_some());
        assert!(!reader.calls().contains(&"deployment_desired_replicas"));
    }

    #[tokio::test]
    async fn reconcile_reaches_decide_and_requeues_at_5s_with_no_operation_in_flight() {
        let reader = FakeClusterReader::default();
        let adopted = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            ..Default::default()
        };
        let outcome = reconcile(
            "vol",
            None,
            &BTreeMap::new(),
            &spec("node-a"),
            &adopted,
            &reader,
            &FakeScaler,
        )
        .await;
        assert!(outcome.status.is_some());
        assert!(outcome.clear_annotations.is_empty());
        assert_eq!(outcome.requeue_after, Duration::from_secs(5));
        assert!(outcome.error.is_none());
    }

    fn spec(target: &str) -> MirroredVolumeSpec {
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
            candidate_nodes: vec!["node-a".to_owned(), "node-b".to_owned()],
            desired_active_node: target.to_owned(),
            operation_timeout_seconds: 900,
            backend: mirrorvol_api::backend::SYNCTHING.to_owned(),
            consistency: Consistency::Strict,
        }
    }

    fn best_effort_spec(target: &str) -> MirroredVolumeSpec {
        MirroredVolumeSpec {
            consistency: Consistency::BestEffort,
            ..spec(target)
        }
    }

    /// Every test below runs with both `spec()`'s candidate nodes reporting
    /// the backend it names.
    fn available() -> BTreeSet<String> {
        BTreeSet::from(["node-a".to_owned(), "node-b".to_owned()])
    }

    /// The two admission checks that need real cluster state
    /// (`owned_by_another`, `pre_adoption_replicas`) default to "no
    /// conflict" / "not checked".
    fn admission(backend_available: &BTreeSet<String>) -> AdmissionContext<'_> {
        AdmissionContext {
            backend_available,
            owned_by_another: false,
            pre_adoption_replicas: None,
        }
    }

    /// No identity data at all.
    fn no_identity_generations() -> BTreeMap<String, u64> {
        BTreeMap::new()
    }

    fn has_condition(status: &MirroredVolumeStatus, type_: &str, reason: &str) -> bool {
        status
            .conditions
            .iter()
            .any(|condition| condition.type_ == type_ && condition.reason == reason)
    }

    #[tokio::test]
    async fn admission_rejects_a_candidate_missing_the_backend() {
        let status = MirroredVolumeStatus::default();
        let only_node_a = BTreeSet::from(["node-a".to_owned()]);
        let next = decide(
            "volume",
            &spec("node-b"),
            &status,
            &admission(&only_node_a),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert!(has_condition(
            &next,
            "AdmissionBlocked",
            "BackendUnavailable"
        ));
        assert!(next.operation.is_none());
    }

    #[tokio::test]
    async fn admission_rejects_a_non_deployment_workload() {
        let mut non_deployment = spec("node-b");
        non_deployment.workload.kind = "StatefulSet".to_owned();
        let next = decide(
            "volume",
            &non_deployment,
            &MirroredVolumeStatus::default(),
            &admission(&available()),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert!(has_condition(
            &next,
            "AdmissionBlocked",
            "UnsupportedWorkloadKind"
        ));
    }

    #[tokio::test]
    async fn admission_rejects_fewer_than_two_candidates() {
        let mut lone_candidate = spec("node-a");
        lone_candidate.candidate_nodes = vec!["node-a".to_owned()];
        let next = decide(
            "volume",
            &lone_candidate,
            &MirroredVolumeStatus::default(),
            &admission(&available()),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert!(has_condition(
            &next,
            "AdmissionBlocked",
            "InsufficientCandidates"
        ));
    }

    #[tokio::test]
    async fn admission_rejects_a_desired_node_outside_the_candidates() {
        let mut outside = spec("node-c");
        outside.candidate_nodes = vec!["node-a".to_owned(), "node-b".to_owned()];
        let next = decide(
            "volume",
            &outside,
            &MirroredVolumeStatus::default(),
            &admission(&available()),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert!(has_condition(
            &next,
            "AdmissionBlocked",
            "DesiredNotCandidate"
        ));
    }

    #[tokio::test]
    async fn admission_rejects_duplicate_candidate_nodes() {
        let mut duplicated = spec("node-a");
        duplicated.candidate_nodes = vec!["node-a".to_owned(), "node-a".to_owned()];
        let next = decide(
            "volume",
            &duplicated,
            &MirroredVolumeStatus::default(),
            &admission(&available()),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert!(has_condition(
            &next,
            "AdmissionBlocked",
            "DuplicateCandidates"
        ));
    }

    #[tokio::test]
    async fn admission_rejects_a_volume_already_owned_elsewhere() {
        let status = MirroredVolumeStatus::default();
        let backend_available = available();
        let admission = AdmissionContext {
            owned_by_another: true,
            ..admission(&backend_available)
        };
        let next = decide(
            "volume",
            &spec("node-b"),
            &status,
            &admission,
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert!(has_condition(
            &next,
            "AdmissionBlocked",
            "OwnedByAnotherVolume"
        ));
    }

    #[tokio::test]
    async fn admission_rejects_adoption_of_a_multi_replica_workload() {
        // Not yet adopted (no status.active) and the workload's Deployment
        // is still configured for more than one replica.
        let status = MirroredVolumeStatus::default();
        let backend_available = available();
        let admission = AdmissionContext {
            pre_adoption_replicas: Some(3),
            ..admission(&backend_available)
        };
        let next = decide(
            "volume",
            &spec("node-b"),
            &status,
            &admission,
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert!(has_condition(
            &next,
            "AdmissionBlocked",
            "PreAdoptionReplicaMismatch"
        ));
    }

    #[tokio::test]
    async fn admission_allows_adoption_of_a_freshly_authored_zero_replica_workload() {
        // The real-world bootstrap shape: an operator hands mirrorvol a
        // Deployment nobody has started yet (spec.replicas: 0), trusting
        // the strict promotion protocol's own Draining step (which scales
        // to 0 regardless of where it started) and GRANTING step (which
        // scales to 1 only once the writer is actually granted) to bring it
        // up safely. Zero must never trip `PreAdoptionReplicaMismatch`.
        let status = MirroredVolumeStatus::default();
        let backend_available = available();
        let admission = AdmissionContext {
            pre_adoption_replicas: Some(0),
            ..admission(&backend_available)
        };
        let next = decide(
            "volume",
            &spec("node-b"),
            &status,
            &admission,
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert!(next
            .conditions
            .iter()
            .all(|condition| condition.type_ != "AdmissionBlocked"));
        assert!(next.operation.is_some());
    }

    #[tokio::test]
    async fn admission_blocked_condition_clears_once_the_rejection_resolves() {
        let status = MirroredVolumeStatus::default();
        let backend_available = available();
        let blocked_admission = AdmissionContext {
            owned_by_another: true,
            ..admission(&backend_available)
        };
        let blocked = decide(
            "volume",
            &spec("node-b"),
            &status,
            &blocked_admission,
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert!(has_condition(
            &blocked,
            "AdmissionBlocked",
            "OwnedByAnotherVolume"
        ));

        let resolved = decide(
            "volume",
            &spec("node-b"),
            &blocked,
            &admission(&backend_available),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert!(resolved
            .conditions
            .iter()
            .all(|condition| condition.type_ != "AdmissionBlocked"));
    }

    #[tokio::test]
    async fn admission_rejection_persists_even_after_the_volume_was_already_adopted() {
        // `owned_by_another` is checked on every reconcile, not just at
        // creation.
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            ..Default::default()
        };
        let backend_available = available();
        let admission = AdmissionContext {
            owned_by_another: true,
            ..admission(&backend_available)
        };
        let next = decide(
            "volume",
            &spec("node-a"),
            &status,
            &admission,
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert!(has_condition(
            &next,
            "AdmissionBlocked",
            "OwnedByAnotherVolume"
        ));
        // Existing status is preserved/not clobbered.
        assert_eq!(
            next.active.map(|active| active.node),
            Some("node-a".to_owned())
        );
    }

    #[tokio::test]
    async fn admission_rejection_short_circuits_before_the_identity_contradiction_check() {
        // Even though node-b's observed generation would contradict the
        // recorded baseline, an admission rejection returns before computing
        // it at all (no `IdentityChanged` condition).
        let status = MirroredVolumeStatus {
            node_identity_generations: BTreeMap::from([("node-b".to_owned(), 1)]),
            ..Default::default()
        };
        let identity_generations =
            BTreeMap::from([("node-a".to_owned(), 1), ("node-b".to_owned(), 2)]);
        let backend_available = available();
        let admission = AdmissionContext {
            owned_by_another: true,
            ..admission(&backend_available)
        };
        let next = decide(
            "volume",
            &spec("node-b"),
            &status,
            &admission,
            &identity_generations,
            &FakeScaler,
        )
        .await;
        assert!(has_condition(
            &next,
            "AdmissionBlocked",
            "OwnedByAnotherVolume"
        ));
        assert!(next
            .conditions
            .iter()
            .all(|condition| condition.type_ != "IdentityChanged"));
    }

    #[tokio::test]
    async fn a_multi_replica_workload_check_never_blocks_an_already_adopted_volume() {
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            ..Default::default()
        };
        let next = decide(
            "volume",
            &spec("node-a"),
            &status,
            &admission(&available()),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert!(next
            .conditions
            .iter()
            .all(|condition| condition.type_ != "AdmissionBlocked"));
        assert_eq!(
            next.active.map(|active| active.node),
            Some("node-a".to_owned())
        );
    }

    #[tokio::test]
    async fn operation_timeout_is_per_volume_not_hardcoded() {
        let status = MirroredVolumeStatus::default();
        let mut short_timeout = spec("node-b");
        short_timeout.operation_timeout_seconds = 60;
        let next = decide(
            "volume",
            &short_timeout,
            &status,
            &admission(&available()),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        let operation = next.operation.expect("operation started");
        let deadline: u64 = operation.deadline.parse().expect("numeric deadline");
        let started_at: u64 = operation.started_at.parse().expect("numeric started_at");
        assert_eq!(deadline - started_at, 60);
    }

    #[tokio::test]
    async fn operation_target_is_immutable_after_drain_starts() {
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            operation: Some(PromotionOperation {
                id: 2,
                source: Some("node-a".to_owned()),
                target: "node-b".to_owned(),
                epoch: 2,
                phase: phase::DRAINING.to_owned(),
                started_at: "0".to_owned(),
                deadline: "9999999999".to_owned(),
            }),
            ..Default::default()
        };
        let next = decide(
            "volume",
            &spec("node-a"),
            &status,
            &admission(&available()),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert!(has_condition(
            &next,
            "ReconcileBlocked",
            "OperationTargetMismatch"
        ));
        // The operation itself is untouched, still targeting node-b.
        assert_eq!(
            next.operation.map(|operation| operation.target),
            Some("node-b".to_owned())
        );
    }

    #[tokio::test]
    async fn identity_changed_condition_survives_an_operation_target_mismatch_rejection() {
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            operation: Some(PromotionOperation {
                id: 2,
                source: Some("node-a".to_owned()),
                target: "node-b".to_owned(),
                epoch: 2,
                phase: phase::DRAINING.to_owned(),
                started_at: "0".to_owned(),
                deadline: "9999999999".to_owned(),
            }),
            node_identity_generations: BTreeMap::from([("node-b".to_owned(), 1)]),
            ..Default::default()
        };
        let identity_generations =
            BTreeMap::from([("node-a".to_owned(), 1), ("node-b".to_owned(), 2)]);
        // desiredActiveNode ("node-a") disagrees with the in-flight
        // operation's target ("node-b") — triggers OperationTargetMismatch.
        let next = decide(
            "volume",
            &spec("node-a"),
            &status,
            &admission(&available()),
            &identity_generations,
            &FakeScaler,
        )
        .await;
        assert!(has_condition(
            &next,
            "ReconcileBlocked",
            "OperationTargetMismatch"
        ));
        assert!(has_condition(
            &next,
            "IdentityChanged",
            "BackendIdentityContradiction"
        ));
    }

    #[tokio::test]
    async fn a_scaler_failure_is_surfaced_instead_of_silently_retried() {
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            ..Default::default()
        };
        let next = decide(
            "volume",
            &spec("node-b"),
            &status,
            &admission(&available()),
            &no_identity_generations(),
            &FailingScaler,
        )
        .await;
        assert!(has_condition(&next, "ReconcileBlocked", "ScalerError"));
        // The failed scale-to-zero never started an operation.
        assert!(next.operation.is_none());
    }

    #[tokio::test]
    async fn reconcile_blocked_condition_clears_once_the_scaler_recovers() {
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            ..Default::default()
        };
        let blocked = decide(
            "volume",
            &spec("node-b"),
            &status,
            &admission(&available()),
            &no_identity_generations(),
            &FailingScaler,
        )
        .await;
        assert!(has_condition(&blocked, "ReconcileBlocked", "ScalerError"));
        assert!(blocked.operation.is_none());

        let recovered = decide(
            "volume",
            &spec("node-b"),
            &blocked,
            &admission(&available()),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert!(recovered
            .conditions
            .iter()
            .all(|condition| condition.type_ != "ReconcileBlocked"));
        assert!(recovered.operation.is_some());
    }

    #[tokio::test]
    async fn decide_strict_flags_an_unrecognized_operation_phase() {
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            operation: Some(PromotionOperation {
                id: 2,
                source: Some("node-a".to_owned()),
                target: "node-b".to_owned(),
                epoch: 2,
                phase: "SomeFuturePhase".to_owned(),
                started_at: "0".to_owned(),
                deadline: "9999999999".to_owned(),
            }),
            ..Default::default()
        };
        let next = decide(
            "volume",
            &spec("node-b"),
            &status,
            &admission(&available()),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert!(has_condition(
            &next,
            "ReconcileBlocked",
            "UnknownOperationPhase"
        ));
        // The operation itself is left exactly as observed, not mutated.
        assert_eq!(
            next.operation.map(|operation| operation.phase),
            Some("SomeFuturePhase".to_owned())
        );
    }

    #[tokio::test]
    async fn a_scaler_failure_during_granting_is_surfaced_and_the_operation_stays_pending() {
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            operation: Some(PromotionOperation {
                id: 2,
                source: Some("node-a".to_owned()),
                target: "node-b".to_owned(),
                epoch: 2,
                phase: phase::GRANTING.to_owned(),
                started_at: "0".to_owned(),
                deadline: "9999999999".to_owned(),
            }),
            // Confirmed-writer now requires both the live grant (below) and
            // a matching local lock, not role/epoch alone.
            grant: Some(PromotionGrant {
                operation_id: 2,
                target: "node-b".to_owned(),
                epoch: 2,
            }),
            nodes: std::collections::BTreeMap::from([
                (
                    "node-a".to_owned(),
                    NodeSyncStatus {
                        role: role::STANDBY.to_owned(),
                        ..Default::default()
                    },
                ),
                (
                    "node-b".to_owned(),
                    NodeSyncStatus {
                        role: role::WRITER.to_owned(),
                        epoch: Some(2),
                        lock_epoch: Some(2),
                        ..Default::default()
                    },
                ),
            ]),
            ..Default::default()
        };
        let next = decide(
            "volume",
            &spec("node-b"),
            &status,
            &admission(&available()),
            &no_identity_generations(),
            &FailingScaler,
        )
        .await;
        assert!(has_condition(&next, "ReconcileBlocked", "ScalerError"));
        // Never committed, the failed promote_workload call means the
        // operation/active fields stay exactly where they were.
        assert_eq!(
            next.operation
                .as_ref()
                .map(|operation| operation.phase.as_str()),
            Some(phase::GRANTING)
        );
        assert_eq!(
            next.active.map(|active| active.node),
            Some("node-a".to_owned())
        );
    }

    #[tokio::test]
    async fn controller_preserves_agent_owned_nodes() {
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            nodes: std::collections::BTreeMap::from([(
                "node-b".to_owned(),
                NodeSyncStatus {
                    role: role::STANDBY.to_owned(),
                    ready: true,
                    lock_absent: true,
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        let next = decide(
            "volume",
            &spec("node-b"),
            &status,
            &admission(&available()),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert_eq!(next.nodes, status.nodes);
    }

    #[tokio::test]
    async fn grants_only_after_release_observation_and_source_standby() {
        let mut status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            operation: Some(PromotionOperation {
                id: 2,
                source: Some("node-a".to_owned()),
                target: "node-b".to_owned(),
                epoch: 2,
                phase: phase::AWAITING_RELEASE.to_owned(),
                started_at: "0".to_owned(),
                deadline: "9999999999".to_owned(),
            }),
            nodes: std::collections::BTreeMap::from([
                (
                    "node-a".to_owned(),
                    NodeSyncStatus {
                        role: role::WRITER.to_owned(),
                        released_operation: Some(2),
                        ..Default::default()
                    },
                ),
                (
                    "node-b".to_owned(),
                    NodeSyncStatus {
                        role: role::STANDBY.to_owned(),
                        ready: true,
                        release_observed_operation: Some(2),
                        ..Default::default()
                    },
                ),
            ]),
            ..Default::default()
        };
        status = decide(
            "volume",
            &spec("node-b"),
            &status,
            &admission(&available()),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert_eq!(
            status
                .operation
                .as_ref()
                .map(|operation| operation.phase.as_str()),
            Some(phase::ENFORCING_STANDBY)
        );
        status.nodes.get_mut("node-a").expect("source").role = role::STANDBY.to_owned();
        let status = decide(
            "volume",
            &spec("node-b"),
            &status,
            &admission(&available()),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert_eq!(
            status.grant.as_ref().map(|grant| grant.target.as_str()),
            Some("node-b")
        );
    }

    #[tokio::test]
    async fn identity_contradiction_forces_degraded_when_an_operation_is_in_flight() {
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            operation: Some(PromotionOperation {
                id: 2,
                source: Some("node-a".to_owned()),
                target: "node-b".to_owned(),
                epoch: 2,
                phase: phase::AWAITING_RELEASE.to_owned(),
                started_at: "0".to_owned(),
                deadline: "9999999999".to_owned(),
            }),
            node_identity_generations: BTreeMap::from([("node-b".to_owned(), 1)]),
            ..Default::default()
        };
        // node-b's observed generation (2) now contradicts the recorded
        // baseline (1).
        let identity_generations =
            BTreeMap::from([("node-a".to_owned(), 1), ("node-b".to_owned(), 2)]);
        let next = decide(
            "volume",
            &spec("node-b"),
            &status,
            &admission(&available()),
            &identity_generations,
            &FakeScaler,
        )
        .await;
        assert_eq!(
            next.operation
                .as_ref()
                .map(|operation| operation.phase.as_str()),
            Some(phase::DEGRADED)
        );
        assert!(next.grant.is_none());
        assert!(next
            .conditions
            .iter()
            .any(|condition| condition.type_ == "IdentityChanged"));
    }

    #[tokio::test]
    async fn a_persisting_identity_contradiction_does_not_grow_conditions_without_bound() {
        let mut status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            operation: Some(PromotionOperation {
                id: 2,
                source: Some("node-a".to_owned()),
                target: "node-b".to_owned(),
                epoch: 2,
                phase: phase::AWAITING_RELEASE.to_owned(),
                started_at: "0".to_owned(),
                deadline: "9999999999".to_owned(),
            }),
            node_identity_generations: BTreeMap::from([("node-b".to_owned(), 1)]),
            ..Default::default()
        };
        let identity_generations =
            BTreeMap::from([("node-a".to_owned(), 1), ("node-b".to_owned(), 2)]);
        for _ in 0..5 {
            status = decide(
                "volume",
                &spec("node-b"),
                &status,
                &admission(&available()),
                &identity_generations,
                &FakeScaler,
            )
            .await;
        }
        assert_eq!(
            status
                .conditions
                .iter()
                .filter(|condition| condition.type_ == "IdentityChanged")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn identity_contradiction_blocks_a_new_operation_from_starting_when_idle() {
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            node_identity_generations: BTreeMap::from([("node-b".to_owned(), 1)]),
            ..Default::default()
        };
        let identity_generations =
            BTreeMap::from([("node-a".to_owned(), 1), ("node-b".to_owned(), 2)]);
        let next = decide(
            "volume",
            &spec("node-b"),
            &status,
            &admission(&available()),
            &identity_generations,
            &FakeScaler,
        )
        .await;
        assert!(next.operation.is_none());
        assert!(next
            .conditions
            .iter()
            .any(|condition| condition.type_ == "IdentityChanged"));
    }

    #[tokio::test]
    async fn identity_changed_condition_is_visible_even_when_no_move_is_requested() {
        // Steady state (`desiredActiveNode` already active, no operation)
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            node_identity_generations: BTreeMap::from([("node-b".to_owned(), 1)]),
            ..Default::default()
        };
        let identity_generations =
            BTreeMap::from([("node-a".to_owned(), 1), ("node-b".to_owned(), 2)]);
        let next = decide(
            "volume",
            &spec("node-a"),
            &status,
            &admission(&available()),
            &identity_generations,
            &FakeScaler,
        )
        .await;
        assert!(next
            .conditions
            .iter()
            .any(|condition| condition.type_ == "IdentityChanged"));
    }

    #[tokio::test]
    async fn identity_changed_condition_clears_once_the_contradiction_resolves() {
        let mut status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            node_identity_generations: BTreeMap::from([("node-b".to_owned(), 1)]),
            ..Default::default()
        };
        let identity_generations =
            BTreeMap::from([("node-a".to_owned(), 1), ("node-b".to_owned(), 2)]);
        status = decide(
            "volume",
            &spec("node-a"),
            &status,
            &admission(&available()),
            &identity_generations,
            &FakeScaler,
        )
        .await;
        assert!(status
            .conditions
            .iter()
            .any(|condition| condition.type_ == "IdentityChanged"));

        status
            .node_identity_generations
            .insert("node-b".to_owned(), 2);
        let resolved = decide(
            "volume",
            &spec("node-a"),
            &status,
            &admission(&available()),
            &identity_generations,
            &FakeScaler,
        )
        .await;
        assert!(resolved
            .conditions
            .iter()
            .all(|condition| condition.type_ != "IdentityChanged"));
    }

    #[test]
    fn identity_reenrollment_requires_the_annotation_to_name_the_current_observed_generation() {
        let observed = BTreeMap::from([("node-b".to_owned(), 2)]);
        let baseline = BTreeMap::from([("node-b".to_owned(), 1)]);
        let stale = BTreeMap::from([(
            mirrorvol_api::naming::REENROLL_NODE_IDENTITY_ANNOTATION.to_owned(),
            "node-b@1".to_owned(), // names the old, not the current, generation
        )]);
        assert!(identity_reenrollment(&stale, &baseline, &observed).is_none());

        let current = BTreeMap::from([(
            mirrorvol_api::naming::REENROLL_NODE_IDENTITY_ANNOTATION.to_owned(),
            "node-b@2".to_owned(),
        )]);
        let (node, generation, message) =
            identity_reenrollment(&current, &baseline, &observed).expect("matches current");
        assert_eq!(node, "node-b");
        assert_eq!(generation, 2);
        assert!(message.contains("node-b"));
    }

    #[test]
    fn identity_reenrollment_is_single_use_once_the_baseline_already_matches() {
        // Same annotation value as above, but the baseline already reflects it.
        let observed = BTreeMap::from([("node-b".to_owned(), 2)]);
        let baseline = BTreeMap::from([("node-b".to_owned(), 2)]);
        let annotations = BTreeMap::from([(
            mirrorvol_api::naming::REENROLL_NODE_IDENTITY_ANNOTATION.to_owned(),
            "node-b@2".to_owned(),
        )]);
        assert!(identity_reenrollment(&annotations, &baseline, &observed).is_none());
    }

    #[tokio::test]
    async fn first_sighting_of_an_identity_generation_establishes_a_baseline_without_triggering() {
        let status = MirroredVolumeStatus::default();
        let identity_generations =
            BTreeMap::from([("node-a".to_owned(), 1), ("node-b".to_owned(), 1)]);
        let next = decide(
            "volume",
            &spec("node-b"),
            &status,
            &admission(&available()),
            &identity_generations,
            &FakeScaler,
        )
        .await;
        assert_eq!(next.node_identity_generations, identity_generations);
        assert!(next
            .conditions
            .iter()
            .all(|condition| condition.type_ != "IdentityChanged"));
    }

    #[tokio::test]
    async fn an_unrelated_candidates_unchanged_generation_never_retriggers() {
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            node_identity_generations: BTreeMap::from([
                ("node-a".to_owned(), 1),
                ("node-b".to_owned(), 1),
            ]),
            ..Default::default()
        };
        // Both nodes report exactly what's already recorded (no change).
        let identity_generations =
            BTreeMap::from([("node-a".to_owned(), 1), ("node-b".to_owned(), 1)]);
        let next = decide(
            "volume",
            &spec("node-b"),
            &status,
            &admission(&available()),
            &identity_generations,
            &FakeScaler,
        )
        .await;
        assert!(next.operation.is_some());
    }

    #[tokio::test]
    async fn best_effort_moves_the_workload_immediately_with_no_operation() {
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            ..Default::default()
        };
        let next = decide(
            "volume",
            &best_effort_spec("node-b"),
            &status,
            &admission(&available()),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert_eq!(
            next.active.map(|active| active.node),
            Some("node-b".to_owned())
        );
        assert!(next.operation.is_none());
        assert!(next.grant.is_none());
    }

    #[tokio::test]
    async fn best_effort_identity_contradiction_blocks_a_move_and_is_visible() {
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            node_identity_generations: BTreeMap::from([("node-b".to_owned(), 1)]),
            ..Default::default()
        };
        let identity_generations =
            BTreeMap::from([("node-a".to_owned(), 1), ("node-b".to_owned(), 2)]);
        let next = decide(
            "volume",
            &best_effort_spec("node-b"),
            &status,
            &admission(&available()),
            &identity_generations,
            &FakeScaler,
        )
        .await;
        // Never promoted, still on node-a.
        assert_eq!(
            next.active.as_ref().map(|active| active.node.clone()),
            Some("node-a".to_owned())
        );
        assert!(has_condition(
            &next,
            "IdentityChanged",
            "BackendIdentityContradiction"
        ));
    }

    #[tokio::test]
    async fn best_effort_scaler_failure_is_surfaced_instead_of_silently_retried() {
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            ..Default::default()
        };
        let next = decide(
            "volume",
            &best_effort_spec("node-b"),
            &status,
            &admission(&available()),
            &no_identity_generations(),
            &FailingScaler,
        )
        .await;
        assert!(has_condition(&next, "ReconcileBlocked", "ScalerError"));
        // Never promoted — the failed promote_workload call leaves active
        // exactly where it was.
        assert_eq!(
            next.active.map(|active| active.node),
            Some("node-a".to_owned())
        );
    }

    #[tokio::test]
    async fn best_effort_conflict_files_produce_a_degraded_condition_that_clears() {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            "node-b".to_owned(),
            mirrorvol_api::NodeSyncStatus {
                conflict_files: vec!["a.sync-conflict-1".to_owned()],
                ..Default::default()
            },
        );
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            nodes,
            ..Default::default()
        };
        let next = decide(
            "volume",
            &best_effort_spec("node-a"),
            &status,
            &admission(&available()),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert!(next
            .conditions
            .iter()
            .any(|condition| condition.type_ == "Degraded" && condition.reason == "SyncConflict"));

        // Resolved: the agent no longer reports any conflict for node-b.
        let mut resolved_status = next;
        resolved_status
            .nodes
            .get_mut("node-b")
            .expect("node-b entry")
            .conflict_files
            .clear();
        let resolved = decide(
            "volume",
            &best_effort_spec("node-a"),
            &resolved_status,
            &admission(&available()),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert!(resolved
            .conditions
            .iter()
            .all(|condition| condition.type_ != "Degraded"));
    }

    #[tokio::test]
    async fn best_effort_persisting_conflict_does_not_grow_conditions_without_bound() {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            "node-b".to_owned(),
            mirrorvol_api::NodeSyncStatus {
                conflict_files: vec!["a.sync-conflict-1".to_owned()],
                ..Default::default()
            },
        );
        let mut status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            nodes,
            ..Default::default()
        };
        for _ in 0..5 {
            status = decide(
                "volume",
                &best_effort_spec("node-a"),
                &status,
                &admission(&available()),
                &no_identity_generations(),
                &FakeScaler,
            )
            .await;
        }
        assert_eq!(
            status
                .conditions
                .iter()
                .filter(|condition| condition.type_ == "Degraded")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn a_source_stuck_writer_past_enforcing_standby_is_a_contradiction_not_pending() {
        // The source's role/lock disagreement is expected during
        // AwaitingRelease and EnforcingStandby (release_writer already ran;
        // enforce_standby runs sometime during EnforcingStandby, not
        // necessarily the instant that phase starts — a real async gap
        // against a live agent, see in_flight_transition's own doc
        // comment). If it's still Writer/lock-absent once the phase has
        // moved on past that (here: Granting), enforce_standby didn't
        // take — that's a stuck transition, not an in-flight one, and
        // shouldn't wait on the full operation deadline to be noticed.
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            operation: Some(PromotionOperation {
                id: 2,
                source: Some("node-a".to_owned()),
                target: "node-b".to_owned(),
                epoch: 2,
                phase: phase::GRANTING.to_owned(),
                started_at: "0".to_owned(),
                deadline: "9999999999".to_owned(),
            }),
            nodes: BTreeMap::from([
                (
                    "node-a".to_owned(),
                    NodeSyncStatus {
                        role: role::WRITER.to_owned(), // should have flipped to Standby by now
                        released_operation: Some(2),
                        lock_absent: true,
                        ..Default::default()
                    },
                ),
                (
                    "node-b".to_owned(),
                    NodeSyncStatus {
                        role: role::STANDBY.to_owned(),
                        ready: true,
                        release_observed_operation: Some(2),
                        ..Default::default()
                    },
                ),
            ]),
            ..Default::default()
        };
        let next = decide(
            "volume",
            &spec("node-b"),
            &status,
            &admission(&available()),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert_eq!(
            next.node_writer_agreement
                .get("node-a")
                .map(|entry| entry.state.as_str()),
            Some(mirrorvol_api::writer_agreement::CONTRADICTION)
        );
        assert_eq!(
            next.operation
                .as_ref()
                .map(|operation| operation.phase.as_str()),
            Some(phase::DEGRADED)
        );
    }

    #[tokio::test]
    async fn a_source_still_writer_during_enforcing_standby_is_pending_not_contradiction() {
        // Regression for a real bug caught against a live KinD cluster: the
        // controller can advance the phase to EnforcingStandby before the
        // agent's own
        // next reconcile tick has actually called enforce_standby —  an
        // ordinary, expected async gap, not a stuck transition. An
        // earlier, tighter version of in_flight_transition only excused
        // this during AwaitingRelease, which forced every real promotion
        // straight to Degraded within seconds of entering EnforcingStandby.
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            operation: Some(PromotionOperation {
                id: 2,
                source: Some("node-a".to_owned()),
                target: "node-b".to_owned(),
                epoch: 2,
                phase: phase::ENFORCING_STANDBY.to_owned(),
                started_at: "0".to_owned(),
                deadline: "9999999999".to_owned(),
            }),
            nodes: BTreeMap::from([
                (
                    "node-a".to_owned(),
                    NodeSyncStatus {
                        role: role::WRITER.to_owned(), // enforce_standby hasn't landed yet
                        released_operation: Some(2),
                        lock_absent: true,
                        ..Default::default()
                    },
                ),
                (
                    "node-b".to_owned(),
                    NodeSyncStatus {
                        role: role::STANDBY.to_owned(),
                        ready: true,
                        release_observed_operation: Some(2),
                        ..Default::default()
                    },
                ),
            ]),
            ..Default::default()
        };
        let next = decide(
            "volume",
            &spec("node-b"),
            &status,
            &admission(&available()),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert_eq!(
            next.node_writer_agreement
                .get("node-a")
                .map(|entry| entry.state.as_str()),
            Some(mirrorvol_api::writer_agreement::PENDING)
        );
        // Not forced into Degraded — this is an expected, still-in-flight
        // transition, exactly like AwaitingRelease already was.
        assert_eq!(
            next.operation
                .as_ref()
                .map(|operation| operation.phase.as_str()),
            Some(phase::ENFORCING_STANDBY)
        );
    }

    #[tokio::test]
    async fn granting_does_not_commit_without_a_matching_local_lock() {
        // The crash window this whole design closes: role already says
        // Writer (the backend flipped it), but the local lock was never
        // created (or doesn't match this epoch yet) — writer_confirmed
        // must not be satisfied by role alone.
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            operation: Some(PromotionOperation {
                id: 2,
                source: Some("node-a".to_owned()),
                target: "node-b".to_owned(),
                epoch: 2,
                phase: phase::GRANTING.to_owned(),
                started_at: "0".to_owned(),
                deadline: "9999999999".to_owned(),
            }),
            grant: Some(PromotionGrant {
                operation_id: 2,
                target: "node-b".to_owned(),
                epoch: 2,
            }),
            nodes: BTreeMap::from([
                (
                    "node-a".to_owned(),
                    NodeSyncStatus {
                        role: role::STANDBY.to_owned(),
                        ..Default::default()
                    },
                ),
                (
                    "node-b".to_owned(),
                    NodeSyncStatus {
                        role: role::WRITER.to_owned(),
                        epoch: Some(2),
                        lock_absent: true, // no lock_epoch — the crash case
                        ..Default::default()
                    },
                ),
            ]),
            ..Default::default()
        };
        let next = decide(
            "volume",
            &spec("node-b"),
            &status,
            &admission(&available()),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        // Held in Granting — the grant explains the disagreement, so this
        // is Pending, not a forced Degraded, and the promotion just hasn't
        // committed yet.
        assert_eq!(
            next.operation
                .as_ref()
                .map(|operation| operation.phase.as_str()),
            Some(phase::GRANTING)
        );
        // Never committed — active still names the old writer, node-a.
        assert_eq!(
            next.active.as_ref().map(|active| active.node.as_str()),
            Some("node-a")
        );
        assert_eq!(
            next.node_writer_agreement
                .get("node-b")
                .map(|entry| entry.state.as_str()),
            Some(mirrorvol_api::writer_agreement::PENDING)
        );
    }

    #[tokio::test]
    async fn a_contradiction_with_no_grant_forces_degraded() {
        // Writer role claimed with a mismatched lock and nothing
        // authorizing it — no grant, no matching active epoch.
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            operation: Some(PromotionOperation {
                id: 2,
                source: Some("node-a".to_owned()),
                target: "node-b".to_owned(),
                epoch: 2,
                phase: phase::AWAITING_RELEASE.to_owned(),
                started_at: "0".to_owned(),
                deadline: "9999999999".to_owned(),
            }),
            nodes: BTreeMap::from([(
                "node-c".to_owned(),
                NodeSyncStatus {
                    role: role::WRITER.to_owned(),
                    lock_absent: true,
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        let mut three_candidates = spec("node-b");
        three_candidates.candidate_nodes = vec![
            "node-a".to_owned(),
            "node-b".to_owned(),
            "node-c".to_owned(),
        ];
        let next = decide(
            "volume",
            &three_candidates,
            &status,
            &admission(&BTreeSet::from([
                "node-a".to_owned(),
                "node-b".to_owned(),
                "node-c".to_owned(),
            ])),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert!(has_condition(
            &next,
            "WriterLockMismatch",
            "LocalLockDisagreesWithRole"
        ));
        assert_eq!(
            next.operation
                .as_ref()
                .map(|operation| operation.phase.as_str()),
            Some(phase::DEGRADED)
        );
        assert!(next.grant.is_none());
    }

    #[tokio::test]
    async fn an_idle_contradiction_blocks_a_new_operation_from_starting() {
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            nodes: BTreeMap::from([(
                "node-b".to_owned(),
                NodeSyncStatus {
                    role: role::WRITER.to_owned(), // no legitimate claim at all
                    lock_absent: true,
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        let next = decide(
            "volume",
            &spec("node-b"),
            &status,
            &admission(&available()),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert!(next.operation.is_none());
        assert!(has_condition(
            &next,
            "WriterLockMismatch",
            "LocalLockDisagreesWithRole"
        ));
    }

    #[tokio::test]
    async fn best_effort_writer_agreement_converges_at_zero_or_one_writer() {
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            nodes: BTreeMap::from([(
                "node-a".to_owned(),
                NodeSyncStatus {
                    role: role::WRITER.to_owned(),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        let next = decide(
            "volume",
            &best_effort_spec("node-a"),
            &status,
            &admission(&available()),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert_eq!(
            next.writer_agreement.map(|entry| entry.state),
            Some(mirrorvol_api::writer_agreement::CONVERGED.to_owned())
        );
    }

    #[tokio::test]
    async fn best_effort_writer_agreement_is_pending_with_more_than_one_writer() {
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            nodes: BTreeMap::from([
                (
                    "node-a".to_owned(),
                    NodeSyncStatus {
                        role: role::WRITER.to_owned(),
                        ..Default::default()
                    },
                ),
                (
                    "node-b".to_owned(),
                    NodeSyncStatus {
                        role: role::WRITER.to_owned(),
                        ..Default::default()
                    },
                ),
            ]),
            ..Default::default()
        };
        let next = decide(
            "volume",
            &best_effort_spec("node-a"),
            &status,
            &admission(&available()),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert_eq!(
            next.writer_agreement
                .as_ref()
                .map(|entry| entry.state.as_str()),
            Some(mirrorvol_api::writer_agreement::PENDING)
        );
        // Fresh Pending — not stale yet, no condition.
        assert!(next
            .conditions
            .iter()
            .all(|condition| condition.type_ != "WriterAgreementStale"));
    }

    #[tokio::test]
    async fn best_effort_pending_only_becomes_stale_after_the_configured_timeout() {
        let mut spec = best_effort_spec("node-a");
        spec.storage.writer_agreement_timeout_seconds = 60;
        let nodes = BTreeMap::from([
            (
                "node-a".to_owned(),
                NodeSyncStatus {
                    role: role::WRITER.to_owned(),
                    ..Default::default()
                },
            ),
            (
                "node-b".to_owned(),
                NodeSyncStatus {
                    role: role::WRITER.to_owned(),
                    ..Default::default()
                },
            ),
        ]);
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 1,
            }),
            nodes: nodes.clone(),
            writer_agreement: Some(mirrorvol_api::WriterAgreementEntry {
                state: mirrorvol_api::writer_agreement::PENDING.to_owned(),
                // Well past the 60s timeout.
                changed_at: Time(chrono::Utc::now() - chrono::Duration::seconds(120)),
            }),
            ..Default::default()
        };
        let next = decide(
            "volume",
            &spec,
            &status,
            &admission(&available()),
            &no_identity_generations(),
            &FakeScaler,
        )
        .await;
        assert!(has_condition(
            &next,
            "WriterAgreementStale",
            "MultipleWriters"
        ));
    }
}
