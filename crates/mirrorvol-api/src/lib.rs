//! The [`MirroredVolume`] and [`BackendNode`] CRD types. No I/O or
//! reconcile logic.

use std::collections::BTreeMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The CRD group every `mirrorvol` custom resource ([`MirroredVolume`],
/// [`BackendNode`]) is registered under.
///
/// This can't be wired directly into their `#[kube(group = ...)]`
/// attributes — `kube`'s [`CustomResource`] derive parses `group`/`version`
/// as string literals at macro-expansion time, before any `const` could be
/// resolved — so the two attributes below are kept in sync with this
/// constant by hand. This crate's own unit tests
/// (`mirrored_volume_group_version_match_the_root_constants` and
/// `backend_node_group_version_match_the_root_constants`) are the safety
/// net: either type's real, derived
/// [`Resource::group`](kube::Resource::group)/[`Resource::version`](kube::Resource::version)
/// drifting from these constants fails `cargo test`.
pub const API_GROUP: &str = "homelab.internal";

/// The CRD version every `mirrorvol` custom resource is currently at. See
/// [`API_GROUP`] for how this stays in sync with the derived types.
pub const API_VERSION: &str = "v1alpha1";

/// Schema for the `detail` fields below, which hold arbitrary backend-
/// specific JSON the controller never interprets. [`schemars`]' default
/// schema for [`serde_json::Value`] (an empty object) fails Kubernetes'
/// structural-schema validation; `x-kubernetes-preserve-unknown-fields`
/// is the CRD spec's escape hatch for that.
fn opaque_json_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    let mut schema = schemars::schema::SchemaObject::default();
    schema.extensions.insert(
        "x-kubernetes-preserve-unknown-fields".to_string(),
        serde_json::Value::Bool(true),
    );
    schemars::schema::Schema::Object(schema)
}

/// `spec.consistency`'s schema: `Consistency`'s own derive already gives
/// schemars the right `enum: [...]` — this only adds the CEL rule the API
/// server enforces on top of it, rejecting any change after creation.
/// Switching modes mid-volume is not supported for simplicity.
fn consistency_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    let mut schema = <Consistency as JsonSchema>::json_schema(gen).into_object();
    schema.extensions.insert(
        "x-kubernetes-validations".to_string(),
        serde_json::json!([{
            "rule": "self == oldSelf",
            "message": "consistency is immutable; delete and recreate the MirroredVolume to change it"
        }]),
    );
    schemars::schema::Schema::Object(schema)
}

// Field doc comments on this struct (and every other #[derive(JsonSchema)]
// struct below) feed straight into the CRD's OpenAPI schema `description`
// via schemars — `kubectl explain` renders them as plain text, so none of
// them use rustdoc `[link]` syntax even where it'd otherwise apply.
#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, PartialEq, JsonSchema)]
// group/version must equal API_GROUP/API_VERSION — see that constant's own
// doc comment for why this can't be wired in directly.
#[kube(
    group = "homelab.internal",
    version = "v1alpha1",
    kind = "MirroredVolume",
    namespaced,
    status = "MirroredVolumeStatus",
    printcolumn = r#"{"name":"Phase", "type":"string", "jsonPath":".status.operation.phase"}"#,
    printcolumn = r#"{"name":"Active", "type":"string", "jsonPath":".status.active.node"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct MirroredVolumeSpec {
    /// The single-replica workload whose named PVC-backed volume is managed.
    pub workload: WorkloadRef,

    /// The local-storage claims used for the candidate replicas.
    pub storage: StorageSpec,

    /// Nodes eligible to hold a replica.
    pub candidate_nodes: Vec<String>,

    /// Human-authored desired writer. The controller snapshots this into an
    /// immutable operation before taking any disruptive action.
    pub desired_active_node: String,

    /// How long an operation may run before the controller gives up and
    /// moves the volume to `Degraded`.
    #[serde(default = "default_operation_timeout_seconds")]
    pub operation_timeout_seconds: u64,

    /// Which backend gates this volume's promotion — `mirrorvol-agent`
    /// reconciles against it, and admission requires every candidate
    /// node to have a `BackendNode` advertising it.
    #[serde(default = "default_backend")]
    pub backend: String,

    /// `bestEffort` (default) or `strict`.
    /// Immutable after creation. Delete and recreate the `MirroredVolume`
    /// to change it.
    #[serde(default)]
    #[schemars(schema_with = "consistency_schema")]
    pub consistency: Consistency,
}

fn default_operation_timeout_seconds() -> u64 {
    900
}

fn default_backend() -> String {
    backend::SYNCTHING.to_owned()
}

/// [`MirroredVolumeSpec::consistency`] — the one place this system's two
/// consistency modes are defined; every other crate matches on this type
/// directly instead of re-deriving its own notion of "which mode" from a
/// string comparison. `consistency_schema` (this crate's private schemars
/// hook) gives the CRD the matching
/// `enum: [bestEffort, strict]` restriction, so an API-server-admitted
/// object can never carry anything this type can't represent — there's no
/// "unrecognized value" case left to fall back on.
///
/// [`Consistency::as_str`]/[`Display`](std::fmt::Display) are for the one
/// place this needs to cross a non-Kubernetes-typed boundary: `mirrorvol-agent`
/// writes the resolved mode to a plain file `mirrorvol-csi` reads back,
/// since `mirrorvol-csi` has no Kubernetes API access of its own to read
/// `spec.consistency` directly.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum Consistency {
    #[default]
    BestEffort,
    Strict,
}

impl Consistency {
    pub fn as_str(self) -> &'static str {
        match self {
            Consistency::BestEffort => "bestEffort",
            Consistency::Strict => "strict",
        }
    }
}

impl std::fmt::Display for Consistency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// [`MirroredVolumeSpec::backend`] / [`BackendNodeSpec::backend`] values —
/// kept as constants for the same reason [`phase`]/[`role`] are: both sides
/// match on the same literal instead of hand-typing `"syncthing"`.
pub mod backend {
    pub const SYNCTHING: &str = "syncthing";
    pub const RSYNC: &str = "rsync";
}

/// Naming/annotation/label conventions shared across crates so they can't
/// drift independently.
pub mod naming {
    /// The deterministic candidate replica PVC name for one node.
    pub fn replica_claim_name(volume_id: &str, node: &str) -> String {
        format!("{volume_id}-{node}")
    }

    /// Recovers `volume_id` from a name minted by [`replica_claim_name`],
    /// given the exact `node` it was minted for (not generic delimiter
    /// parsing — both parts can contain `-`, so `node` must already be
    /// known, e.g. from [`env::NODE_NAME`]). `None` if `name` wasn't minted
    /// for it.
    pub fn strip_replica_claim_suffix<'a>(name: &'a str, node: &str) -> Option<&'a str> {
        name.strip_suffix(&format!("-{node}"))
    }

    /// `PersistentVolume.spec.csi.volumeAttributes` key `mirrorvol-csi`
    /// writes the real underlying storage path to (when
    /// `mirrorvol.io/underlyingPathTemplate` resolves one), which
    /// `mirrorvol-agent` reads to override
    /// [`StorageSpec::replica_path_template`](crate::StorageSpec::replica_path_template).
    pub const RESOLVED_PATH_ATTRIBUTE: &str = "mirrorvol.io/resolvedPath";

    /// Operator-applied annotation (`kubectl annotate`) that resets a
    /// `Degraded` [`MirroredVolume`](crate::MirroredVolume)'s
    /// [`active`](crate::MirroredVolumeStatus::active)/[`operation`](crate::MirroredVolumeStatus::operation)/[`grant`](crate::MirroredVolumeStatus::grant)
    /// to `None`, after the operator has externally confirmed the old
    /// writer can never run again. Must equal the exact stuck
    /// `status.operation.id` — per-operation and single-use, not a standing
    /// switch (operation ids only increase, so a stale value can never
    /// match again). Consumed: `mirrorvol-controller` removes both this and
    /// [`RECOVERY_REASON_ANNOTATION`] and records a `Recovered` condition
    /// instead. Doesn't re-promote anything itself — the reset status
    /// re-enters `decide()`'s ordinary "no source" bootstrap path.
    pub const RECOVER_DEGRADED_OPERATION_ANNOTATION: &str = "mirrorvol.io/recoverDegradedOperation";

    /// Optional free-text reason accompanying
    /// [`RECOVER_DEGRADED_OPERATION_ANNOTATION`] or
    /// [`REENROLL_NODE_IDENTITY_ANNOTATION`], carried into the resulting
    /// condition's message for the audit trail.
    pub const RECOVERY_REASON_ANNOTATION: &str = "mirrorvol.io/recoveryReason";

    /// Operator-applied annotation that re-baselines one candidate node's
    /// recorded identity generation (`status.nodeIdentityGenerations[node]`)
    /// to its currently-observed value — the idle-state equivalent of
    /// [`RECOVER_DEGRADED_OPERATION_ANNOTATION`]: identity changes are
    /// checked in both states, but an idle contradiction has no `operation`
    /// object to force into `Degraded`. Value must be exactly
    /// `<node>@<generation>` (see [`parse_reenroll_node_identity`]), matching
    /// the node's *current* observed
    /// [`BackendNodeStatus::identity_generation`](crate::BackendNodeStatus::identity_generation)
    /// exactly. Consumed: `mirrorvol-controller` removes both this and
    /// [`RECOVERY_REASON_ANNOTATION`] and records an `IdentityReenrolled` condition.
    pub const REENROLL_NODE_IDENTITY_ANNOTATION: &str = "mirrorvol.io/reenrollNodeIdentity";

    /// Parses [`REENROLL_NODE_IDENTITY_ANNOTATION`]'s `<node>@<generation>`
    /// value. [`rsplit_once`](str::rsplit_once): a node name is free-form
    /// and could itself contain `@`, but the generation suffix never does,
    /// so splitting from the right is unambiguous.
    pub fn parse_reenroll_node_identity(value: &str) -> Option<(&str, u64)> {
        let (node, generation) = value.rsplit_once('@')?;
        if node.is_empty() {
            return None;
        }
        let generation = generation.parse::<u64>().ok()?;
        Some((node, generation))
    }

    /// File name (inside [`env::CONSISTENCY_DIR`]) `mirrorvol-agent` writes
    /// a volume's resolved active-peer address to, `rsync`-only — read
    /// back by `mirrorvol-csi`'s `attach_rsync`, which has no Kubernetes
    /// API access of its own.
    pub fn active_peer_signal_file_name(volume_id: &str) -> String {
        format!("{volume_id}.active-peer")
    }

    /// File name (inside [`env::CONSISTENCY_DIR`]) `mirrorvol-agent` writes
    /// this node's own controller-computed
    /// [`writer_agreement`](crate::writer_agreement) verdict to (`strict`
    /// only — `bestEffort` has nothing per-node to relay, see
    /// `MirroredVolumeStatus::writer_agreement`) — read back by
    /// `mirrorvol-csi`'s attach hook as a supplementary veto alongside its
    /// own live local check, since it has no Kubernetes API access of its
    /// own to read `status.nodeWriterAgreement` directly.
    pub fn writer_agreement_signal_file_name(volume_id: &str) -> String {
        format!("{volume_id}.writer-agreement")
    }

    /// Per-node Syncthing API key `Secret` name — get-or-created by that
    /// node's agent, same self-ownership shape as
    /// [`BackendNode`](crate::BackendNode).
    pub fn per_node_secret_name(node: &str) -> String {
        format!("mirrorvol-syncthing-apikey-{node}")
    }

    /// The rsync module password `Secret`'s name — **not** per-node,
    /// unlike [`per_node_secret_name`]: a puller needs the *source*
    /// node's secret, not its own, so every candidate shares one value.
    /// Owned by [`CONTROLLER_DEPLOYMENT_NAME`] (install-wide scope), not by
    /// any one `Node`: deleting an unrelated node must never delete a
    /// secret every other candidate still depends on.
    pub const RSYNC_SHARED_SECRET_NAME: &str = "mirrorvol-rsync-secret";

    /// Per-node `rsync` device identity token `Secret` name — **unlike**
    /// [`RSYNC_SHARED_SECRET_NAME`] (the shared module *auth* password),
    /// this is per-node and owned by that node's `Node`, mirroring
    /// [`per_node_secret_name`]'s shape exactly. Deliberately a separate
    /// `Secret` from both: "are you allowed to connect" (shared password)
    /// and "which device is this" (this token, backing
    /// `BackendNodeStatus::identity_generation`'s contradiction detection)
    /// are different concerns that used to be conflated by reusing the
    /// node name as `rsync`'s `device_id`.
    pub fn per_node_rsync_identity_secret_name(node: &str) -> String {
        format!("mirrorvol-rsync-identity-{node}")
    }

    /// The `mirrorvol-controller` `Deployment`'s well-known name — the
    /// owner for objects whose scope is "this operator installation exists"
    /// (currently just [`RSYNC_SHARED_SECRET_NAME`]), as opposed to
    /// per-node objects owned by a `Node`. Must match
    /// `deploy/controller/deployment.yaml`'s `metadata.name`. Hardcoded,
    /// same convention as [`SYNCTHING_POD_APP_NAME`]/
    /// [`RSYNC_SHARED_SECRET_NAME`] themselves.
    pub const CONTROLLER_DEPLOYMENT_NAME: &str = "mirrorvol-controller";

    /// Per-node `Service` name fronting one node's Syncthing sync port,
    /// plus the `stunnel` sidecar's TLS port when that node runs `rsync`.
    /// One per-node Service shared by every backend, not one per backend.
    /// Get-or-created by that node's agent at startup, selecting on
    /// [`NODE_LABEL`].
    pub fn per_node_service_name(node: &str) -> String {
        format!("mirrorvol-syncthing-{node}")
    }

    /// Label a node's own agent patches onto its own Pod at startup — the
    /// one thing a DaemonSet's shared pod template can't set per-instance —
    /// and what [`per_node_service_name`]'s `Service` selects on.
    pub const NODE_LABEL: &str = "mirrorvol.io/node";

    /// Label + annotation `mirrorvol-controller` stamps onto each replica
    /// PVC it creates, naming the owning [`MirroredVolume`](crate::MirroredVolume)
    /// by id. Both a label (for `kubectl get pvc -l`/selector use) and an
    /// annotation (for `kubectl describe`/observability) on the same
    /// object.
    pub const VOLUME_LABEL: &str = "mirrorvol.io/volume";

    /// Label `mirrorvol-controller` stamps onto each replica PVC naming the
    /// candidate node it was created for.
    pub const CANDIDATE_NODE_LABEL: &str = "mirrorvol.io/candidate-node";

    /// Label selecting a `MirroredVolume`'s candidate nodes cluster-wide.
    pub const CANDIDATE_LABEL: &str = "mirrorvol.io/candidate";

    /// `app.kubernetes.io/name` label value every `mirrorvol-syncthing`
    /// DaemonSet Pod carries; What [`per_node_service_name`]'s
    /// `Service` selects on (`ensure_node_label_and_service` in
    /// `mirrorvol-agent`), and what test code queries by to find a
    /// specific node's Syncthing Pod.
    pub const SYNCTHING_POD_APP_NAME: &str = "mirrorvol-syncthing";

    /// Env var *names* read by more than one binary
    pub mod env {
        /// This node's Kubernetes `Node` name. No default — required.
        /// Read by `mirrorvol-agent` (`run` and `provision`) and
        /// `mirrorvol-csi`.
        pub const NODE_NAME: &str = "NODE_NAME";

        /// This Pod's namespace. Read by `mirrorvol-agent` (`run` and
        /// `provision`).
        pub const POD_NAMESPACE: &str = "POD_NAMESPACE";
        pub const POD_NAMESPACE_DEFAULT: &str = "mirrorvol";

        /// Directory `mirrorvol-agent` writes each volume's resolved
        /// `spec.consistency` to (one file per volume ID), and
        /// `mirrorvol-csi`'s attach hook reads back.
        pub const CONSISTENCY_DIR: &str = "MIRRORVOL_CONSISTENCY_DIR";
        pub const CONSISTENCY_DIR_DEFAULT: &str = "/shared-consistency";
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn strip_recovers_exactly_what_replica_claim_name_minted() {
            let name = replica_claim_name("my-app", "worker-1");
            assert_eq!(
                strip_replica_claim_suffix(&name, "worker-1"),
                Some("my-app")
            );
        }

        #[test]
        fn strip_is_only_correct_because_the_caller_supplies_its_own_true_node() {
            let name = replica_claim_name("my-app", "worker-1");
            assert_eq!(
                strip_replica_claim_suffix(&name, "app-worker-1"),
                Some("my")
            );
        }

        #[test]
        fn strip_rejects_a_name_minted_for_a_different_node() {
            let name = replica_claim_name("my-app", "worker-1");
            assert_eq!(strip_replica_claim_suffix(&name, "worker-2"), None);
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadRef {
    pub kind: String,
    pub name: String,
    pub volume_name: String,
    pub mount_path: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StorageSpec {
    pub storage_class_name: String,
    /// Local-provisioner adapter path with `{claim}` substituted by the
    /// deterministic candidate claim name.
    pub replica_path_template: String,
    #[schemars(schema_with = "opaque_json_schema")]
    pub claim_template: serde_json::Value,

    /// Gitignore-style patterns applied identically on every candidate. Files
    /// these patterns match should never be synced in either direction.
    #[serde(default)]
    pub ignore_patterns: Vec<String>,

    /// `backend: rsync` only: how often a standby candidate's periodic warm
    /// sync pulls from the active node, keeping it warm ahead of any move.
    /// Independent of the gated final resync a `strict` promotion runs at
    /// `AwaitingRelease`/before `acquire_writer` (see
    /// `mirrorvol-backend::rsync`'s `completion`), which always runs a
    /// fresh full pass regardless of this interval. Ignored by every other
    /// backend. No volume has been measured yet, so this default (like
    /// `operationTimeoutSeconds`'s) is a starting guess, not a tuned value.
    #[serde(default = "default_warm_sync_interval_seconds")]
    pub warm_sync_interval_seconds: u32,

    /// `bestEffort` only: how long `status.writerAgreement` may stay
    /// `Pending` (more than one candidate currently write-capable) before
    /// the controller surfaces a `Degraded`/`MultipleWriters` condition.
    /// Informational only — never gates, same as `SyncConflict`. `strict`
    /// reuses `operationTimeoutSeconds` for the analogous bound instead
    /// (its own `Pending` only ever exists inside a live, deadline-bound
    /// operation), so this field is ignored there. Bounded by how long a
    /// candidate's *initial* full sync can legitimately take, which is
    /// unmeasured and data-size-dependent — same starting-guess caveat as
    /// `operationTimeoutSeconds`/`warmSyncIntervalSeconds`.
    #[serde(default = "default_writer_agreement_timeout_seconds")]
    pub writer_agreement_timeout_seconds: u32,
}

fn default_warm_sync_interval_seconds() -> u32 {
    300
}

fn default_writer_agreement_timeout_seconds() -> u32 {
    1800
}

pub mod phase {
    pub const DRAINING: &str = "Draining";
    pub const AWAITING_RELEASE: &str = "AwaitingRelease";
    pub const ENFORCING_STANDBY: &str = "EnforcingStandby";
    pub const GRANTING: &str = "Granting";
    pub const DEGRADED: &str = "Degraded";
}

/// `status.nodes[node].role` values — self-reported by that node's own
/// agent. Also kept as a `String` on the wire for the same reason [`phase`]
/// is; these constants exist so both the controller and the agent match on
/// the same literals instead of hand-typing `"Writer"`/`"Standby"`.
pub mod role {
    pub const WRITER: &str = "Writer";
    pub const STANDBY: &str = "Standby";
}

/// [`WriterAgreementEntry::state`] values — computed entirely by
/// `mirrorvol-controller` (never the agent) from data agents already
/// report (`NodeSyncStatus.role`/`lock_epoch` for `strict`; a count of
/// `role == Writer` across candidates for `bestEffort`). Kept as a plain
/// `String` on the wire for the same reason [`phase`]/[`role`] are.
///
/// - `CONVERGED`: role and lock agree with what's expected right now (or,
///   for a standby, there's nothing to disagree about).
/// - `PENDING`: role and lock disagree, but something currently authorizes
///   this node to be converging toward `Writer` — expected and
///   self-healing. `strict` bounds this with the operation's own
///   `deadline`; `bestEffort` bounds it with
///   `StorageSpec::writer_agreement_timeout_seconds`.
/// - `CONTRADICTION`: `strict` only — role and lock disagree with nothing
///   authorizing it. Never expected to resolve on its own, so it's never
///   time-gated the way `PENDING` is.
pub mod writer_agreement {
    pub const CONVERGED: &str = "Converged";
    pub const PENDING: &str = "Pending";
    pub const CONTRADICTION: &str = "Contradiction";
}

// See MirroredVolumeSpec's own comment above — no rustdoc [link] syntax on
// this struct's field docs either, for the same reason.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct NodeSyncStatus {
    /// The locally observed backend role — one of `role::WRITER`/
    /// `role::STANDBY`.
    pub role: String,

    /// The writer epoch when `role == Writer`.
    #[serde(default)]
    pub epoch: Option<u64>,

    /// Local replica completion, never a cached remote completion result.
    pub ready: bool,

    /// Local view of the synchronized writer lock.
    pub lock_absent: bool,

    /// Epoch read from a present lock.
    #[serde(default)]
    pub lock_epoch: Option<u64>,

    /// This source node has proved workload and mount teardown.
    #[serde(default)]
    pub quiesced_operation: Option<u64>,

    /// This source node removed its lock for the operation.
    #[serde(default)]
    pub released_operation: Option<u64>,

    /// This target saw the operation's release in its own local replica.
    #[serde(default)]
    pub release_observed_operation: Option<u64>,

    /// Writer Pod UIDs recently observed locally. Used to prove that a
    /// force-deleted writer process is no longer running before lock release.
    #[serde(default)]
    pub writer_pod_uids: Vec<String>,

    /// `bestEffort` only: unresolved `*.sync-conflict-*` paths this node's
    /// backend currently reports for its own replica. The controller
    /// aggregates these across candidates into a `Degraded`/`SyncConflict`
    /// status condition — informational only, never a gate.
    #[serde(default)]
    pub conflict_files: Vec<String>,

    #[serde(default)]
    #[schemars(schema_with = "opaque_json_schema")]
    pub detail: serde_json::Value,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActiveWriter {
    pub node: String,
    pub epoch: u64,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PromotionOperation {
    pub id: u64,
    pub source: Option<String>,
    pub target: String,
    pub epoch: u64,
    pub phase: String,
    pub started_at: String,
    pub deadline: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PromotionGrant {
    pub operation_id: u64,
    pub target: String,
    pub epoch: u64,
}

/// One [`writer_agreement`] verdict plus when it last actually changed —
/// `changed_at` is only overwritten when `state` changes, the same
/// "only bump on a genuine transition" discipline `Condition`'s own
/// `lastTransitionTime` already follows in this codebase. Entirely
/// controller-owned; agents never write this.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WriterAgreementEntry {
    /// One of [`writer_agreement`]'s constants.
    pub state: String,
    pub changed_at: Time,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct MirroredVolumeStatus {
    #[serde(default)]
    pub active: Option<ActiveWriter>,

    #[serde(default)]
    pub operation: Option<PromotionOperation>,

    #[serde(default)]
    pub grant: Option<PromotionGrant>,

    #[serde(default)]
    pub nodes: BTreeMap<String, NodeSyncStatus>,

    #[serde(default)]
    pub conditions: Vec<Condition>,

    /// Identity generation the controller has last acknowledged per
    /// candidate node. A mismatch against `BackendNode.status.identityGeneration`
    /// is what `mirrorvol_controller::decide` treats as a contradiction.
    /// Set on first sighting of a node; never auto-updated after that.
    #[serde(default)]
    pub node_identity_generations: BTreeMap<String, u64>,

    /// `strict` only: per-candidate [`writer_agreement`] verdict, computed
    /// by the controller from that candidate's own `nodes[node]` entry.
    /// Empty for `bestEffort` — see [`writer_agreement`] (the field below)
    /// for its volume-wide equivalent there.
    #[serde(default)]
    pub node_writer_agreement: BTreeMap<String, WriterAgreementEntry>,

    /// `bestEffort` only: the volume-wide [`writer_agreement`] verdict —
    /// `Converged` when at most one candidate currently reports itself
    /// writer, `Pending` when more than one does. `None` for `strict`,
    /// which tracks this per-candidate in `node_writer_agreement` instead.
    #[serde(default)]
    pub writer_agreement: Option<WriterAgreementEntry>,
}

/// One per (node, backend type): a node can run more than one backend for
/// the same volume. Created and owned entirely by that node's agent on boot.
/// Read-only to the controller and every other consumer.
// Field doc comments on this struct also feed the CRD schema — see
// `MirroredVolumeSpec`'s comment above.
#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, PartialEq, JsonSchema)]
// group/version must equal API_GROUP/API_VERSION — see that constant's own
// doc comment for why this can't be wired in directly.
#[kube(
    group = "homelab.internal",
    version = "v1alpha1",
    kind = "BackendNode",
    namespaced,
    status = "BackendNodeStatus",
    printcolumn = r#"{"name":"Node", "type":"string", "jsonPath":".spec.node"}"#,
    printcolumn = r#"{"name":"Backend", "type":"string", "jsonPath":".spec.backend"}"#,
    printcolumn = r#"{"name":"Healthy", "type":"boolean", "jsonPath":".status.healthy"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct BackendNodeSpec {
    pub node: String,
    /// Only `backend::SYNCTHING` (`"syncthing"`) for now.
    pub backend: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct BackendNodeStatus {
    #[serde(default)]
    pub healthy: bool,

    /// Stable local backend identity, e.g. a Syncthing device ID.
    #[serde(default)]
    pub device_id: Option<String>,

    /// Changes only after explicit backend identity re-enrollment.
    #[serde(default)]
    pub identity_generation: u64,

    /// Which device-cert path this node's agent is running:
    /// `"cert-manager"` (attested, not implemented yet) or `"self-managed"`
    /// (today's TOFU cert — the only value currently written).
    #[serde(default)]
    pub device_cert_source: Option<String>,

    #[serde(default)]
    #[schemars(schema_with = "opaque_json_schema")]
    pub detail: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::Resource;

    /// The safety net [`API_GROUP`]/[`API_VERSION`]'s own doc comments
    /// promise: [`MirroredVolume`]'s `#[kube(group = ..., version = ...)]`
    /// attribute can't reference those constants directly, so this is what
    /// actually catches the two drifting apart.
    #[test]
    fn mirrored_volume_group_version_match_the_root_constants() {
        assert_eq!(MirroredVolume::group(&()), API_GROUP);
        assert_eq!(MirroredVolume::version(&()), API_VERSION);
    }

    /// Same as [`mirrored_volume_group_version_match_the_root_constants`],
    /// for [`BackendNode`].
    #[test]
    fn backend_node_group_version_match_the_root_constants() {
        assert_eq!(BackendNode::group(&()), API_GROUP);
        assert_eq!(BackendNode::version(&()), API_VERSION);
    }

    #[test]
    fn promotion_status_round_trips_through_json() {
        let status = MirroredVolumeStatus {
            active: Some(ActiveWriter {
                node: "node-a".to_owned(),
                epoch: 17,
            }),
            operation: Some(PromotionOperation {
                id: 18,
                source: Some("node-a".to_owned()),
                target: "node-b".to_owned(),
                epoch: 18,
                phase: phase::AWAITING_RELEASE.to_owned(),
                started_at: "1".to_owned(),
                deadline: "2".to_owned(),
            }),
            grant: None,
            nodes: BTreeMap::from([(
                "node-b".to_owned(),
                NodeSyncStatus {
                    role: role::STANDBY.to_owned(),
                    ready: true,
                    lock_absent: true,
                    ..Default::default()
                },
            )]),
            conditions: vec![],
            node_identity_generations: BTreeMap::from([("node-a".to_owned(), 2)]),
            node_writer_agreement: BTreeMap::from([(
                "node-a".to_owned(),
                WriterAgreementEntry {
                    state: writer_agreement::CONVERGED.to_owned(),
                    // Whole seconds only — Time's wire format is
                    // second-precision RFC3339, so a sub-second value
                    // wouldn't round-trip through JSON unchanged.
                    changed_at: Time(
                        chrono::DateTime::from_timestamp(1_700_000_000, 0)
                            .expect("valid timestamp"),
                    ),
                },
            )]),
            writer_agreement: None,
        };
        assert_eq!(
            status,
            serde_json::from_str::<MirroredVolumeStatus>(
                &serde_json::to_string(&status).expect("serialize"),
            )
            .expect("deserialize"),
        );
    }

    #[test]
    fn backend_identity_round_trips_through_json() {
        let status = BackendNodeStatus {
            healthy: true,
            device_id: Some("DEVICE".to_owned()),
            identity_generation: 1,
            device_cert_source: Some("self-managed".to_owned()),
            detail: serde_json::json!({ "version": "1.27.3" }),
        };
        assert_eq!(
            status,
            serde_json::from_str::<BackendNodeStatus>(
                &serde_json::to_string(&status).expect("serialize"),
            )
            .expect("deserialize"),
        );
    }
}
