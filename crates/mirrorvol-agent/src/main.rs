mod provision;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use futures::StreamExt;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{Node as K8sNode, PersistentVolume, PersistentVolumeClaim, Pod};
use kube::api::{Api, ListParams, Patch, PatchParams};
use kube::core::ObjectList;
use kube::runtime::controller::{Action, Controller};
use kube::{Client, Resource, ResourceExt};
use mirrorvol_agent::{reconcile_node, NodeReader, NodeReconcileOutcome};
use mirrorvol_api::{naming, BackendNode, MirroredVolume, MirroredVolumeStatus};
use mirrorvol_backend::rsync::{self, RsyncBackend, SystemRsyncRunner};
use mirrorvol_backend::syncthing::{
    fetch_device_id, ConflictDetection, LocalEndpoint, SyncthingBackend,
};
use mirrorvol_backend::{LocalBackend, ReplicaConfig};

/// One agent process, one node, potentially several backends. Keyed by
/// [`mirrorvol_api::backend`]'s constants. `Arc` because `rsync`'s warm-sync
/// background task needs to outlive any single reconcile call; every other
/// backend just ignores the extra refcount.
type BackendRegistry = HashMap<&'static str, Arc<dyn LocalBackend>>;

/// Field manager name for every server-side-apply patch this binary makes.
const AGENT_NAME: &str = "mirrorvol-agent";

#[derive(Parser)]
#[command(name = AGENT_NAME)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Long-running per-node reconcile loop — the default when no
    /// subcommand is given, so the existing DaemonSet convention of
    /// exec'ing this binary with no arguments keeps working unchanged.
    Run,
    /// One-shot initContainer step: get-or-creates this node's
    /// Syncthing API key and writes it to a shared file.
    Provision,
}

struct Ctx {
    node: String,
    namespace: String,
    /// Downward-API identity of this agent's Pod, when available. Used
    /// by [`ensure_node_label_and_service`] to patch this Pod's label.
    pod_name: Option<String>,
    client: Client,
    /// Every backend this node's agent process has configured, keyed by
    /// `mirrorvol_api::backend`'s constants — one process, potentially
    /// several backends, rather than one process per `(node, backend)`,
    /// which would multiply this node's cluster-wide `MirroredVolume` watch
    /// by its backend count. Starting a backend's own background work
    /// (`rsync`'s warm-sync task) no longer needs a second, concrete-typed
    /// field alongside this one — see
    /// [`LocalBackend::start_background_tasks`].
    backends: BackendRegistry,
    /// Shared `emptyDir` directory this agent writes each volume's resolved
    /// consistency mode (and, `rsync`-only, active-peer address) to.
    /// `mirrorvol-csi`'s attach hook reads it back since it has no
    /// Kubernetes API access of its own to read `MirroredVolume` directly.
    consistency_dir: String,
}

fn backend_node_name(node: &str, backend: &str) -> String {
    format!("{node}-{backend}")
}

/// This node's `Node` object UID, for building an ownerReference that
/// survives ordinary Pod restarts — unlike owning by the Pod itself, which
/// the garbage collector deletes the owned object for on every restart,
/// destroying the identity-change/API-key history that depends on it.
/// Owning by the Node instead only GCs once the node leaves the cluster.
pub(crate) async fn node_uid(client: &Client, node: &str) -> anyhow::Result<Option<String>> {
    Ok(Api::<K8sNode>::all(client.clone())
        .get(node)
        .await?
        .metadata
        .uid)
}

/// This install's `mirrorvol-controller` `Deployment` UID, for owning
/// objects scoped to "this operator installation exists" rather than any
/// one node — currently just [`naming::RSYNC_SHARED_SECRET_NAME`]. `Ok(None)`
/// (not an error) if the Deployment isn't found yet — fail-open, same shape
/// as [`node_uid`]: a `provision` run racing ahead of the controller's own
/// rollout still succeeds, just without an owner reference until a later
/// reconcile.
pub(crate) async fn controller_deployment_uid(
    client: &Client,
    namespace: &str,
) -> anyhow::Result<Option<String>> {
    Ok(Api::<Deployment>::namespaced(client.clone(), namespace)
        .get_opt(naming::CONTROLLER_DEPLOYMENT_NAME)
        .await?
        .and_then(|deployment| deployment.metadata.uid))
}

/// Builds a single-entry ownerReference list from a UID, or an empty one
/// (no owner) when the lookup missed — [`node_uid`]/[`controller_deployment_uid`]
/// both fail open rather than blocking provisioning on the owner existing
/// yet.
pub(crate) fn owner_references(
    api_version: &str,
    kind: &str,
    name: &str,
    uid: Option<String>,
) -> Vec<k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference> {
    match uid {
        Some(uid) => vec![
            k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
                api_version: api_version.to_owned(),
                kind: kind.to_owned(),
                name: name.to_owned(),
                uid,
                controller: Some(true),
                block_owner_deletion: Some(true),
            },
        ],
        None => Vec::new(),
    }
}

async fn register_backend_node(ctx: &Ctx, backend: &str, device_id: &str) -> anyhow::Result<()> {
    let api = Api::<BackendNode>::namespaced(ctx.client.clone(), &ctx.namespace);
    let name = backend_node_name(&ctx.node, backend);
    // Read whatever this node's own BackendNode already carries, before
    // overwriting it below — `next_identity_generation` needs the
    // previously-registered `device_id` to tell a genuine identity change
    // apart from a routine reconcile re-registering the same one.
    let previous = api
        .get_opt(&name)
        .await?
        .and_then(|existing| existing.status)
        .map(|status| (status.device_id, status.identity_generation));
    let identity_generation = mirrorvol_agent::next_identity_generation(
        previous
            .as_ref()
            .and_then(|(device_id, generation)| Some((device_id.as_deref()?, *generation))),
        device_id,
    );
    let mut metadata = serde_json::json!({ "name": name });
    if let Some(uid) = node_uid(&ctx.client, &ctx.node).await? {
        metadata["ownerReferences"] = serde_json::json!([{
            "apiVersion": "v1",
            "kind": "Node",
            "name": ctx.node,
            "uid": uid,
            "controller": true,
            "blockOwnerDeletion": true,
        }]);
    }
    api.patch(
        &name,
        &PatchParams::apply(AGENT_NAME),
        &Patch::Apply(serde_json::json!({
            "apiVersion": BackendNode::api_version(&()),
            "kind": BackendNode::kind(&()),
            "metadata": metadata,
            "spec": { "node": ctx.node, "backend": backend }
        })),
    )
    .await?;
    api.patch_status(
        &name,
        &PatchParams::apply(AGENT_NAME),
        &Patch::Merge(serde_json::json!({ "status": {
            "healthy": true,
            // Must exactly match BackendNodeStatus's camelCase field
            // (`deviceId`)
            "deviceId": device_id,
            "identityGeneration": identity_generation,
            // TODO: update when we have cert-manager support
            // Always "self-managed" until the cert-manager branch exists.
            "deviceCertSource": "self-managed"
        }})),
    )
    .await?;
    Ok(())
}

/// The real, cluster-backed [`NodeReader`] `main.rs` wires into
/// [`reconcile_node`] — everything below is the same I/O this crate always
/// did, just grouped behind that trait instead of called directly from
/// `reconcile()`. A separate, smaller struct than [`Ctx`] — these three
/// reads only ever need `client`/`namespace`/`node`, none of `Ctx`'s
/// backend registry or consistency-signal state.
struct KubeNodeReader {
    client: Client,
    namespace: String,
    node: String,
}

#[async_trait::async_trait]
impl NodeReader for KubeNodeReader {
    async fn replica_config(&self, mv: &MirroredVolume) -> Result<ReplicaConfig, String> {
        replica_config(self, mv)
            .await
            .map_err(|error| error.to_string())
    }

    async fn local_writer_pod_uids(&self, mv: &MirroredVolume) -> Result<Vec<String>, String> {
        local_writer_pod_uids(self, mv)
            .await
            .map_err(|error| error.to_string())
    }

    async fn source_quiesced(
        &self,
        status: &MirroredVolumeStatus,
        current_pods: &[String],
    ) -> bool {
        source_quiesced(self, status, current_pods).await
    }
}

async fn replica_config(
    ctx: &KubeNodeReader,
    mv: &MirroredVolume,
) -> anyhow::Result<ReplicaConfig> {
    let nodes = Api::<BackendNode>::namespaced(ctx.client.clone(), &ctx.namespace)
        .list(&Default::default())
        .await?;
    let mut peers = Vec::new();
    let mut peer_addresses = std::collections::BTreeMap::new();
    for backend_node in nodes.items {
        if backend_node.spec.backend != mv.spec.backend
            || backend_node.spec.node == ctx.node
            || !mv.spec.candidate_nodes.contains(&backend_node.spec.node)
        {
            continue;
        }
        // The candidate device set is fixed by identity, not momentary
        // health. Health gates promotion elsewhere; it doesn't gate folder
        // membership. Only meaningful for a peer-to-peer backend
        // (Syncthing) — a pull-based backend (rsync) has no device-ID
        // concept and resolves who to talk to via `active_peer_address`
        // instead, computed below.
        if let Some(device_id) = backend_node.status.and_then(|status| status.device_id) {
            // Each candidate's agent fronts its sync port with a per-node
            // Service via `ensure_node_label_and_service`.
            peer_addresses.insert(
                device_id.clone(),
                format!(
                    "tcp://{}.{}.svc:22000",
                    naming::per_node_service_name(&backend_node.spec.node),
                    ctx.namespace
                ),
            );
            peers.push(device_id);
        }
    }
    peers.sort();

    let claim_name = mirrorvol_api::naming::replica_claim_name(&mv.name_any(), &ctx.node);
    let local_path = match resolved_underlying_path(ctx, &claim_name).await {
        Ok(Some(path)) => path,
        // No resolved path yet (no `underlyingPathTemplate`, a block-based
        // driver, or CreateVolume hasn't landed): fall back unchanged.
        Ok(None) => mv
            .spec
            .storage
            .replica_path_template
            .replace("{claim}", &claim_name),
        Err(error) => {
            tracing::warn!(volume = %mv.name_any(), node = %ctx.node, %error, "resolved replica path unavailable, falling back to replicaPathTemplate");
            mv.spec
                .storage
                .replica_path_template
                .replace("{claim}", &claim_name)
        }
    };

    // Combines each peer's address and local_path, not just device IDs —
    // `ensure_replica` skips its PATCH when generation is unchanged, so
    // omitting either would leave a later address/path change stuck.
    let peer_bytes = peers.iter().flat_map(|device_id| {
        let address = peer_addresses.get(device_id).map_or("", String::as_str);
        device_id
            .bytes()
            .chain(std::iter::once(b'\0'))
            .chain(address.bytes())
            .chain(std::iter::once(b'\0'))
    });
    let generation = peer_bytes
        .chain(local_path.bytes())
        .fold(0_u64, |hash, byte| {
            hash.wrapping_mul(1_099_511_628_211) ^ u64::from(byte)
        });
    // rsync-only; resolved fresh every call, not folded into `generation`.
    let active_peer_address = mv.status.as_ref().and_then(|status| {
        let active_node = status.active.as_ref()?.node.as_str();
        (active_node != ctx.node).then(|| {
            format!(
                "{}.{}.svc",
                naming::per_node_service_name(active_node),
                ctx.namespace
            )
        })
    });

    Ok(ReplicaConfig {
        volume_id: mv.name_any(),
        local_path,
        peer_device_ids: peers,
        peer_addresses,
        generation,
        ignore_patterns: mv.spec.storage.ignore_patterns.clone(),
        active_peer_address,
    })
}

/// Reads this candidate's replica PVC's bound `PersistentVolume` and
/// returns `mirrorvol_api::naming::RESOLVED_PATH_ATTRIBUTE` from its
/// `spec.csi.volumeAttributes`, if present. `Ok(None)` covers every
/// "not applicable yet" case uniformly; only a real API error
/// is `Err`.
async fn resolved_underlying_path(
    ctx: &KubeNodeReader,
    claim_name: &str,
) -> anyhow::Result<Option<String>> {
    let claims = Api::<PersistentVolumeClaim>::namespaced(ctx.client.clone(), &ctx.namespace);
    let Some(claim) = claims.get_opt(claim_name).await? else {
        return Ok(None);
    };
    let Some(pv_name) = claim.spec.and_then(|spec| spec.volume_name) else {
        return Ok(None);
    };
    let volumes = Api::<PersistentVolume>::all(ctx.client.clone());
    let Some(volume) = volumes.get_opt(&pv_name).await? else {
        return Ok(None);
    };
    Ok(volume
        .spec
        .and_then(|spec| spec.csi)
        .and_then(|csi| csi.volume_attributes)
        .and_then(|attributes| {
            attributes
                .get(mirrorvol_api::naming::RESOLVED_PATH_ATTRIBUTE)
                .cloned()
        }))
}

async fn local_writer_pod_uids(
    ctx: &KubeNodeReader,
    mv: &MirroredVolume,
) -> anyhow::Result<Vec<String>> {
    let deployment = Api::<Deployment>::namespaced(ctx.client.clone(), &ctx.namespace)
        .get(&mv.spec.workload.name)
        .await?;
    // An empty selector here isn't "match nothing that needs filtering", it's
    // "match every Pod in the namespace" once passed to `.labels()` so
    // Deployment without `matchLabels` (or with none set) does not silently
    // widen this into an unfiltered namespace-wide Pod list.
    let labels = deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.selector.match_labels.as_ref())
        .filter(|labels| !labels.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Deployment {} has no spec.selector.matchLabels to scope writer pod lookup by",
                mv.spec.workload.name
            )
        })?;
    let selector = labels
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",");
    let pods = Api::<Pod>::namespaced(ctx.client.clone(), &ctx.namespace)
        .list(
            &ListParams::default()
                .fields(&format!("spec.nodeName={}", ctx.node))
                .labels(&selector),
        )
        .await?;
    Ok(pods.items.into_iter().filter_map(|pod| pod.uid()).collect())
}

/// Proves a writer Pod is really gone via the kubelet's `/pods` view
/// (`nodes/proxy` RBAC, proxied by the apiserver), since a Pod-delete
/// event alone is never sufficient. Fails closed (assumes the process
/// is still there) on any request error.
async fn writer_process_gone(client: &Client, node: &str, uid: &str) -> bool {
    let request =
        match http::Request::get(format!("/api/v1/nodes/{node}/proxy/pods")).body(Vec::new()) {
            Ok(request) => request,
            Err(error) => {
                tracing::warn!(%node, %error, "failed to build kubelet proxy request");
                return false;
            }
        };
    let pods: ObjectList<Pod> = match client.request(request).await {
        Ok(pods) => pods,
        Err(error) => {
            tracing::warn!(%node, %error, "kubelet proxy pods query failed");
            return false;
        }
    };
    match pods
        .items
        .iter()
        .find(|pod| pod.uid().as_deref() == Some(uid))
    {
        None => true,
        Some(pod) => pod
            .status
            .as_ref()
            .and_then(|status| status.container_statuses.as_ref())
            .is_some_and(|statuses| {
                !statuses.is_empty()
                    && statuses.iter().all(|status| {
                        status
                            .state
                            .as_ref()
                            .is_some_and(|s| s.terminated.is_some())
                    })
            }),
    }
}

async fn source_quiesced(
    ctx: &KubeNodeReader,
    status: &MirroredVolumeStatus,
    current_pods: &[String],
) -> bool {
    let Some(operation) = status.operation.as_ref() else {
        return false;
    };
    if operation.source.as_deref() != Some(ctx.node.as_str()) {
        return false;
    }
    let Some(previous_pods) = status
        .nodes
        .get(&ctx.node)
        .map(|node| &node.writer_pod_uids)
    else {
        return false;
    };
    if previous_pods.is_empty() || !current_pods.is_empty() {
        return false;
    }
    for uid in previous_pods {
        if !writer_process_gone(&ctx.client, &ctx.node, uid).await {
            return false;
        }
    }
    true
}

/// Patches this node's label onto its own Pod (the one thing a
/// `DaemonSet`'s shared pod template can't set per-instance), then
/// get-or-creates a small `Service` selecting on it for stable per-node
/// addressing. Idempotent, safe every boot; the Service has no
/// ownerReference so it outlives Pod restarts.
async fn ensure_node_label_and_service(ctx: &Ctx) -> anyhow::Result<()> {
    let Some(pod_name) = ctx.pod_name.as_deref() else {
        tracing::warn!(
            node = %ctx.node,
            "POD_NAME not set, skipping self-label/Service — peer addressing for this node will be unreachable"
        );
        return Ok(());
    };
    Api::<Pod>::namespaced(ctx.client.clone(), &ctx.namespace)
        .patch(
            pod_name,
            &PatchParams::default(),
            &Patch::Merge(serde_json::json!({
                "metadata": { "labels": { naming::NODE_LABEL: ctx.node } }
            })),
        )
        .await?;

    let mut ports = vec![
        serde_json::json!({ "name": "sync-tcp", "port": 22000, "protocol": "TCP" }),
        serde_json::json!({ "name": "sync-udp", "port": 22000, "protocol": "UDP" }),
    ];
    // stunnel's TLS port, never rsyncd's own (loopback-only).
    if ctx.backends.contains_key(mirrorvol_api::backend::RSYNC) {
        ports.push(
            serde_json::json!({ "name": "rsync-tls", "port": rsync::STUNNEL_PORT, "protocol": "TCP" }),
        );
    }

    let service_name = naming::per_node_service_name(&ctx.node);
    Api::<k8s_openapi::api::core::v1::Service>::namespaced(ctx.client.clone(), &ctx.namespace)
        .patch(
            &service_name,
            &PatchParams::apply(AGENT_NAME),
            &Patch::Apply(serde_json::json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": { "name": service_name },
                "spec": {
                    "selector": {
                        "app.kubernetes.io/name": naming::SYNCTHING_POD_APP_NAME,
                        naming::NODE_LABEL: ctx.node,
                    },
                    "ports": ports,
                }
            })),
        )
        .await?;
    Ok(())
}

/// Writes `volume_id`'s resolved consistency mode to `{dir}/{volume_id}` for
/// `mirrorvol-csi`'s attach hook to read — an atomic write (`.tmp` + rename)
/// so a concurrent reader never sees a torn file. Logged, not fatal: a
/// write failure here must not stall this volume's reconcile loop, and the
/// reader already fails safe (treats a missing/unreadable file as `strict`)
/// on the other end.
async fn write_consistency_signal(dir: &str, volume_id: &str, consistency: &str) {
    if let Err(error) = tokio::fs::create_dir_all(dir).await {
        tracing::warn!(%dir, %error, "cannot create shared consistency directory");
        return;
    }
    let path = std::path::Path::new(dir).join(volume_id);
    let tmp_path = path.with_extension("tmp");
    if let Err(error) = tokio::fs::write(&tmp_path, consistency).await {
        tracing::warn!(path = %tmp_path.display(), %error, "cannot write consistency signal");
        return;
    }
    if let Err(error) = tokio::fs::rename(&tmp_path, &path).await {
        tracing::warn!(path = %path.display(), %error, "cannot publish consistency signal");
    }
}

/// Thin wiring: build the real [`KubeNodeReader`], start whatever background
/// work this volume's backend needs (a no-op for every backend but
/// `rsync` — see [`LocalBackend::start_background_tasks`] — so this runs
/// unconditionally rather than gating on `spec.backend == RSYNC` itself),
/// then hand everything off to [`mirrorvol_agent::reconcile_node`]
/// (unit-tested against a fake `NodeReader`), and turn the
/// [`NodeReconcileOutcome`] into signal writes and a status patch.
async fn reconcile(mv: Arc<MirroredVolume>, ctx: Arc<Ctx>) -> Result<Action, kube::Error> {
    let namespace = mv.namespace().unwrap_or_else(|| ctx.namespace.clone());
    let status = mv.status.clone().unwrap_or_default();

    let backend = ctx.backends.get(mv.spec.backend.as_str());
    if let Some(backend) = backend.cloned() {
        backend.start_background_tasks(
            mv.name_any(),
            Duration::from_secs(mv.spec.storage.warm_sync_interval_seconds.into()),
        );
    }

    let reader = KubeNodeReader {
        client: ctx.client.clone(),
        namespace: namespace.clone(),
        node: ctx.node.clone(),
    };
    let outcome = reconcile_node(
        &ctx.node,
        &mv,
        &status,
        &reader,
        backend.map(|backend| backend.as_ref()),
    )
    .await;

    let NodeReconcileOutcome {
        consistency_signal,
        active_peer_signal,
        writer_agreement_signal,
        node_status,
        requeue_after,
        error,
    } = outcome;

    if let Some(error) = error {
        tracing::warn!(volume = %mv.name_any(), node = %ctx.node, %error, "node reconcile deferred");
    }

    // Order-independent — three separate files, none read by anything that
    // needs them written in a particular sequence relative to each other.
    if let Some(consistency) = &consistency_signal {
        write_consistency_signal(&ctx.consistency_dir, &mv.name_any(), consistency).await;
    }
    if let Some(active_peer) = &active_peer_signal {
        write_consistency_signal(
            &ctx.consistency_dir,
            &naming::active_peer_signal_file_name(&mv.name_any()),
            active_peer,
        )
        .await;
    }
    if let Some(writer_agreement) = &writer_agreement_signal {
        write_consistency_signal(
            &ctx.consistency_dir,
            &naming::writer_agreement_signal_file_name(&mv.name_any()),
            writer_agreement,
        )
        .await;
    }

    if let Some(node_status) = node_status {
        let node_status = match serde_json::to_value(node_status) {
            Ok(node_status) => node_status,
            Err(error) => {
                tracing::error!(volume = %mv.name_any(), node = %ctx.node, %error, "cannot serialize node status");
                return Ok(Action::requeue(Duration::from_secs(15)));
            }
        };
        let mut nodes = serde_json::Map::new();
        nodes.insert(ctx.node.clone(), node_status);
        Api::<MirroredVolume>::namespaced(ctx.client.clone(), &namespace)
            .patch_status(
                &mv.name_any(),
                &PatchParams::apply(&format!("{AGENT_NAME}-{}", ctx.node)),
                &Patch::Merge(serde_json::json!({ "status": { "nodes": nodes } })),
            )
            .await?;
    }

    Ok(match requeue_after {
        Some(duration) => Action::requeue(duration),
        None => Action::await_change(),
    })
}

fn error_policy(_: Arc<MirroredVolume>, _: &kube::Error, _: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(15))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();
    match Cli::parse().command {
        Some(Command::Provision) => provision::run().await,
        None | Some(Command::Run) => run().await,
    }
}

async fn run() -> anyhow::Result<()> {
    let node = std::env::var(naming::env::NODE_NAME)?;
    let namespace = std::env::var(naming::env::POD_NAMESPACE)
        .unwrap_or_else(|_| naming::env::POD_NAMESPACE_DEFAULT.to_owned());
    let pod_name = std::env::var("POD_NAME").ok();
    let endpoint = LocalEndpoint::from_env()?;
    let data_root = std::env::var("MIRRORVOL_DATA_ROOT").unwrap_or_else(|_| "/data".to_owned());
    let consistency_dir = std::env::var(naming::env::CONSISTENCY_DIR)
        .unwrap_or_else(|_| naming::env::CONSISTENCY_DIR_DEFAULT.to_owned());
    // Which `bestEffort` conflict_files implementation to run — "filesystem"
    // (default) or "events".
    let conflict_detection = match std::env::var("MIRRORVOL_CONFLICT_DETECTION").as_deref() {
        Ok("events") => ConflictDetection::SyncthingEvents,
        _ => ConflictDetection::Filesystem,
    };
    let client = Client::try_default().await?;
    let http = reqwest::Client::new();
    let device_id = fetch_device_id(&http, &endpoint.base_url, &endpoint.api_key).await?;

    // One agent process, potentially several backends. Syncthing is always
    // on; rsync is opt-in, enabled by a module secret being set.
    let mut backends: BackendRegistry = HashMap::new();
    backends.insert(
        mirrorvol_api::backend::SYNCTHING,
        Arc::new(
            SyncthingBackend::new(endpoint, &data_root).with_conflict_detection(conflict_detection),
        ),
    );
    let rsync_secret_file = std::env::var(rsync::env::RSYNC_SECRET_FILE).ok();
    let rsyncd_conf_path =
        std::env::var("RSYNC_DAEMON_CONFIG_FILE").unwrap_or_else(|_| "/etc/rsyncd.conf".to_owned());
    let rsync_backend = rsync_secret_file.map(|secret_file| {
        Arc::new(
            RsyncBackend::new(Arc::new(SystemRsyncRunner), &data_root)
                .with_secret_file(secret_file.clone())
                .with_rsyncd_conf_path(rsyncd_conf_path)
                .with_stunnel(Arc::new(rsync::SystemStunnelClient::new(
                    rsync::STUNNEL_PORT,
                    secret_file,
                ))),
        )
    });
    if let Some(rsync_backend) = &rsync_backend {
        backends.insert(
            mirrorvol_api::backend::RSYNC,
            Arc::clone(rsync_backend) as Arc<dyn LocalBackend>,
        );
        // Write the daemon config once now, even with zero volumes
        // configured — the rsyncd/stunnel sidecars wait for these files
        // before starting, so a node with no rsync-backed volume assigned
        // yet would otherwise never become ready.
        if let Err(error) = rsync_backend.write_rsyncd_conf().await {
            tracing::warn!(%error, "failed to write initial rsyncd.conf");
        }
    }

    let ctx = Arc::new(Ctx {
        node,
        namespace,
        pod_name,
        client: client.clone(),
        backends,
        consistency_dir,
    });
    register_backend_node(&ctx, mirrorvol_api::backend::SYNCTHING, &device_id).await?;
    if ctx.backends.contains_key(mirrorvol_api::backend::RSYNC) {
        // A real per-node identity token, written by `mirrorvol-agent
        // provision` (see `provision.rs`) — persisted, not derived from the
        // node name, so `identity_generation` is a live signal here now:
        // it bumps if this node's identity Secret is ever recreated with a
        // fresh value. Falls back to the node name only if the file is
        // somehow missing (e.g. an agent upgraded ahead of its own
        // `provision` initContainer having run once) — logged loudly since
        // that fallback silently reintroduces the old decorative behavior.
        let rsync_device_id = match std::env::var(rsync::env::RSYNC_IDENTITY_FILE)
            .ok()
            .and_then(|path| std::fs::read_to_string(&path).ok())
        {
            Some(identity) => identity.trim().to_owned(),
            None => {
                tracing::warn!(
                    node = %ctx.node,
                    "rsync per-node identity file unavailable, falling back to node name — identityGeneration will not track real changes until this is resolved"
                );
                ctx.node.clone()
            }
        };
        register_backend_node(&ctx, mirrorvol_api::backend::RSYNC, &rsync_device_id).await?;
    }
    ensure_node_label_and_service(&ctx).await?;
    Controller::new(
        Api::<MirroredVolume>::namespaced(client, &ctx.namespace),
        Default::default(),
    )
    .run(reconcile, error_policy, ctx)
    .for_each(|result| async move {
        if let Err(error) = result {
            tracing::error!(%error, "agent reconcile failed");
        }
    })
    .await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{owner_references, write_consistency_signal};

    #[test]
    fn owner_references_is_empty_when_the_lookup_missed() {
        assert!(owner_references("v1", "Node", "node-a", None).is_empty());
    }

    #[test]
    fn owner_references_builds_a_single_controller_owner_from_a_uid() {
        let refs = owner_references(
            "apps/v1",
            "Deployment",
            "mirrorvol-controller",
            Some("uid-123".to_owned()),
        );
        assert_eq!(refs.len(), 1);
        let owner = &refs[0];
        assert_eq!(owner.api_version, "apps/v1");
        assert_eq!(owner.kind, "Deployment");
        assert_eq!(owner.name, "mirrorvol-controller");
        assert_eq!(owner.uid, "uid-123");
        assert_eq!(owner.controller, Some(true));
        assert_eq!(owner.block_owner_deletion, Some(true));
    }

    #[tokio::test]
    async fn write_consistency_signal_writes_the_given_content() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_consistency_signal(dir.path().to_str().expect("utf8 path"), "vol-a", "strict").await;
        let contents = std::fs::read_to_string(dir.path().join("vol-a")).expect("read back");
        assert_eq!(contents, "strict");
    }

    #[tokio::test]
    async fn write_consistency_signal_creates_the_directory_if_missing() {
        let parent = tempfile::tempdir().expect("tempdir");
        let dir = parent.path().join("nested").join("consistency");
        write_consistency_signal(dir.to_str().expect("utf8 path"), "vol-a", "bestEffort").await;
        let contents = std::fs::read_to_string(dir.join("vol-a")).expect("read back");
        assert_eq!(contents, "bestEffort");
    }

    #[tokio::test]
    async fn write_consistency_signal_overwrites_atomically_leaving_no_tmp_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let dir_str = dir.path().to_str().expect("utf8 path");
        write_consistency_signal(dir_str, "vol-a", "strict").await;
        write_consistency_signal(dir_str, "vol-a", "bestEffort").await;
        let contents = std::fs::read_to_string(dir.path().join("vol-a")).expect("read back");
        assert_eq!(contents, "bestEffort");
        assert!(!dir.path().join("vol-a.tmp").exists());
    }
}
