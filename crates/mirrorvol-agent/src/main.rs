mod provision;

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
use mirrorvol_agent::reconcile_local;
use mirrorvol_api::{naming, BackendNode, MirroredVolume, MirroredVolumeStatus};
use mirrorvol_backend::syncthing::{
    api_key_from_env, fetch_device_id, ConflictDetection, LocalEndpoint, SyncthingBackend,
};
use mirrorvol_backend::ReplicaConfig;

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
    backend: SyncthingBackend,
    /// Shared `emptyDir` directory this agent writes each volume's resolved
    /// consistency mode to. `mirrorvol-csi`'s attach hook reads it back
    /// since mirrorvol-csi has no Kubernetes API access of its own to read
    /// `MirroredVolume.spec` directly.
    consistency_dir: String,
}

fn backend_node_name(node: &str) -> String {
    format!("{node}-syncthing")
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

async fn register_backend_node(ctx: &Ctx, device_id: &str) -> anyhow::Result<()> {
    let api = Api::<BackendNode>::namespaced(ctx.client.clone(), &ctx.namespace);
    let name = backend_node_name(&ctx.node);
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
            "spec": { "node": ctx.node, "backend": mirrorvol_api::backend::SYNCTHING }
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

async fn replica_config(ctx: &Ctx, mv: &MirroredVolume) -> anyhow::Result<ReplicaConfig> {
    let nodes = Api::<BackendNode>::namespaced(ctx.client.clone(), &ctx.namespace)
        .list(&Default::default())
        .await?;
    let mut peers = Vec::new();
    let mut peer_addresses = std::collections::BTreeMap::new();
    for backend_node in nodes.items {
        if backend_node.spec.backend != mirrorvol_api::backend::SYNCTHING
            || backend_node.spec.node == ctx.node
            || !mv.spec.candidate_nodes.contains(&backend_node.spec.node)
        {
            continue;
        }
        // The candidate device set is fixed by identity, not momentary
        // health. Health gates promotion elsewhere; it doesn't gate folder
        // membership.
        if let Some(device_id) = backend_node.status.and_then(|status| status.device_id) {
            // Each candidate's agent fronts its Syncthing sync port
            // with a per-node Service via `ensure_node_label_and_service`.
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
    Ok(ReplicaConfig {
        volume_id: mv.name_any(),
        local_path,
        peer_device_ids: peers,
        peer_addresses,
        generation,
        ignore_patterns: mv.spec.storage.ignore_patterns.clone(),
    })
}

/// Reads this candidate's replica PVC's bound `PersistentVolume` and
/// returns `mirrorvol_api::naming::RESOLVED_PATH_ATTRIBUTE` from its
/// `spec.csi.volumeAttributes`, if present. `Ok(None)` covers every
/// "not applicable yet" case uniformly; only a real API error
/// is `Err`.
async fn resolved_underlying_path(ctx: &Ctx, claim_name: &str) -> anyhow::Result<Option<String>> {
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

async fn local_writer_pod_uids(ctx: &Ctx, mv: &MirroredVolume) -> anyhow::Result<Vec<String>> {
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
    ctx: &Ctx,
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
                    "ports": [
                        { "name": "sync-tcp", "port": 22000, "protocol": "TCP" },
                        { "name": "sync-udp", "port": 22000, "protocol": "UDP" },
                    ],
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

async fn reconcile(mv: Arc<MirroredVolume>, ctx: Arc<Ctx>) -> Result<Action, kube::Error> {
    if !mv.spec.candidate_nodes.contains(&ctx.node) {
        return Ok(Action::await_change());
    }
    let namespace = mv.namespace().unwrap_or_else(|| ctx.namespace.clone());
    let status = mv.status.clone().unwrap_or_default();
    // `mv.spec.consistency` is already the one canonical `Consistency` the
    // API server validated at admission — no re-resolution needed, and
    // `.as_str()` is the same type's own string form, not a hand-typed
    // constant that could drift from it.
    write_consistency_signal(
        &ctx.consistency_dir,
        &mv.name_any(),
        mv.spec.consistency.as_str(),
    )
    .await;
    let config = match replica_config(&ctx, &mv).await {
        Ok(config) => config,
        Err(error) => {
            tracing::warn!(volume = %mv.name_any(), node = %ctx.node, %error, "replica configuration unavailable");
            return Ok(Action::requeue(Duration::from_secs(15)));
        }
    };
    let writer_pod_uids = match local_writer_pod_uids(&ctx, &mv).await {
        Ok(pods) => pods,
        Err(error) => {
            tracing::warn!(volume = %mv.name_any(), node = %ctx.node, %error, "writer pod observation unavailable");
            return Ok(Action::requeue(Duration::from_secs(15)));
        }
    };
    let quiesced = source_quiesced(&ctx, &status, &writer_pod_uids).await;
    match reconcile_local(
        &ctx.node,
        &config,
        &status,
        mv.spec.consistency,
        mv.spec.storage.pull_only,
        quiesced,
        writer_pod_uids,
        &ctx.backend,
    )
    .await
    {
        Ok(node_status) => {
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
            Ok(Action::requeue(Duration::from_secs(5)))
        }
        Err(error) => {
            tracing::warn!(volume = %mv.name_any(), node = %ctx.node, %error, "local reconcile deferred");
            Ok(Action::requeue(Duration::from_secs(5)))
        }
    }
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
    let base_url = std::env::var(mirrorvol_backend::syncthing::env::SYNCTHING_BASE_URL)
        .unwrap_or_else(|_| {
            mirrorvol_backend::syncthing::env::SYNCTHING_BASE_URL_DEFAULT.to_owned()
        });
    let api_key = api_key_from_env(
        mirrorvol_backend::syncthing::env::SYNCTHING_API_KEY,
        mirrorvol_backend::syncthing::env::SYNCTHING_API_KEY_FILE,
    )?;
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
    let device_id = fetch_device_id(&http, &base_url, &api_key).await?;
    let ctx = Arc::new(Ctx {
        node,
        namespace,
        pod_name,
        client: client.clone(),
        backend: SyncthingBackend::new(LocalEndpoint { base_url, api_key }, &data_root)
            .with_conflict_detection(conflict_detection),
        consistency_dir,
    });
    register_backend_node(&ctx, &device_id).await?;
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
