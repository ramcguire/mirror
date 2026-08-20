use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::PersistentVolumeClaim;
use kube::api::{Api, Patch, PatchParams};
use kube::runtime::controller::{Action, Controller};
use kube::{Client, ResourceExt};
use mirrorvol_api::{naming, BackendNode, MirroredVolume, MirroredVolumeSpec};
use mirrorvol_controller::{
    reconcile as controller_reconcile, replica_claim_name, AppScaler, ClusterReader,
    ReconcileOutcome,
};

struct Ctx {
    client: Client,
}

struct KubeAppScaler {
    client: Client,
    namespace: String,
}

pub(crate) const CONTROLLER_NAME: &str = "mirrorvol-controller";

#[async_trait::async_trait]
impl AppScaler for KubeAppScaler {
    async fn scale(&self, deployment: &str, replicas: u32) -> Result<(), String> {
        Api::<Deployment>::namespaced(self.client.clone(), &self.namespace)
            .patch_scale(
                deployment,
                &PatchParams::apply(CONTROLLER_NAME),
                &Patch::Merge(serde_json::json!({ "spec": { "replicas": replicas } })),
            )
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    async fn is_scaled_to(&self, deployment: &str, replicas: u32) -> Result<bool, String> {
        let deployment = Api::<Deployment>::namespaced(self.client.clone(), &self.namespace)
            .get(deployment)
            .await
            .map_err(|error| error.to_string())?;
        Ok(deployment
            .status
            .and_then(|status| status.replicas)
            .unwrap_or(0) as u32
            == replicas)
    }

    async fn set_node_selector(&self, deployment: &str, node: &str) -> Result<(), String> {
        Api::<Deployment>::namespaced(self.client.clone(), &self.namespace)
            .patch(
                deployment,
                // Distinct field manager from set_volume_claim's below is
                // load-bearing: server-side apply's field ownership is
                // per-manager-per-apply-call, so two Patch::Apply calls
                // under the same manager name would cause the second to
                // silently revert that field.
                &PatchParams::apply(&format!("{CONTROLLER_NAME}-node-selector")),
                &Patch::Apply(serde_json::json!({
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "spec": { "template": { "spec": {
                        "nodeSelector": { "kubernetes.io/hostname": node }
                    }}}
                })),
            )
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    async fn set_volume_claim(
        &self,
        deployment: &str,
        volume_name: &str,
        claim_name: &str,
    ) -> Result<(), String> {
        Api::<Deployment>::namespaced(self.client.clone(), &self.namespace)
            .patch(
                deployment,
                // .force(): nulling `emptyDir` below only takes effect if
                // this apply also takes over its ownership from whichever
                // manager (typically the operator's placeholder Deployment)
                // currently holds it.
                &PatchParams::apply(&format!("{CONTROLLER_NAME}-volume-claim")).force(),
                &Patch::Apply(serde_json::json!({
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "spec": { "template": { "spec": {
                        "volumes": [{
                            "name": volume_name,
                            "persistentVolumeClaim": { "claimName": claim_name },
                            // Server-side apply merges list items by `name`
                            // field-by-field, never clearing a sibling field
                            // on its own. An explicit `null` is what
                            // actually removes the placeholder `emptyDir`;
                            // without it the API server rejects the merge.
                            "emptyDir": null
                        }]
                    }}}
                })),
            )
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }
}

/// The real, cluster-backed [`ClusterReader`] `main.rs` wires into
/// [`controller_reconcile`] — everything below is the same I/O this crate
/// always did, just grouped behind that trait instead of called directly
/// from `reconcile()`.
struct KubeClusterReader {
    client: Client,
    namespace: String,
}

#[async_trait::async_trait]
impl ClusterReader for KubeClusterReader {
    async fn backend_status_for_candidates(
        &self,
        spec: &MirroredVolumeSpec,
    ) -> Result<(BTreeSet<String>, BTreeMap<String, u64>), String> {
        let nodes = Api::<BackendNode>::namespaced(self.client.clone(), &self.namespace)
            .list(&Default::default())
            .await
            .map_err(|error| error.to_string())?;
        let matching = nodes
            .items
            .into_iter()
            .filter(|node| node.spec.backend == spec.backend);
        let mut available = BTreeSet::new();
        let mut identity_generations = BTreeMap::new();
        for node in matching {
            // Only recorded once a status exists. A BackendNode briefly has
            // none between its spec-apply and its first status-apply, and
            // treating that gap as "generation 0" would read as a false
            // contradiction.
            if let Some(generation) = node
                .status
                .as_ref()
                .map(|status| status.identity_generation)
            {
                identity_generations.insert(node.spec.node.clone(), generation);
            }
            available.insert(node.spec.node);
        }
        Ok((available, identity_generations))
    }

    async fn owned_by_another_volume(
        &self,
        self_name: &str,
        spec: &MirroredVolumeSpec,
    ) -> Result<bool, String> {
        let others = Api::<MirroredVolume>::namespaced(self.client.clone(), &self.namespace)
            .list(&Default::default())
            .await
            .map_err(|error| error.to_string())?;
        Ok(others.items.iter().any(|other| {
            other.name_any() != self_name
                && other.spec.workload.name == spec.workload.name
                && other.spec.workload.volume_name == spec.workload.volume_name
        }))
    }

    /// The target Deployment's *configured* replica count. This is a check
    /// on what the workload was authored with, not on how many Pods happen
    /// to be running this instant. Kubernetes defaults an unset
    /// `spec.replicas` to `1`.
    async fn deployment_desired_replicas(&self, deployment: &str) -> Result<u32, String> {
        let deployment = Api::<Deployment>::namespaced(self.client.clone(), &self.namespace)
            .get(deployment)
            .await
            .map_err(|error| error.to_string())?;
        Ok(deployment
            .spec
            .and_then(|spec| spec.replicas)
            .unwrap_or(1)
            .max(0) as u32)
    }

    async fn ensure_replica_claims(
        &self,
        volume_id: &str,
        spec: &MirroredVolumeSpec,
    ) -> Result<(), String> {
        let claims = Api::<PersistentVolumeClaim>::namespaced(self.client.clone(), &self.namespace);
        for node in &spec.candidate_nodes {
            let mut claim_spec = spec.storage.claim_template.clone();
            let Some(claim_spec) = claim_spec.as_object_mut() else {
                return Err("storage.claimTemplate must be a PVC spec object".to_owned());
            };
            claim_spec.insert(
                "storageClassName".to_owned(),
                serde_json::Value::String(spec.storage.storage_class_name.clone()),
            );
            let claim_name = replica_claim_name(volume_id, node);
            claims
                .patch(
                    &claim_name,
                    &PatchParams::apply(CONTROLLER_NAME),
                    &Patch::Apply(serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "PersistentVolumeClaim",
                        "metadata": {
                            "name": claim_name,
                            "labels": {
                                naming::VOLUME_LABEL: volume_id,
                                naming::CANDIDATE_NODE_LABEL: node
                            },
                            "annotations": {
                                "volume.kubernetes.io/selected-node": node,
                                // For kubectl/observability only — `mirrorvol-csi`
                                // has no Kubernetes API access and recovers
                                // `volume_id` from the claim name's suffix instead.
                                naming::VOLUME_LABEL: volume_id
                            }
                        },
                        "spec": claim_spec
                    })),
                )
                .await
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

/// Thin wiring: build the real [`KubeClusterReader`]/[`KubeAppScaler`],
/// hand everything off to [`controller_reconcile`] (`mirrorvol-controller`'s
/// `lib.rs` — the whole per-reconcile sequence lives there now, unit-tested
/// against a fake `ClusterReader`/`AppScaler`), then turn the
/// [`ReconcileOutcome`] into the actual patches.
async fn reconcile(mv: Arc<MirroredVolume>, ctx: Arc<Ctx>) -> Result<Action, kube::Error> {
    let namespace = mv.namespace().unwrap_or_else(|| "default".to_owned());
    let name = mv.name_any();
    let status = mv.status.clone().unwrap_or_default();

    let reader = KubeClusterReader {
        client: ctx.client.clone(),
        namespace: namespace.clone(),
    };
    let scaler = KubeAppScaler {
        client: ctx.client.clone(),
        namespace: namespace.clone(),
    };
    let ReconcileOutcome {
        status: next,
        clear_annotations,
        requeue_after,
        error,
    } = controller_reconcile(
        &name,
        mv.metadata.generation,
        mv.annotations(),
        &mv.spec,
        &status,
        &reader,
        &scaler,
    )
    .await;

    if let Some(error) = error {
        tracing::warn!(volume = %name, %error, "cluster read unavailable");
    }

    let volumes = Api::<MirroredVolume>::namespaced(ctx.client.clone(), &namespace);
    if let Some(next) = next {
        // Never include agent-owned `status.nodes` in this patch. Sent as
        // one merge patch on every path that reaches here, including the
        // operator-override paths — there, most of these fields are
        // unchanged from what's already stored (a merge patch setting a
        // field to its current value is a no-op), which is what lets this
        // stay the one patch shape every path produces instead of a
        // narrower one per override.
        volumes
            .patch_status(
                &name,
                &PatchParams::apply(CONTROLLER_NAME),
                &Patch::Merge(serde_json::json!({ "status": {
                    "active": next.active,
                    "operation": next.operation,
                    "grant": next.grant,
                    "conditions": next.conditions,
                    "nodeIdentityGenerations": next.node_identity_generations,
                    "nodeWriterAgreement": next.node_writer_agreement,
                    "writerAgreement": next.writer_agreement,
                }})),
            )
            .await?;
    }
    if !clear_annotations.is_empty() {
        // Plain JSON merge patch, not server-side apply — deletes these
        // annotation keys regardless of which manager owns them. Consumed
        // once: the next reconcile sees no matching annotation.
        let annotations: serde_json::Map<String, serde_json::Value> = clear_annotations
            .into_iter()
            .map(|key| (key, serde_json::Value::Null))
            .collect();
        volumes
            .patch(
                &name,
                &PatchParams::default(),
                &Patch::Merge(serde_json::json!({ "metadata": { "annotations": annotations } })),
            )
            .await?;
    }
    // A `MirroredVolume` change (including this reconcile's own status
    // patch) already retriggers immediately via the watch this Controller
    // is built on — this fallback tick only exists for time passing on its
    // own, which Kubernetes has no "event" for. `requeue_after` already
    // picked the right tier (1s operator-override, 15s read failure,
    // deadline-aware/5s normal path) — see `controller_reconcile`'s own doc
    // comment.
    Ok(Action::requeue(requeue_after))
}

fn error_policy(_: Arc<MirroredVolume>, _: &kube::Error, _: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(15))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();
    let client = Client::try_default().await?;
    let volumes = Api::<MirroredVolume>::all(client.clone());
    let backend_nodes = Api::<BackendNode>::all(client.clone());
    let controller = Controller::new(volumes, Default::default());
    // `BackendNode` health/identity changes previously had no watch of
    // their own — the only thing that noticed one was this reconcile's own
    // periodic tick re-listing `BackendNode`s (see
    // `backend_status_for_candidates` above). This links a `BackendNode`
    // change to every `MirroredVolume` naming that node as a candidate, so
    // it requeues immediately instead of waiting up to 5s. Reads
    // `controller.store()` (this Controller's own in-memory `MirroredVolume`
    // reflector) synchronously — `watches`' mapper can't be async — rather
    // than issuing a fresh List call per event.
    let store = controller.store();
    let controller = controller.watches(
        backend_nodes,
        Default::default(),
        move |node: BackendNode| {
            let candidate_node = node.spec.node.clone();
            store
                .state()
                .into_iter()
                .filter(move |mv| mv.spec.candidate_nodes.contains(&candidate_node))
                .map(|mv| kube::runtime::reflector::ObjectRef::from_obj(mv.as_ref()))
                .collect::<Vec<_>>()
        },
    );
    controller
        .run(reconcile, error_policy, Arc::new(Ctx { client }))
        .for_each(|result| async move {
            if let Err(error) = result {
                tracing::error!(%error, "controller reconcile failed");
            }
        })
        .await;
    Ok(())
}

// `degraded_recovery_message`'s own tests moved to `mirrorvol-controller`'s
// `lib.rs` alongside the function itself; the sequencing this crate adds on
// top (`controller_reconcile`'s use of it, `KubeClusterReader`) needs a real
// or fake `kube::Client`, so it's covered by `task test:integration`/
// `task test:e2e` instead of a `#[cfg(test)]` module here.
