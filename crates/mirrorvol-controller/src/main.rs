use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::PersistentVolumeClaim;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::api::{Api, Patch, PatchParams};
use kube::runtime::controller::{Action, Controller};
use kube::{Client, ResourceExt};
use mirrorvol_api::{
    naming, BackendNode, MirroredVolume, MirroredVolumeSpec, MirroredVolumeStatus,
};
use mirrorvol_controller::{
    decide, identity_reenrollment, replica_claim_name, AdmissionContext, AppScaler,
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

/// Every candidate node in `spec` with a `BackendNode` advertising
/// `spec.backend`, plus each node's `status.identityGeneration`. The
/// I/O read stays here so `decide()` remains a pure function.
async fn backend_status_for_candidates(
    client: Client,
    namespace: &str,
    spec: &MirroredVolumeSpec,
) -> Result<(BTreeSet<String>, BTreeMap<String, u64>), String> {
    let nodes = Api::<BackendNode>::namespaced(client, namespace)
        .list(&Default::default())
        .await
        .map_err(|error| error.to_string())?;
    let matching = nodes
        .items
        .into_iter()
        .filter(|node| node.spec.backend == spec.backend);
    let mut available = std::collections::BTreeSet::new();
    let mut identity_generations = std::collections::BTreeMap::new();
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

/// True if some other `MirroredVolume` in this namespace already names the
/// same `(workload.name, workload.volumeName)` pair as `spec`.
async fn owned_by_another_volume(
    client: Client,
    namespace: &str,
    self_name: &str,
    spec: &MirroredVolumeSpec,
) -> Result<bool, String> {
    let others = Api::<MirroredVolume>::namespaced(client, namespace)
        .list(&Default::default())
        .await
        .map_err(|error| error.to_string())?;
    Ok(others.items.iter().any(|other| {
        other.name_any() != self_name
            && other.spec.workload.name == spec.workload.name
            && other.spec.workload.volume_name == spec.workload.volume_name
    }))
}

/// The target Deployment's *configured* replica count. This is a check on
/// what the workload was authored with, not on how many Pods happen to be
/// running this instant. Kubernetes defaults an unset `spec.replicas` to
/// `1`.
async fn deployment_desired_replicas(
    client: Client,
    namespace: &str,
    deployment: &str,
) -> Result<u32, String> {
    let deployment = Api::<Deployment>::namespaced(client, namespace)
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
    client: Client,
    namespace: &str,
    volume_id: &str,
    spec: &MirroredVolumeSpec,
) -> Result<(), String> {
    let claims = Api::<PersistentVolumeClaim>::namespaced(client, namespace);
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

/// Detects a matching `mirrorvol.io/recoverDegradedOperation` override on a
/// `Degraded` volume. Returns the `Recovered` condition's message when it
/// applies; `None` otherwise.
fn degraded_recovery_message(
    annotations: &std::collections::BTreeMap<String, String>,
    status: &MirroredVolumeStatus,
) -> Option<String> {
    let operation = status.operation.as_ref()?;
    if operation.phase != mirrorvol_api::phase::DEGRADED {
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

async fn reconcile(mv: Arc<MirroredVolume>, ctx: Arc<Ctx>) -> Result<Action, kube::Error> {
    let namespace = mv.namespace().unwrap_or_else(|| "default".to_owned());
    let name = mv.name_any();
    let status = mv.status.clone().unwrap_or_default();

    if let Some(message) = degraded_recovery_message(mv.annotations(), &status) {
        tracing::warn!(volume = %name, %message, "degraded operation recovered by operator override");
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
            observed_generation: mv.metadata.generation,
            last_transition_time: k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
                chrono::Utc::now(),
            ),
        });
        let volumes = Api::<MirroredVolume>::namespaced(ctx.client.clone(), &namespace);
        // Same field manager decide()'s own status patch uses, sending all
        // fields together — avoids the field-manager collision KubeAppScaler
        // documents.
        volumes
            .patch_status(
                &name,
                &PatchParams::apply(CONTROLLER_NAME),
                &Patch::Merge(serde_json::json!({ "status": {
                    "active": null,
                    "operation": null,
                    "grant": null,
                    "conditions": conditions,
                }})),
            )
            .await?;
        // Plain JSON merge patch, not server-side apply — deletes both
        // annotation keys regardless of which manager owns them. Consumed
        // once: the next reconcile sees no matching annotation.
        volumes
            .patch(
                &name,
                &PatchParams::default(),
                &Patch::Merge(serde_json::json!({ "metadata": { "annotations": {
                    mirrorvol_api::naming::RECOVER_DEGRADED_OPERATION_ANNOTATION: null,
                    mirrorvol_api::naming::RECOVERY_REASON_ANNOTATION: null,
                }}})),
            )
            .await?;
        return Ok(Action::requeue(Duration::from_secs(1)));
    }

    if let Err(error) = ensure_replica_claims(ctx.client.clone(), &namespace, &name, &mv.spec).await
    {
        tracing::warn!(volume = %name, %error, "replica claims unavailable");
        return Ok(Action::requeue(Duration::from_secs(15)));
    }
    let (backend_available, identity_generations) =
        match backend_status_for_candidates(ctx.client.clone(), &namespace, &mv.spec).await {
            Ok(result) => result,
            Err(error) => {
                tracing::warn!(volume = %name, %error, "backend availability unavailable");
                return Ok(Action::requeue(Duration::from_secs(15)));
            }
        };

    if let Some((node, generation, message)) = identity_reenrollment(
        mv.annotations(),
        &status.node_identity_generations,
        &identity_generations,
    ) {
        tracing::warn!(volume = %name, %node, generation, %message, "identity contradiction re-enrolled by operator override");
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
            observed_generation: mv.metadata.generation,
            last_transition_time: k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
                chrono::Utc::now(),
            ),
        });
        let volumes = Api::<MirroredVolume>::namespaced(ctx.client.clone(), &namespace);
        // Same field manager decide()'s own status patch uses (see
        // degraded_recovery_message's identical comment above). Only
        // touches this one node's baseline entry — a merge patch on a map
        // key, same shape ensure_replica_claims/backend_status_for_candidates'
        // callers already rely on elsewhere for per-node status.
        volumes
            .patch_status(
                &name,
                &PatchParams::apply(CONTROLLER_NAME),
                &Patch::Merge(serde_json::json!({ "status": {
                    "nodeIdentityGenerations": { node: generation },
                    "conditions": conditions,
                }})),
            )
            .await?;
        volumes
            .patch(
                &name,
                &PatchParams::default(),
                &Patch::Merge(serde_json::json!({ "metadata": { "annotations": {
                    mirrorvol_api::naming::REENROLL_NODE_IDENTITY_ANNOTATION: null,
                    mirrorvol_api::naming::RECOVERY_REASON_ANNOTATION: null,
                }}})),
            )
            .await?;
        return Ok(Action::requeue(Duration::from_secs(1)));
    }

    let owned_by_another =
        match owned_by_another_volume(ctx.client.clone(), &namespace, &name, &mv.spec).await {
            Ok(result) => result,
            Err(error) => {
                tracing::warn!(volume = %name, %error, "sibling MirroredVolume lookup unavailable");
                return Ok(Action::requeue(Duration::from_secs(15)));
            }
        };
    // Only read (and only enforce) before this volume has ever adopted a
    // writer — see AdmissionContext::pre_adoption_replicas.
    let pre_adoption_replicas = if status.active.is_none() {
        match deployment_desired_replicas(ctx.client.clone(), &namespace, &mv.spec.workload.name)
            .await
        {
            Ok(replicas) => Some(replicas),
            Err(error) => {
                tracing::warn!(volume = %name, %error, "target Deployment unavailable");
                return Ok(Action::requeue(Duration::from_secs(15)));
            }
        }
    } else {
        None
    };
    let admission = AdmissionContext {
        backend_available: &backend_available,
        owned_by_another,
        pre_adoption_replicas,
    };
    let scaler = KubeAppScaler {
        client: ctx.client.clone(),
        namespace: namespace.clone(),
    };

    let next = decide(
        &name,
        &mv.spec,
        &status,
        &admission,
        &identity_generations,
        &scaler,
    )
    .await;
    // Never include agent-owned `status.nodes` in this patch.
    let patch = serde_json::json!({ "status": {
        "active": next.active,
        "operation": next.operation,
        "grant": next.grant,
        "conditions": next.conditions,
        "nodeIdentityGenerations": next.node_identity_generations,
    }});
    Api::<MirroredVolume>::namespaced(ctx.client.clone(), &namespace)
        .patch_status(
            &name,
            &PatchParams::apply(CONTROLLER_NAME),
            &Patch::Merge(&patch),
        )
        .await?;
    Ok(Action::requeue(Duration::from_secs(5)))
}

fn error_policy(_: Arc<MirroredVolume>, _: &kube::Error, _: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(15))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();
    let client = Client::try_default().await?;
    let volumes = Api::<MirroredVolume>::all(client.clone());
    Controller::new(volumes, Default::default())
        .run(reconcile, error_policy, Arc::new(Ctx { client }))
        .for_each(|result| async move {
            if let Err(error) = result {
                tracing::error!(%error, "controller reconcile failed");
            }
        })
        .await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mirrorvol_api::PromotionOperation;

    fn degraded_status(id: u64) -> MirroredVolumeStatus {
        MirroredVolumeStatus {
            operation: Some(PromotionOperation {
                id,
                source: Some("node-a".to_owned()),
                target: "node-b".to_owned(),
                epoch: id,
                phase: mirrorvol_api::phase::DEGRADED.to_owned(),
                started_at: "0".to_owned(),
                deadline: "0".to_owned(),
            }),
            ..Default::default()
        }
    }

    fn annotations(pairs: &[(&str, &str)]) -> std::collections::BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
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
}
