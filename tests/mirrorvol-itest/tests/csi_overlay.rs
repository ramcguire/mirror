//! Proves `mirrorvol-csi` against a real deployed overlay driver plus
//! a real underlying driver. Three claims:
//!
//! 1. Generic delegation actually works: a PVC with no `mirrorvol.io/backend`
//!    set really provisions/mounts through the underlying `csi-hostpath`
//!    driver (isolated from claim 2 below).
//! 2. The Syncthing attach hook's readiness gate is real: requesting
//!    `mirrorvol.io/backend: syncthing` without a configured folder must
//!    fail closed — `NodeStageVolume` keeps returning FailedPrecondition.
//!    The positive case is covered by `full_promotion_cycle.rs`.
//! 3. That same gate is skipped outright for a `bestEffort` volume: with
//!    the shared consistency signal (see `node.rs`'s `volume_is_best_effort`)
//!    saying `bestEffort` for a volume whose backend was never configured
//!    (the exact unready state claim 2 proves fails closed under `strict`),
//!    `NodeStageVolume` must still succeed.
//!
//! Requires `task deploy:full` plus `task csi:install-test-driver` — `task
//! test:csi` does both, then runs just this file.

use std::process::Command;
use std::time::Duration;

use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod};
use kube::api::{Api, DeleteParams, PostParams};
use kube::core::ObjectMeta;
use mirrorvol_itest::{
    candidate_node_names, pod_events, pod_is_running, pvc_is_bound, syncthing_pod_for_node,
    test_client, unique_name, wait_until,
};

const NAMESPACE: &str = "mirrorvol";
const READY_TIMEOUT: Duration = Duration::from_secs(120);
const POLL_INTERVAL: Duration = Duration::from_secs(2);

fn storage_class(name: &str, backend: Option<&str>) -> k8s_openapi::api::storage::v1::StorageClass {
    let mut parameters = std::collections::BTreeMap::new();
    parameters.insert(
        "mirrorvol.io/underlyingDriver".to_owned(),
        "csi-hostpath".to_owned(),
    );
    if let Some(backend) = backend {
        parameters.insert("mirrorvol.io/backend".to_owned(), backend.to_owned());
    }
    k8s_openapi::api::storage::v1::StorageClass {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            ..Default::default()
        },
        provisioner: "mirrorvol.csi.homelab.internal".to_owned(),
        volume_binding_mode: Some("WaitForFirstConsumer".to_owned()),
        reclaim_policy: Some("Delete".to_owned()), // test fixture — fine to auto-clean
        parameters: Some(parameters),
        ..Default::default()
    }
}

fn pvc(name: &str, storage_class_name: &str) -> PersistentVolumeClaim {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": { "name": name, "namespace": NAMESPACE },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "storageClassName": storage_class_name,
            "resources": { "requests": { "storage": "16Mi" } },
        },
    }))
    .expect("static PVC JSON is always valid")
}

/// A Pod pinned to `node` via `nodeSelector` (not `spec.nodeName`, which
/// skips the real kube-scheduler — a WaitForFirstConsumer StorageClass only
/// gets its PVC's `selected-node` annotation set as a side effect of real
/// scheduling, which `--node-deployment` external-provisioner watches for)
/// that just mounts the PVC and sleeps.
fn consumer_pod(name: &str, node: &str, claim: &str) -> Pod {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": name, "namespace": NAMESPACE },
        "spec": {
            "nodeSelector": { "kubernetes.io/hostname": node },
            "containers": [{
                "name": "consumer",
                "image": "busybox:1.36",
                "command": ["sleep", "infinity"],
                "volumeMounts": [{ "name": "data", "mountPath": "/data" }],
            }],
            "volumes": [{
                "name": "data",
                "persistentVolumeClaim": { "claimName": claim },
            }],
        },
    }))
    .expect("static Pod JSON is always valid")
}

fn exec(pod: &str, container: &str, script: &str) -> bool {
    Command::new("kubectl")
        .args([
            "exec", "-n", NAMESPACE, pod, "-c", container, "--", "sh", "-c", script,
        ])
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

struct Cleanup {
    client: kube::Client,
    pod: String,
    pvc: String,
    storage_class: String,
}
impl Drop for Cleanup {
    fn drop(&mut self) {
        let (client, pod, pvc, storage_class) = (
            self.client.clone(),
            self.pod.clone(),
            self.pvc.clone(),
            self.storage_class.clone(),
        );
        tokio::spawn(async move {
            let _ = Api::<Pod>::namespaced(client.clone(), NAMESPACE)
                .delete(&pod, &DeleteParams::default())
                .await;
            let _ = Api::<PersistentVolumeClaim>::namespaced(client.clone(), NAMESPACE)
                .delete(&pvc, &DeleteParams::default())
                .await;
            let _ = Api::<k8s_openapi::api::storage::v1::StorageClass>::all(client)
                .delete(&storage_class, &DeleteParams::default())
                .await;
        });
    }
}

#[tokio::test]
#[ignore = "requires a fully deployed cluster plus the CSI test fixture — `task test:csi`"]
async fn a_backendless_pvc_delegates_real_provisioning_to_the_underlying_driver() {
    let client = test_client().await.expect("cluster reachable");
    let nodes = candidate_node_names(&client)
        .await
        .expect("list candidate nodes");
    let node = nodes.first().expect("at least one candidate node").clone();

    let sc_name = unique_name("mirrorvol-csi-test-passthrough");
    let sc_api = Api::<k8s_openapi::api::storage::v1::StorageClass>::all(client.clone());
    sc_api
        .create(&PostParams::default(), &storage_class(&sc_name, None))
        .await
        .expect("create passthrough StorageClass");

    let pvc_name = unique_name("csi-delegation");
    let pod_name = unique_name("csi-delegation-consumer");
    let _cleanup = Cleanup {
        client: client.clone(),
        pod: pod_name.clone(),
        pvc: pvc_name.clone(),
        storage_class: sc_name.clone(),
    };

    let claims: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), NAMESPACE);
    claims
        .create(&PostParams::default(), &pvc(&pvc_name, &sc_name))
        .await
        .expect("create PVC");

    let pods: Api<Pod> = Api::namespaced(client.clone(), NAMESPACE);
    pods.create(
        &PostParams::default(),
        &consumer_pod(&pod_name, &node, &pvc_name),
    )
    .await
    .expect("create consumer pod");

    let bound = wait_until(READY_TIMEOUT, POLL_INTERVAL, || {
        let client = client.clone();
        let pvc_name = pvc_name.clone();
        async move { pvc_is_bound(&client, NAMESPACE, &pvc_name).await }
    })
    .await;
    assert!(bound, "PVC {pvc_name} never became Bound — CreateVolume delegation to csi-hostpath never completed");

    let running = wait_until(READY_TIMEOUT, POLL_INTERVAL, || {
        let client = client.clone();
        let pod_name = pod_name.clone();
        async move { pod_is_running(&client, NAMESPACE, &pod_name).await }
    })
    .await;
    if !running {
        let events = pod_events(&client, NAMESPACE, &pod_name).await;
        panic!(
            "pod {pod_name} never became Running — NodeStageVolume/NodePublishVolume delegation \
             to csi-hostpath never completed. Recent events: {events:?}"
        );
    }
}

#[tokio::test]
#[ignore = "requires a fully deployed cluster plus the CSI test fixture — `task test:csi`"]
async fn node_stage_volume_fails_closed_for_an_unready_syncthing_backend() {
    let client = test_client().await.expect("cluster reachable");
    let nodes = candidate_node_names(&client)
        .await
        .expect("list candidate nodes");
    let node = nodes.first().expect("at least one candidate node").clone();

    // The real deploy/csi/storageclass.yaml — mirrorvol.io/backend:
    // syncthing — not a test-local one, since this is proving that exact
    // StorageClass's real enforcement behavior.
    let sc_name = "mirrorvol-syncthing";

    let pvc_name = unique_name("csi-unready-backend");
    let pod_name = unique_name("csi-unready-backend-consumer");
    let _cleanup = Cleanup {
        client: client.clone(),
        pod: pod_name.clone(),
        pvc: pvc_name.clone(),
        // Never actually created by this test — deleting it in Cleanup is
        // a harmless no-op (kube's delete on a missing name just errors,
        // swallowed by the best-effort `let _ =` above).
        storage_class: sc_name.to_owned(),
    };

    let claims: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), NAMESPACE);
    claims
        .create(&PostParams::default(), &pvc(&pvc_name, sc_name))
        .await
        .expect("create PVC");

    let pods: Api<Pod> = Api::namespaced(client.clone(), NAMESPACE);
    pods.create(
        &PostParams::default(),
        &consumer_pod(&pod_name, &node, &pvc_name),
    )
    .await
    .expect("create consumer pod");

    // The PVC itself should still bind — CreateVolume delegation doesn't
    // know or care about the backend gate; only NodeStageVolume does (see
    // node.rs). Confirms this test isn't accidentally exercising claim 1
    // (delegation) instead of claim 2 (the gate).
    let bound = wait_until(READY_TIMEOUT, POLL_INTERVAL, || {
        let client = client.clone();
        let pvc_name = pvc_name.clone();
        async move { pvc_is_bound(&client, NAMESPACE, &pvc_name).await }
    })
    .await;
    assert!(bound, "PVC {pvc_name} never became Bound");

    // The gate must hold for a real, sustained window — a single false
    // "still not running" read shortly after creation wouldn't distinguish
    // "correctly rejected" from "just hasn't gotten there yet".
    let started = wait_until(Duration::from_secs(30), POLL_INTERVAL, || {
        let client = client.clone();
        let pod_name = pod_name.clone();
        async move { pod_is_running(&client, NAMESPACE, &pod_name).await }
    })
    .await;
    assert!(
        !started,
        "pod {pod_name} became Running — NodeStageVolume's Syncthing readiness gate let an \
         unconfigured/never-synced folder through, which is exactly the invariant it exists to \
         enforce (see attach_syncthing in node.rs)"
    );

    let events = pod_events(&client, NAMESPACE, &pod_name).await;
    let saw_failed_mount = events.iter().any(|message| {
        message.contains("FailedMount")
            || message.contains("not fully synced")
            || message.contains("not this node's writer role")
    });
    assert!(
        saw_failed_mount,
        "expected a FailedMount event referencing the readiness gate, got: {events:?}"
    );
}

#[tokio::test]
#[ignore = "requires a fully deployed cluster plus the CSI test fixture — `task test:csi`"]
async fn node_stage_volume_skips_the_gate_for_a_best_effort_volume() {
    let client = test_client().await.expect("cluster reachable");
    let nodes = candidate_node_names(&client)
        .await
        .expect("list candidate nodes");
    let node = nodes.first().expect("at least one candidate node").clone();

    let sc_name = "mirrorvol-syncthing";

    // No StorageClass parameter sets mirrorvol.io/volumeId, and this PVC
    // name carries no "-{node}" suffix for resolve_volume_id to strip, so
    // mirrorvol-csi resolves this exact name as the volume ID — see
    // controller.rs's resolve_volume_id. That's what volume_is_best_effort
    // (node.rs) looks up in the shared consistency directory below.
    let pvc_name = unique_name("csi-best-effort-gate");
    let pod_name = unique_name("csi-best-effort-gate-consumer");
    let _cleanup = Cleanup {
        client: client.clone(),
        pod: pod_name.clone(),
        pvc: pvc_name.clone(),
        storage_class: sc_name.to_owned(),
    };

    // Plant the signal mirrorvol-agent would otherwise only write once it
    // has actually reconciled a bestEffort MirroredVolume naming this exact
    // volume ID — no such MirroredVolume exists here on purpose, so this is
    // the only way to prove NodeStageVolume itself honors the signal,
    // isolated from mirrorvol-agent ever getting a chance to write it.
    let syncthing_pod = syncthing_pod_for_node(&client, NAMESPACE, &node)
        .await
        .expect("node's syncthing pod");
    assert!(
        exec(
            &syncthing_pod,
            "mirrorvol-agent",
            &format!(
                "mkdir -p /shared-consistency && printf '%s' 'bestEffort' > /shared-consistency/{pvc_name}"
            ),
        ),
        "failed to plant the bestEffort consistency signal for {pvc_name}"
    );

    let claims: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), NAMESPACE);
    claims
        .create(&PostParams::default(), &pvc(&pvc_name, sc_name))
        .await
        .expect("create PVC");

    let pods: Api<Pod> = Api::namespaced(client.clone(), NAMESPACE);
    pods.create(
        &PostParams::default(),
        &consumer_pod(&pod_name, &node, &pvc_name),
    )
    .await
    .expect("create consumer pod");

    let bound = wait_until(READY_TIMEOUT, POLL_INTERVAL, || {
        let client = client.clone();
        let pvc_name = pvc_name.clone();
        async move { pvc_is_bound(&client, NAMESPACE, &pvc_name).await }
    })
    .await;
    assert!(bound, "PVC {pvc_name} never became Bound");

    // The exact backend state (never configured, never synced, never
    // writer) that node_stage_volume_fails_closed_for_an_unready_syncthing_backend
    // proves fails closed under strict — here it must not gate at all.
    let running = wait_until(READY_TIMEOUT, POLL_INTERVAL, || {
        let client = client.clone();
        let pod_name = pod_name.clone();
        async move { pod_is_running(&client, NAMESPACE, &pod_name).await }
    })
    .await;
    if !running {
        let events = pod_events(&client, NAMESPACE, &pod_name).await;
        panic!(
            "pod {pod_name} never became Running — a bestEffort volume's NodeStageVolume must \
             skip the Syncthing readiness gate entirely (see volume_is_best_effort in node.rs). \
             Recent events: {events:?}"
        );
    }
}
