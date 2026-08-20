//! Proves the writer-agreement CSI veto (`node.rs`'s
//! `relayed_writer_agreement_is_contradiction`/`verify_local_writer`)
//! against a real, fully deployed mirrorvol: a real `mirrorvol-controller`,
//! real per-node `mirrorvol-agent`+Syncthing DaemonSet pods, and a real
//! `NodeStageVolume` call through `mirrorvol-csi`.
//!
//! `full_promotion_cycle.rs` already proves the writer-agreement machinery
//! doesn't break a real promotion (it gates `Granting` internally). What it
//! can't prove is the new veto path specifically: a node whose own local
//! checks all pass (synced, holds the writer role, holds the lock) but
//! that the controller no longer authorizes as writer. Engineering that
//! state for real is deliberately hard: a naive
//! `status.nodeWriterAgreement` patch doesn't hold — the controller's own
//! reconcile loop treats that patch as a fresh watch event and recomputes
//! (thus overwrites) it almost immediately, and `mirrorvol-agent`'s own
//! reconcile loop only relays whatever the controller currently says.
//!
//! This test breaks that race the only clean way available without
//! tampering with a real lock file (which would also trip the *existing*
//! local lock-state check, making it impossible to isolate the new veto
//! from that already-tested path): it briefly scales `mirrorvol-controller`
//! to 0 replicas so nothing undoes an injected `status.nodeWriterAgreement`
//! patch, lets the real agent's own reconcile loop relay that patched value
//! for real, forces a fresh attach, and restores the controller afterward
//! (via a `Drop` guard, so it's restored even on a panic/assertion
//! failure). Because this pauses the *cluster-wide* controller Deployment,
//! it must not run concurrently with any other test that needs it live —
//! it's its own `task test:writer-agreement`, not folded into `test:e2e`.
//!
//! Requires `task deploy:full` plus `task csi:install-test-driver`.

use std::process::Command;
use std::time::Duration;

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod};
use kube::api::{Api, DeleteParams, ListParams, Patch, PatchParams, PostParams};
use mirrorvol_api::{MirroredVolume, MirroredVolumeSpec, StorageSpec, WorkloadRef};
use mirrorvol_itest::{
    candidate_node_names, pod_events, pod_is_running, test_client, unique_name, wait_until,
};

const NAMESPACE: &str = "mirrorvol";
const READY_TIMEOUT: Duration = Duration::from_secs(180);
const POLL_INTERVAL: Duration = Duration::from_secs(2);

fn app_deployment(name: &str) -> Deployment {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": name, "namespace": NAMESPACE },
        "spec": {
            "replicas": 0,
            "selector": { "matchLabels": { "app": name } },
            "template": {
                "metadata": { "labels": { "app": name } },
                "spec": {
                    "containers": [{
                        "name": "app",
                        "image": "busybox:1.36",
                        "command": ["sleep", "infinity"],
                        "volumeMounts": [{ "name": "config", "mountPath": "/config" }],
                    }],
                    "volumes": [{ "name": "config", "emptyDir": {} }],
                },
            },
        },
    }))
    .expect("static Deployment JSON is always valid")
}

async fn app_pod_name(client: &kube::Client, app_name: &str) -> Option<String> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), NAMESPACE);
    let list = pods
        .list(&ListParams::default().labels(&format!("app={app_name}")))
        .await
        .ok()?;
    list.items.into_iter().find_map(|pod| pod.metadata.name)
}

fn kubectl(args: &[&str]) -> bool {
    Command::new("kubectl")
        .args(args)
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

/// Restores `mirrorvol-controller` to 1 replica on drop — even on a panic
/// mid-test, the rest of the suite (and anyone using this cluster
/// afterward) must never be left with a permanently paused controller.
struct ControllerPauseGuard;
impl ControllerPauseGuard {
    fn pause() -> Self {
        assert!(
            kubectl(&[
                "-n",
                NAMESPACE,
                "scale",
                "deployment/mirrorvol-controller",
                "--replicas=0",
            ]),
            "failed to scale mirrorvol-controller to 0"
        );
        assert!(
            kubectl(&[
                "-n",
                NAMESPACE,
                "wait",
                "--for=delete",
                "pod",
                "-l",
                "app.kubernetes.io/name=mirrorvol-controller",
                "--timeout=60s",
            ]),
            "mirrorvol-controller pod never terminated after scale-to-0"
        );
        Self
    }
}
impl Drop for ControllerPauseGuard {
    fn drop(&mut self) {
        // Best-effort synchronous restore — deliberately not spawned like
        // the other Cleanup guards in this suite: leaving the controller
        // paused for even a few extra seconds after this test ends is a
        // materially worse failure mode than blocking Drop briefly here.
        let _ = kubectl(&[
            "-n",
            NAMESPACE,
            "scale",
            "deployment/mirrorvol-controller",
            "--replicas=1",
        ]);
        let _ = kubectl(&[
            "-n",
            NAMESPACE,
            "wait",
            "--for=condition=Available",
            "deployment/mirrorvol-controller",
            "--timeout=60s",
        ]);
    }
}

#[tokio::test]
#[ignore = "requires a fully deployed cluster — `task deploy:full`; pauses the cluster-wide \
            controller, run in isolation via `task test:writer-agreement`, never alongside \
            other tests that need it live"]
async fn a_controller_relayed_contradiction_blocks_a_fresh_attach_despite_healthy_local_state() {
    let client = test_client().await.expect("cluster reachable");
    let mut nodes = candidate_node_names(&client)
        .await
        .expect("list candidate nodes");
    // A MirroredVolume requires at least two candidates at admission,
    // even though this test only ever manipulates the first one's writer
    // agreement — matches full_promotion_cycle.rs's own candidate count.
    nodes.truncate(2);
    assert!(
        nodes.len() >= 2,
        "need at least 2 mirrorvol.io/candidate nodes, found {nodes:?}"
    );
    let node = nodes[0].clone();

    let volume_name = unique_name("wa-config");
    let app_name = unique_name("wa-app");

    let apps = Api::<Deployment>::namespaced(client.clone(), NAMESPACE);
    apps.create(&PostParams::default(), &app_deployment(&app_name))
        .await
        .expect("create app deployment");

    let volumes = Api::<MirroredVolume>::namespaced(client.clone(), NAMESPACE);
    let mv = MirroredVolume::new(
        &volume_name,
        MirroredVolumeSpec {
            workload: WorkloadRef {
                kind: "Deployment".to_owned(),
                name: app_name.clone(),
                volume_name: "config".to_owned(),
                mount_path: "/config".to_owned(),
            },
            storage: StorageSpec {
                storage_class_name: "mirrorvol-syncthing".to_owned(),
                replica_path_template: "/data/{claim}".to_owned(),
                claim_template: serde_json::json!({
                    "accessModes": ["ReadWriteOnce"],
                    "resources": { "requests": { "storage": "64Mi" } },
                }),
                ignore_patterns: vec![],
                warm_sync_interval_seconds: 300,
                writer_agreement_timeout_seconds: 1800,
            },
            candidate_nodes: nodes.clone(),
            desired_active_node: node.clone(),
            operation_timeout_seconds: 300,
            backend: mirrorvol_api::backend::SYNCTHING.to_owned(),
            consistency: mirrorvol_api::Consistency::Strict,
        },
    );
    volumes
        .create(&PostParams::default(), &mv)
        .await
        .expect("create MirroredVolume");

    let cleanup_client = client.clone();
    let cleanup_volume = volume_name.clone();
    let cleanup_app = app_name.clone();
    struct Cleanup {
        client: kube::Client,
        volume_name: String,
        app_name: String,
    }
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let client = self.client.clone();
            let volume_name = self.volume_name.clone();
            let app_name = self.app_name.clone();
            tokio::spawn(async move {
                let _ = Api::<MirroredVolume>::namespaced(client.clone(), NAMESPACE)
                    .delete(&volume_name, &DeleteParams::default())
                    .await;
                let _ = Api::<Deployment>::namespaced(client.clone(), NAMESPACE)
                    .delete(&app_name, &DeleteParams::default())
                    .await;
                let claims = Api::<PersistentVolumeClaim>::namespaced(client, NAMESPACE);
                if let Ok(list) = claims
                    .list(
                        &ListParams::default()
                            .labels(&format!("mirrorvol.io/volume={volume_name}")),
                    )
                    .await
                {
                    for claim in list
                        .items
                        .into_iter()
                        .filter_map(|claim| claim.metadata.name)
                    {
                        let _ = claims.delete(&claim, &DeleteParams::default()).await;
                    }
                }
            });
        }
    }
    let _cleanup = Cleanup {
        client: cleanup_client,
        volume_name: cleanup_volume,
        app_name: cleanup_app,
    };

    // --- Bootstrap with the controller live: real writer, real lock, real
    // attach. Everything this test's veto assertion needs to isolate from.
    let bootstrapped = wait_until(READY_TIMEOUT, POLL_INTERVAL, || {
        let volumes = volumes.clone();
        let volume_name = volume_name.clone();
        let node = node.clone();
        async move {
            volumes
                .get(&volume_name)
                .await
                .ok()
                .and_then(|mv| mv.status)
                .and_then(|status| status.active)
                .is_some_and(|active| active.node == node)
        }
    })
    .await;
    assert!(
        bootstrapped,
        "bootstrap never committed status.active to {node}"
    );

    let running = wait_until(READY_TIMEOUT, POLL_INTERVAL, || {
        let client = client.clone();
        let app_name = app_name.clone();
        async move {
            match app_pod_name(&client, &app_name).await {
                Some(pod) => pod_is_running(&client, NAMESPACE, &pod).await,
                None => false,
            }
        }
    })
    .await;
    assert!(running, "app pod never became Running during bootstrap");

    // Force a genuine unstage: kubelet only calls NodeStageVolume once per
    // node per volume — merely deleting/recreating the *pod* while the PVC
    // stays referenced on this node the whole time does **not** re-trigger
    // it (a later pod reuses the already-staged mount via NodePublishVolume
    // alone, which has no verification logic at all — see node.rs's own
    // doc comment). Scaling the Deployment to 0 and waiting for the pod to
    // fully disappear is what actually gets kubelet to NodeUnstageVolume,
    // so scaling back up later forces a real fresh NodeStageVolume call —
    // the only way to make the veto path run again at all.
    apps.patch(
        &app_name,
        &PatchParams::default(),
        &Patch::Merge(serde_json::json!({ "spec": { "replicas": 0 } })),
    )
    .await
    .expect("scale the app deployment to 0");
    let unstaged = wait_until(READY_TIMEOUT, POLL_INTERVAL, || {
        let client = client.clone();
        let app_name = app_name.clone();
        async move { app_pod_name(&client, &app_name).await.is_none() }
    })
    .await;
    assert!(unstaged, "app pod never disappeared after scaling to 0");
    // Give kubelet's own unstage/unmount a real moment to complete — no
    // event this test can watch for directly confirms it finished.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // --- Pause the controller, then inject a contradiction nothing will
    // now self-heal. Restored on drop even if an assertion below panics.
    let _pause = ControllerPauseGuard::pause();

    // changedAt's exact value doesn't matter to this test — just needs to
    // be a valid RFC3339 instant to satisfy the CRD's structural schema
    // (WriterAgreementEntry::changed_at is a required field, not Option).
    let patch = serde_json::json!({ "status": { "nodeWriterAgreement": {
        node.clone(): {
            "state": mirrorvol_api::writer_agreement::CONTRADICTION,
            "changedAt": "2024-01-01T00:00:00Z",
        }
    }}});
    volumes
        .patch_status(&volume_name, &PatchParams::default(), &Patch::Merge(&patch))
        .await
        .expect("inject a synthetic writer-agreement contradiction");

    // Give the real mirrorvol-agent reconcile loop (5s interval, see
    // main.rs's reconcile) a real chance to relay the patched value into
    // the shared consistency signal file before forcing a fresh attach —
    // not a race to win, just waiting for real, already-running machinery.
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Force the fresh NodeStageVolume call: scale back up, onto the same
    // node (nodeSelector-free here, but this node is the only one holding
    // an already-Bound replica claim's PV binding for this workload).
    apps.patch(
        &app_name,
        &PatchParams::default(),
        &Patch::Merge(serde_json::json!({ "spec": { "replicas": 1 } })),
    )
    .await
    .expect("scale the app deployment back to 1");

    let started = wait_until(Duration::from_secs(45), POLL_INTERVAL, || {
        let client = client.clone();
        let app_name = app_name.clone();
        async move {
            match app_pod_name(&client, &app_name).await {
                Some(pod) => pod_is_running(&client, NAMESPACE, &pod).await,
                None => false,
            }
        }
    })
    .await;
    assert!(
        !started,
        "a fresh app pod became Running despite an injected writer-agreement Contradiction — \
         the controller-relayed veto (verify_local_writer in node.rs) never blocked it"
    );

    let new_pod = app_pod_name(&client, &app_name)
        .await
        .expect("a replacement pod should exist even if not Running");
    let events = pod_events(&client, NAMESPACE, &new_pod).await;
    let saw_veto = events
        .iter()
        .any(|message| message.contains("no grant currently authorizes this node as writer"));
    assert!(
        saw_veto,
        "expected a FailedMount event referencing the writer-agreement veto, got: {events:?}"
    );
}
