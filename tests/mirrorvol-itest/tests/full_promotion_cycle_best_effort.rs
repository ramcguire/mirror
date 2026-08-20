//! `bestEffort` mode's own e2e coverage, against a fully deployed cluster —
//! see `full_promotion_cycle.rs`'s own module doc for `strict`'s coverage
//! and the general shape both suites share. Requires the same `task
//! deploy:full`/`task test:e2e`.
//!
//! Covers what's specific to this mode: a move lands immediately with no
//! `status.operation` ever appearing, and a genuine write conflict produces
//! a `Degraded`/`SyncConflict` condition that never stops the workload and
//! clears once the losing `.sync-conflict-*` file is gone.

use std::process::Command;
use std::time::Duration;

use k8s_openapi::api::apps::v1::Deployment;
use kube::api::{Api, DeleteParams, ListParams, Patch, PatchParams, PostParams};
use mirrorvol_api::{MirroredVolume, MirroredVolumeSpec, StorageSpec, WorkloadRef};
use mirrorvol_itest::{
    candidate_node_names, pod_is_running, syncthing_pod_for_node, test_client, unique_name,
    wait_until,
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

fn exec(pod: &str, container: &str, script: &str) -> bool {
    Command::new("kubectl")
        .args([
            "exec", "-n", NAMESPACE, pod, "-c", container, "--", "sh", "-c", script,
        ])
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

async fn data_path(client: &kube::Client, volume_id: &str, node: &str) -> String {
    let claim_name = mirrorvol_api::naming::replica_claim_name(volume_id, node);
    mirrorvol_itest::resolved_underlying_path(client, NAMESPACE, &claim_name)
        .await
        .unwrap_or_else(|| format!("/data/{claim_name}"))
}

async fn write_file(
    client: &kube::Client,
    pod: &str,
    volume_id: &str,
    node: &str,
    file: &str,
    contents: &str,
) {
    let path = data_path(client, volume_id, node).await;
    assert!(
        exec(
            pod,
            "syncthing",
            &format!("mkdir -p {path} && printf '%s' '{contents}' > {path}/{file}")
        ),
        "writing {file} on {pod} ({path}) failed"
    );
}

async fn conflict_condition(volumes: &Api<MirroredVolume>, name: &str) -> bool {
    volumes
        .get(name)
        .await
        .ok()
        .and_then(|mv| mv.status)
        .is_some_and(|status| {
            status.conditions.iter().any(|condition| {
                condition.type_ == "Degraded" && condition.reason == "SyncConflict"
            })
        })
}

#[tokio::test]
#[ignore = "requires a fully deployed cluster — `task deploy:full`, or just `task test:e2e`"]
async fn best_effort_moves_immediately_and_surfaces_a_resolved_conflict() {
    let client = test_client().await.expect("cluster reachable");
    let mut nodes = candidate_node_names(&client)
        .await
        .expect("list candidate nodes");
    nodes.truncate(2);
    assert!(
        nodes.len() >= 2,
        "need at least 2 mirrorvol.io/candidate nodes, found {nodes:?}"
    );
    let (node_a, node_b) = (nodes[0].clone(), nodes[1].clone());

    let volume_name = unique_name("e2e-be-config");
    let app_name = unique_name("e2e-be-app");

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
            desired_active_node: node_a.clone(),
            operation_timeout_seconds: 300,
            backend: mirrorvol_api::backend::SYNCTHING.to_owned(),
            consistency: mirrorvol_api::Consistency::BestEffort,
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
                let _ = Api::<Deployment>::namespaced(client, NAMESPACE)
                    .delete(&app_name, &DeleteParams::default())
                    .await;
            });
        }
    }
    let _cleanup = Cleanup {
        client: cleanup_client,
        volume_name: cleanup_volume,
        app_name: cleanup_app,
    };

    // --- No operation ever appears, and the workload starts on node_a ---
    let active = wait_until(READY_TIMEOUT, POLL_INTERVAL, || {
        let volumes = volumes.clone();
        let volume_name = volume_name.clone();
        let node_a = node_a.clone();
        async move {
            volumes
                .get(&volume_name)
                .await
                .ok()
                .and_then(|mv| mv.status)
                .is_some_and(|status| {
                    status.operation.is_none()
                        && status.active.is_some_and(|active| active.node == node_a)
                })
        }
    })
    .await;
    assert!(
        active,
        "bestEffort never committed status.active to {node_a} with no operation"
    );

    let pods: Api<k8s_openapi::api::core::v1::Pod> = Api::namespaced(client.clone(), NAMESPACE);
    let app_pod_running = wait_until(READY_TIMEOUT, POLL_INTERVAL, || {
        let client = client.clone();
        let pods = pods.clone();
        let app_name = app_name.clone();
        async move {
            let Ok(list) = pods
                .list(&ListParams::default().labels(&format!("app={app_name}")))
                .await
            else {
                return false;
            };
            for pod in list.items.into_iter().filter_map(|pod| pod.metadata.name) {
                if pod_is_running(&client, NAMESPACE, &pod).await {
                    return true;
                }
            }
            false
        }
    })
    .await;
    assert!(
        app_pod_running,
        "app pod never became Running under bestEffort — attach hook must never gate it"
    );

    // --- A genuine conflict: write different content to the same file on
    // both replicas directly, which is exactly what bestEffort allows.
    let source_pod = syncthing_pod_for_node(&client, NAMESPACE, &node_a)
        .await
        .expect("node_a's syncthing pod");
    let other_pod = syncthing_pod_for_node(&client, NAMESPACE, &node_b)
        .await
        .expect("node_b's syncthing pod");
    write_file(
        &client,
        &source_pod,
        &volume_name,
        &node_a,
        "shared.txt",
        "from-a",
    )
    .await;
    write_file(
        &client,
        &other_pod,
        &volume_name,
        &node_b,
        "shared.txt",
        "from-b",
    )
    .await;

    let flagged = wait_until(READY_TIMEOUT, POLL_INTERVAL, || {
        let volumes = volumes.clone();
        let volume_name = volume_name.clone();
        async move { conflict_condition(&volumes, &volume_name).await }
    })
    .await;
    assert!(
        flagged,
        "a real overlapping write never produced a Degraded/SyncConflict condition"
    );

    // The workload is never stopped by this — same Pod, still Running.
    assert!(
        pods.list(&ListParams::default().labels(&format!("app={app_name}")))
            .await
            .is_ok_and(|list| !list.items.is_empty()),
        "the workload's Pod was removed while only a SyncConflict condition was active"
    );

    // --- Resolve it: delete every sync-conflict-* file on both replicas ---
    let node_a_path = data_path(&client, &volume_name, &node_a).await;
    let node_b_path = data_path(&client, &volume_name, &node_b).await;
    exec(
        &source_pod,
        "syncthing",
        &format!("find {node_a_path} -name '*.sync-conflict-*' -delete"),
    );
    exec(
        &other_pod,
        "syncthing",
        &format!("find {node_b_path} -name '*.sync-conflict-*' -delete"),
    );

    let cleared = wait_until(READY_TIMEOUT, POLL_INTERVAL, || {
        let volumes = volumes.clone();
        let volume_name = volume_name.clone();
        async move { !conflict_condition(&volumes, &volume_name).await }
    })
    .await;
    assert!(
        cleared,
        "the Degraded/SyncConflict condition never cleared after the conflict was resolved"
    );

    // --- Moving stays immediate and gate-free ---
    volumes
        .patch(
            &volume_name,
            &PatchParams::default(),
            &Patch::Merge(serde_json::json!({ "spec": { "desiredActiveNode": node_b } })),
        )
        .await
        .expect("patch desiredActiveNode");
    let moved = wait_until(READY_TIMEOUT, POLL_INTERVAL, || {
        let volumes = volumes.clone();
        let volume_name = volume_name.clone();
        let node_b = node_b.clone();
        async move {
            volumes
                .get(&volume_name)
                .await
                .ok()
                .and_then(|mv| mv.status)
                .and_then(|status| status.active)
                .is_some_and(|active| active.node == node_b)
        }
    })
    .await;
    assert!(moved, "bestEffort move to {node_b} never landed");
}
