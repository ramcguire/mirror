//! Drives a real bootstrap and a real planned move through a *fully
//! deployed* mirrorvol: the real `mirrorvol-controller` Deployment and
//! per-node `mirrorvol-agent`+Syncthing DaemonSet pods, running inside the
//! KinD cluster itself — not `FakeBackend`, not bare processes on the
//! test-runner host.
//!
//! Requires `task deploy:full` plus `task csi:install-test-driver` — `task
//! test:e2e` does this for you, then runs just this file.
//!
//! Covers what a fake backend or mocked Syncthing server can't exercise for
//! real: peers surviving reconciles, standby writes never propagating, a
//! clean move transferring real data including deletes, only the target
//! being writable before the workload restarts, a demoted writer returning
//! as standby, and the CSI attach hook's positive path (this
//! `MirroredVolume`'s replica PVCs route through the real
//! `mirrorvol-syncthing` StorageClass, so the app Pod's `NodeStageVolume`
//! call goes through `mirrorvol-csi` for real — `csi_overlay.rs` only
//! proves the fail-closed half).
//!
//! Not covered here: incomplete replicas/present locks rejecting
//! promotion, which needs a deliberately large, slow write to observe
//! timing and doesn't fit a bounded automated test well.

use std::process::Command;
use std::time::Duration;

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod, Secret};
use kube::api::{Api, DeleteParams, ListParams, Patch, PatchParams, PostParams};
use mirrorvol_api::{MirroredVolume, MirroredVolumeSpec, StorageSpec, WorkloadRef};
use mirrorvol_controller::replica_claim_name;
use mirrorvol_itest::{
    bound_pv_csi_driver, candidate_node_names, pod_events, pod_is_running, syncthing_pod_for_node,
    test_client, unique_name, wait_until, PortForward,
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
                    // the controller repoints this at the winning replica's PVC
                    "volumes": [{ "name": "config", "emptyDir": {} }],
                },
            },
        },
    }))
    .expect("static Deployment JSON is always valid")
}

/// Each node's own agent get-or-creates its own key Secret, so callers must
/// read the specific node's key they intend to query.
async fn fetch_syncthing_api_key(client: &kube::Client, node: &str) -> anyhow::Result<String> {
    let secrets: Api<Secret> = Api::namespaced(client.clone(), NAMESPACE);
    let name = mirrorvol_api::naming::per_node_secret_name(node);
    let secret = secrets.get(&name).await.map_err(|error| {
        anyhow::anyhow!("reading {name} Secret: {error} — run `task deploy:full` first")
    })?;
    let bytes = secret
        .data
        .and_then(|mut data| data.remove("apikey"))
        .ok_or_else(|| anyhow::anyhow!("{name} Secret has no `apikey` key"))?;
    Ok(String::from_utf8(bytes.0)?)
}

async fn folder_type(pod: &str, api_key: &str, volume_id: &str) -> anyhow::Result<String> {
    let forward = PortForward::start(NAMESPACE, pod, 8384).await?;
    let http = reqwest::Client::new();
    let url = format!("{}/rest/config/folders/{volume_id}", forward.base_url());
    let response = http.get(&url).header("X-API-Key", api_key).send().await?;
    anyhow::ensure!(
        response.status().is_success(),
        "GET {url}: {}",
        response.status()
    );
    let json: serde_json::Value = response.json().await?;
    Ok(json["type"].as_str().unwrap_or_default().to_owned())
}

async fn assert_folder_role(
    pod: &str,
    api_key: &str,
    volume_id: &str,
    expected: &str,
    label: &str,
) {
    let ready = wait_until(READY_TIMEOUT, POLL_INTERVAL, || {
        let pod = pod.to_owned();
        let api_key = api_key.to_owned();
        let volume_id = volume_id.to_owned();
        let expected = expected.to_owned();
        async move {
            folder_type(&pod, &api_key, &volume_id)
                .await
                .is_ok_and(|folder_type| folder_type == expected)
        }
    })
    .await;
    assert!(ready, "{label}: folder never became {expected}");
}

/// The real directory this candidate's Syncthing folder is using —
/// `mirrorvol.io/resolvedPath` off the bound PV when `mirrorvol-csi`
/// resolved one, falling back to `replica_path_template` otherwise. Using
/// the wrong path wouldn't fail loudly, it would silently read/write an
/// empty directory nobody's actually syncing.
async fn data_path(client: &kube::Client, volume_id: &str, node: &str) -> String {
    let claim_name = mirrorvol_api::naming::replica_claim_name(volume_id, node);
    mirrorvol_itest::resolved_underlying_path(client, NAMESPACE, &claim_name)
        .await
        .unwrap_or_else(|| format!("/data/{claim_name}"))
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

async fn delete_file(client: &kube::Client, pod: &str, volume_id: &str, node: &str, file: &str) {
    let path = data_path(client, volume_id, node).await;
    assert!(
        exec(pod, "syncthing", &format!("rm -f {path}/{file}")),
        "deleting {file} on {pod} ({path}) failed"
    );
}

async fn file_exists(
    client: &kube::Client,
    pod: &str,
    volume_id: &str,
    node: &str,
    file: &str,
) -> bool {
    let path = data_path(client, volume_id, node).await;
    exec(pod, "syncthing", &format!("test -f {path}/{file}"))
}

/// The workload Pod a Deployment currently owns — looked up by label since
/// the controller (`KubeAppScaler`) manages this Deployment's
/// scale/selector/claim, not this test, so the actual Pod name is whatever
/// the ReplicaSet controller generated.
async fn app_pod_name(client: &kube::Client, app_name: &str) -> Option<String> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), NAMESPACE);
    let list = pods
        .list(&ListParams::default().labels(&format!("app={app_name}")))
        .await
        .ok()?;
    list.items.into_iter().find_map(|pod| pod.metadata.name)
}

/// Proves the CSI attach hook's positive path for real: the app Pod using
/// `claim_name` only reaches `Running` once kubelet's `NodeStageVolume`
/// call — delegated through `mirrorvol-csi`, then gated by
/// `node.rs::attach_syncthing`'s live completion/writer check — actually
/// succeeds. The bound PV's CSI driver name is checked too, so this can't
/// pass by accident via some other provisioning path.
async fn assert_app_pod_running_through_mirrorvol_csi(
    client: &kube::Client,
    app_name: &str,
    claim_name: &str,
    label: &str,
) {
    let running = wait_until(READY_TIMEOUT, POLL_INTERVAL, || {
        let client = client.clone();
        let app_name = app_name.to_owned();
        async move {
            match app_pod_name(&client, &app_name).await {
                Some(pod) => pod_is_running(&client, NAMESPACE, &pod).await,
                None => false,
            }
        }
    })
    .await;
    if !running {
        let events = match app_pod_name(client, app_name).await {
            Some(pod) => pod_events(client, NAMESPACE, &pod).await,
            None => vec!["no pod found for this app label yet".to_owned()],
        };
        panic!(
            "{label}: app pod for {app_name} never became Running — mirrorvol-csi's \
             NodeStageVolume/Syncthing readiness gate never unblocked it. Recent events: {events:?}"
        );
    }
    let driver = bound_pv_csi_driver(client, NAMESPACE, claim_name).await;
    assert_eq!(
        driver.as_deref(),
        Some("mirrorvol.csi.homelab.internal"),
        "{label}: replica claim {claim_name} wasn't actually provisioned through mirrorvol-csi \
         (bound PV's CSI driver was {driver:?}) — this test needs the mirrorvol-syncthing \
         StorageClass, not a passthrough one"
    );
}

#[tokio::test]
#[ignore = "requires a fully deployed cluster — `task deploy:full`, or just `task test:e2e`"]
async fn a_planned_move_promotes_the_target_and_demotes_the_source() {
    let client = test_client().await.expect("cluster reachable");
    let mut nodes = candidate_node_names(&client)
        .await
        .expect("list candidate nodes");
    nodes.truncate(2); // only need two candidates for this cycle
    assert!(
        nodes.len() >= 2,
        "need at least 2 mirrorvol.io/candidate nodes, found {nodes:?}"
    );
    let (source_node, target_node) = (nodes[0].clone(), nodes[1].clone());

    let source_api_key = fetch_syncthing_api_key(&client, &source_node)
        .await
        .expect("read source node's syncthing api key");
    let target_api_key = fetch_syncthing_api_key(&client, &target_node)
        .await
        .expect("read target node's syncthing api key");

    let volume_name = unique_name("e2e-config");
    let app_name = unique_name("e2e-app");

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
                // The real mirrorvol-csi StorageClass, not KinD's bundled
                // `standard` class — proves the attach-hook positive path,
                // not just the promotion protocol.
                storage_class_name: "mirrorvol-syncthing".to_owned(),
                replica_path_template: "/data/{claim}".to_owned(),
                claim_template: serde_json::json!({
                    "accessModes": ["ReadWriteOnce"],
                    "resources": { "requests": { "storage": "64Mi" } },
                }),
                pull_only: false,
                ignore_patterns: vec![],
            },
            candidate_nodes: nodes.clone(),
            desired_active_node: source_node.clone(),
            operation_timeout_seconds: 300,
            backend: mirrorvol_api::backend::SYNCTHING.to_owned(),
            // This suite exercises the full gated lock/epoch protocol —
            // see full_promotion_cycle_best_effort.rs for bestEffort's own
            // coverage.
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

    // --- Bootstrap: no source, target acquires epoch 1 and becomes writer ---
    let bootstrapped = wait_until(READY_TIMEOUT, POLL_INTERVAL, || {
        let volumes = volumes.clone();
        let volume_name = volume_name.clone();
        let source_node = source_node.clone();
        async move {
            volumes
                .get(&volume_name)
                .await
                .ok()
                .and_then(|mv| mv.status)
                .and_then(|status| status.active)
                .is_some_and(|active| active.node == source_node)
        }
    })
    .await;
    assert!(
        bootstrapped,
        "bootstrap never committed status.active to {source_node}"
    );

    // Attach-hook positive path, first instance: the bootstrap writer's own
    // Pod only starts once its real NodeStageVolume call sees the folder
    // already driven to ready+writer.
    assert_app_pod_running_through_mirrorvol_csi(
        &client,
        &app_name,
        &replica_claim_name(&volume_name, &source_node),
        "post-bootstrap",
    )
    .await;

    let source_pod = syncthing_pod_for_node(&client, NAMESPACE, &source_node)
        .await
        .expect("source node's syncthing pod");
    let target_pod = syncthing_pod_for_node(&client, NAMESPACE, &target_node)
        .await
        .expect("target node's syncthing pod");

    assert_folder_role(
        &source_pod,
        &source_api_key,
        &volume_name,
        "sendreceive",
        "post-bootstrap source",
    )
    .await;
    assert_folder_role(
        &target_pod,
        &target_api_key,
        &volume_name,
        "receiveonly",
        "post-bootstrap target",
    )
    .await;

    // --- Planned move: source -> target ---
    volumes
        .patch(
            &volume_name,
            &PatchParams::default(),
            &Patch::Merge(serde_json::json!({ "spec": { "desiredActiveNode": target_node } })),
        )
        .await
        .expect("patch desiredActiveNode");

    let promoted = wait_until(READY_TIMEOUT, POLL_INTERVAL, || {
        let volumes = volumes.clone();
        let volume_name = volume_name.clone();
        let target_node = target_node.clone();
        async move {
            volumes
                .get(&volume_name)
                .await
                .ok()
                .and_then(|mv| mv.status)
                .and_then(|status| status.active)
                .is_some_and(|active| active.node == target_node)
        }
    })
    .await;
    assert!(
        promoted,
        "planned move never committed status.active to {target_node}"
    );

    // Attach-hook positive path, second instance — for a promoted target,
    // not a bootstrap writer, proving the gate keeps unblocking across a
    // real writer handoff.
    assert_app_pod_running_through_mirrorvol_csi(
        &client,
        &app_name,
        &replica_claim_name(&volume_name, &target_node),
        "post-move",
    )
    .await;

    // The target is the only writable folder once the move lands.
    assert_folder_role(
        &target_pod,
        &target_api_key,
        &volume_name,
        "sendreceive",
        "post-move target",
    )
    .await;
    // The former writer returns as standby, not a persisted role.
    assert_folder_role(
        &source_pod,
        &source_api_key,
        &volume_name,
        "receiveonly",
        "post-move source",
    )
    .await;

    // A clean move transfers real data, including deletes.
    write_file(
        &client,
        &target_pod,
        &volume_name,
        &target_node,
        "hello.txt",
        "e2e",
    )
    .await;
    let propagated = wait_until(READY_TIMEOUT, POLL_INTERVAL, || {
        let client = client.clone();
        let source_pod = source_pod.clone();
        let volume_name = volume_name.clone();
        let source_node = source_node.clone();
        async move {
            file_exists(
                &client,
                &source_pod,
                &volume_name,
                &source_node,
                "hello.txt",
            )
            .await
        }
    })
    .await;
    assert!(
        propagated,
        "a write on the new writer never reached the new standby"
    );

    delete_file(
        &client,
        &target_pod,
        &volume_name,
        &target_node,
        "hello.txt",
    )
    .await;
    let delete_propagated = wait_until(READY_TIMEOUT, POLL_INTERVAL, || {
        let client = client.clone();
        let source_pod = source_pod.clone();
        let volume_name = volume_name.clone();
        let source_node = source_node.clone();
        async move {
            !file_exists(
                &client,
                &source_pod,
                &volume_name,
                &source_node,
                "hello.txt",
            )
            .await
        }
    })
    .await;
    assert!(
        delete_propagated,
        "a delete on the new writer never reached the new standby"
    );

    // A receive-only standby never lets a local write escape to the writer.
    write_file(
        &client,
        &source_pod,
        &volume_name,
        &source_node,
        "sneaky.txt",
        "should never propagate",
    )
    .await;
    tokio::time::sleep(Duration::from_secs(10)).await; // give Syncthing every chance to (wrongly) sync it
    assert!(
        !file_exists(
            &client,
            &target_pod,
            &volume_name,
            &target_node,
            "sneaky.txt"
        )
        .await,
        "a standby's local write reached the writer"
    );

    // A file written through the workload's actual CSI mount (not this
    // test's exec-into-syncthing shortcut) must be visible through
    // Syncthing's sync root — mirrorvol.io/resolvedPath aliasing, not just
    // an unblocked readiness gate.
    let app_pod = app_pod_name(&client, &app_name)
        .await
        .expect("app pod exists after the move");
    assert!(
        exec(&app_pod, "app", "echo -n aliased > /config/aliased.txt"),
        "writing through the workload's real CSI mount failed"
    );
    let aliased = wait_until(READY_TIMEOUT, POLL_INTERVAL, || {
        let client = client.clone();
        let target_pod = target_pod.clone();
        let volume_name = volume_name.clone();
        let target_node = target_node.clone();
        async move {
            file_exists(
                &client,
                &target_pod,
                &volume_name,
                &target_node,
                "aliased.txt",
            )
            .await
        }
    })
    .await;
    assert!(
        aliased,
        "a file written through the workload's real CSI mount ({app_pod}:/config) never showed \
         up in Syncthing's own sync root — mirrorvol.io/resolvedPath aliasing isn't working"
    );
}
