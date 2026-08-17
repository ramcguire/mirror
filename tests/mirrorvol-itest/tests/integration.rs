//! Kubernetes API integration checks for the current CRD contract.

use kube::api::{Api, Patch, PatchParams, PostParams};
use mirrorvol_api::{MirroredVolume, NodeSyncStatus};
use mirrorvol_itest::{fixtures, test_client, TestNamespace};

#[tokio::test]
#[ignore = "requires a running cluster"]
async fn mirrored_volume_status_ownership_round_trips() {
    let client = test_client().await.expect("cluster reachable");
    let namespace = TestNamespace::create(client.clone(), "itest-crd")
        .await
        .expect("create namespace");
    let volumes = Api::<MirroredVolume>::namespaced(client, &namespace.name);
    volumes
        .create(
            &PostParams::default(),
            &fixtures::mirrored_volume("example-config", &["node-a", "node-b"], "node-a"),
        )
        .await
        .expect("create volume");

    let patch = serde_json::json!({ "status": { "nodes": {
        "node-b": NodeSyncStatus {
            role: "Standby".to_owned(),
            ready: true,
            lock_absent: true,
            ..Default::default()
        }
    }}});
    volumes
        .patch_status(
            "example-config",
            &PatchParams::apply("mirrorvol-agent-node-b"),
            &Patch::Merge(&patch),
        )
        .await
        .expect("agent status patch");
    let volume = volumes.get("example-config").await.expect("get volume");
    assert!(volume
        .status
        .expect("status")
        .nodes
        .get("node-b")
        .is_some_and(|node| node.ready));
}

#[tokio::test]
#[ignore = "requires a running cluster"]
async fn operation_timeout_seconds_defaults_on_the_real_api_server() {
    let client = test_client().await.expect("cluster reachable");
    let namespace = TestNamespace::create(client.clone(), "itest-timeout")
        .await
        .expect("create namespace");
    let volumes = Api::<MirroredVolume>::namespaced(client, &namespace.name);

    // Send raw JSON with `operationTimeoutSeconds` omitted entirely — this
    // proves the CRD's own structural-schema default (900) is applied by
    // the real API server, not just mirrorvol-api's client-side serde
    // default. A unit test against a fake client can't tell those apart;
    // only a real API server actually runs CRD defaulting.
    let mut raw = serde_json::to_value(fixtures::mirrored_volume(
        "example-config",
        &["node-a", "node-b"],
        "node-a",
    ))
    .expect("serialize fixture");
    raw["spec"]
        .as_object_mut()
        .expect("spec object")
        .remove("operationTimeoutSeconds");
    let defaulted: MirroredVolume = volumes
        .create(
            &PostParams::default(),
            &serde_json::from_value(raw).expect("valid MirroredVolume without an explicit timeout"),
        )
        .await
        .expect("create volume without an explicit timeout");
    assert_eq!(defaulted.spec.operation_timeout_seconds, 900);

    // A caller-supplied value round-trips unchanged rather than being
    // overridden by the default.
    let mut custom =
        fixtures::mirrored_volume("example-config-custom", &["node-a", "node-b"], "node-a");
    custom.spec.operation_timeout_seconds = 120;
    let custom = volumes
        .create(&PostParams::default(), &custom)
        .await
        .expect("create volume with a custom timeout");
    assert_eq!(custom.spec.operation_timeout_seconds, 120);
}
