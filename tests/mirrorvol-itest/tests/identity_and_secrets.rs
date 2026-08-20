//! Proves the rsync secret ownership fix against a real API server: the
//! shared rsync module secret is owned by the
//! `mirrorvol-controller` `Deployment`, not an arbitrary node, and each
//! candidate's rsync identity token is owned by that node's own `Node`.
//!
//! Both require a cluster with the `rsync` backend actually deployed
//! (`deploy/syncthing/daemonset.yaml`'s `rsyncd`/`stunnel` containers
//! running and having provisioned their secrets at least once) — the same
//! "fully deployed cluster" assumption `csi_overlay.rs`'s ignored tests
//! already make, not a new one introduced here.
//!
//! Deliberately does **not** delete a real `Node` to prove the secret
//! survives that: no other test in this suite deletes a cluster `Node`,
//! and doing so
//! here would be a materially bigger blast radius than the ownership
//! assertion actually needs to make its point (a correct owner reference
//! *is* why the deletion wouldn't cascade).

use k8s_openapi::api::core::v1::Secret;
use kube::api::Api;
use mirrorvol_itest::{candidate_node_names, test_client};

const NAMESPACE: &str = "mirrorvol";

#[tokio::test]
#[ignore = "requires a running cluster with the rsync backend deployed"]
async fn rsync_shared_secret_is_owned_by_the_controller_deployment() {
    let client = test_client().await.expect("cluster reachable");
    let secrets: Api<Secret> = Api::namespaced(client, NAMESPACE);
    let secret = secrets
        .get(mirrorvol_api::naming::RSYNC_SHARED_SECRET_NAME)
        .await
        .expect("rsync shared secret exists — deploy the rsync backend first");

    let owners = secret.metadata.owner_references.unwrap_or_default();
    assert_eq!(
        owners.len(),
        1,
        "expected exactly one owner reference, got {owners:?}"
    );
    let owner = &owners[0];
    assert_eq!(owner.kind, "Deployment");
    assert_eq!(
        owner.name,
        mirrorvol_api::naming::CONTROLLER_DEPLOYMENT_NAME
    );
}

#[tokio::test]
#[ignore = "requires a running cluster with the rsync backend deployed"]
async fn rsync_identity_secret_is_owned_by_its_node() {
    let client = test_client().await.expect("cluster reachable");
    let nodes = candidate_node_names(&client)
        .await
        .expect("list candidate nodes");
    assert!(!nodes.is_empty(), "expected at least one candidate node");

    let secrets: Api<Secret> = Api::namespaced(client, NAMESPACE);
    for node in nodes {
        let name = mirrorvol_api::naming::per_node_rsync_identity_secret_name(&node);
        let secret = secrets.get(&name).await.unwrap_or_else(|error| {
            panic!(
                "rsync identity secret {name} for node {node} should exist — \
                 deploy the rsync backend first ({error})"
            )
        });
        let owners = secret.metadata.owner_references.unwrap_or_default();
        assert_eq!(
            owners.len(),
            1,
            "node {node}: expected exactly one owner reference, got {owners:?}"
        );
        let owner = &owners[0];
        assert_eq!(owner.kind, "Node");
        assert_eq!(owner.name, node);
    }
}
