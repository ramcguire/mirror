//! Confirms the *deployed* RBAC (`deploy/controller/`, `deploy/agent/`) —
//! not just what the YAML claims — actually grants the verbs each process
//! uses and withholds `update`/`delete`, via a real
//! `SelfSubjectAccessReview` submitted as each `ServiceAccount`
//! (`kubectl create --as=...`). A YAML review can't catch a typo'd verb or
//! a `ClusterRoleBinding` pointing at the wrong `ServiceAccount`; this can.
//!
//! Deliberately not `kubectl auth can-i <verb> <resource>/<subresource>`:
//! that command's own subresource parsing is unreliable for custom
//! resources on some kubectl versions. A raw `SelfSubjectAccessReview`
//! with a structured `resourceAttributes.subresource` field doesn't share
//! that ambiguity.
//!
//! Requires `deploy/`'s namespace/serviceaccount/clusterrole/
//! clusterrolebinding objects already applied — `task rbac:apply`, which
//! `task test:integration` runs for you.

use std::io::Write;
use std::process::{Command, Stdio};

fn can_i(service_account: &str, verb: &str, resource: &str, subresource: Option<&str>) -> bool {
    can_i_in_group(
        service_account,
        verb,
        "homelab.internal",
        resource,
        subresource,
    )
}

fn can_i_in_group(
    service_account: &str,
    verb: &str,
    group: &str,
    resource: &str,
    subresource: Option<&str>,
) -> bool {
    let mut resource_attributes = serde_json::json!({
        "namespace": "mirrorvol",
        "verb": verb,
        "group": group,
        "resource": resource,
    });
    if let Some(subresource) = subresource {
        resource_attributes["subresource"] = serde_json::Value::String(subresource.to_owned());
    }
    let review = serde_json::json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SelfSubjectAccessReview",
        "spec": { "resourceAttributes": resource_attributes },
    });

    let mut child = Command::new("kubectl")
        .args([
            "create",
            "--as",
            &format!("system:serviceaccount:mirrorvol:{service_account}"),
            // Impersonation makes kubectl's own client-side schema
            // validation try (and fail) to list CRDs as the impersonated,
            // deliberately unprivileged identity — irrelevant to the
            // question this review is actually asking.
            "--validate=false",
            "-o",
            "json",
            "-f",
            "-",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawning `kubectl create` — is kubectl on PATH and KUBECONFIG set?");
    child
        .stdin
        .take()
        .expect("piped stdin")
        .write_all(review.to_string().as_bytes())
        .expect("writing SelfSubjectAccessReview to kubectl's stdin");
    let output = child
        .wait_with_output()
        .expect("waiting for kubectl create");
    let response: serde_json::Value =
        serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
            panic!(
                "parsing SelfSubjectAccessReview response for {service_account} {verb} \
             {resource}{}: {error}\nstdout: {}\nstderr: {}",
                subresource.map(|s| format!("/{s}")).unwrap_or_default(),
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            )
        });
    response["status"]["allowed"].as_bool().unwrap_or(false)
}

#[test]
#[ignore = "requires a running cluster with deploy/'s RBAC applied (task rbac:apply)"]
fn controller_can_patch_status_but_not_update_it() {
    assert!(
        can_i(
            "mirrorvol-controller",
            "patch",
            "mirroredvolumes",
            Some("status")
        ),
        "controller must be able to patch status"
    );
    assert!(
        !can_i(
            "mirrorvol-controller",
            "update",
            "mirroredvolumes",
            Some("status")
        ),
        "controller never calls update/replace — RBAC should not grant it"
    );
}

#[test]
#[ignore = "requires a running cluster with deploy/'s RBAC applied (task rbac:apply)"]
fn controller_reads_but_never_writes_backendnodes() {
    // Read-only: BackendNode is owned exclusively by each node's own agent.
    assert!(can_i("mirrorvol-controller", "get", "backendnodes", None));
    assert!(!can_i(
        "mirrorvol-controller",
        "patch",
        "backendnodes",
        None
    ));
    assert!(!can_i(
        "mirrorvol-controller",
        "create",
        "backendnodes",
        None
    ));
}

#[test]
#[ignore = "requires a running cluster with deploy/'s RBAC applied (task rbac:apply)"]
fn agent_can_patch_but_not_update_backendnode_status() {
    assert!(can_i(
        "mirrorvol-agent",
        "patch",
        "backendnodes",
        Some("status")
    ));
    assert!(!can_i(
        "mirrorvol-agent",
        "update",
        "backendnodes",
        Some("status")
    ));
}

#[test]
#[ignore = "requires a running cluster with deploy/'s RBAC applied (task rbac:apply)"]
fn agent_can_register_backendnodes_but_not_delete_them() {
    assert!(can_i("mirrorvol-agent", "create", "backendnodes", None));
    assert!(can_i("mirrorvol-agent", "patch", "backendnodes", None));
    assert!(!can_i("mirrorvol-agent", "delete", "backendnodes", None));
}

#[test]
#[ignore = "requires a running cluster with deploy/'s RBAC applied (task rbac:apply)"]
fn agent_can_patch_but_not_update_mirroredvolume_status() {
    assert!(can_i(
        "mirrorvol-agent",
        "patch",
        "mirroredvolumes",
        Some("status")
    ));
    assert!(!can_i(
        "mirrorvol-agent",
        "update",
        "mirroredvolumes",
        Some("status")
    ));
}

#[test]
#[ignore = "requires a running cluster with deploy/'s RBAC applied (task rbac:apply)"]
fn agent_can_provision_its_own_secret_and_service_but_not_delete_them() {
    // Per-node API key Secret and per-node Service, both get-or-created by
    // convention, never deleted.
    for resource in ["secrets", "services"] {
        assert!(
            can_i_in_group("mirrorvol-agent", "create", "", resource, None),
            "agent must be able to create its own {resource}"
        );
        assert!(
            can_i_in_group("mirrorvol-agent", "patch", "", resource, None),
            "agent must be able to patch its own {resource}"
        );
        assert!(
            !can_i_in_group("mirrorvol-agent", "delete", "", resource, None),
            "agent should never delete {resource} — RBAC should not grant it"
        );
    }
}

#[test]
#[ignore = "requires a running cluster with deploy/'s RBAC applied (task rbac:apply)"]
fn agent_can_patch_pods_for_self_labeling_but_not_delete_them() {
    assert!(
        can_i_in_group("mirrorvol-agent", "patch", "", "pods", None),
        "agent must be able to patch its own Pod to add mirrorvol.io/node"
    );
    assert!(
        !can_i_in_group("mirrorvol-agent", "delete", "", "pods", None),
        "agent should never delete pods — RBAC should not grant it"
    );
}

#[test]
#[ignore = "requires a running cluster with deploy/'s RBAC applied (task rbac:apply)"]
fn agent_can_read_nodes_proxy_for_quiescence_but_nothing_else_on_nodes() {
    // A real, sensitive grant (reaches any node's kubelet read endpoints) —
    // asserts both that it exists and that it's scoped to `get` only.
    assert!(
        can_i_in_group("mirrorvol-agent", "get", "", "nodes", Some("proxy")),
        "agent must be able to query nodes/proxy for kubelet-backed quiescence proof"
    );
    assert!(
        !can_i_in_group("mirrorvol-agent", "update", "", "nodes", Some("proxy")),
        "agent should never write nodes/proxy — RBAC should not grant it"
    );
}
