//! Shared harness for integration tests that exercise mirrorvol's code
//! against a real Kubernetes API server (a local KinD cluster).
//!
//! What belongs here vs. in the unit tests already scattered across
//! `crates/*`: anything that depends on real API-server behavior a fake
//! can't reproduce; e.g. CRD schema acceptance, status-subresource semantics,
//! JSON-merge-patch behavior on a shared map key, and watch/list
//! propagation timing.
//!
//! Every test using this harness must be `#[ignore]`d (see `tests/`) so
//! `cargo test --workspace` remains fast.

use std::io::{BufRead, BufReader};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Context;
use k8s_openapi::api::core::v1::{
    Event, Namespace, Node, PersistentVolume, PersistentVolumeClaim, Pod,
};
use kube::api::{Api, DeleteParams, ListParams, PostParams};
use kube::Client;

/// Connects using the ambient kubeconfig/context (whatever `kubectl` would
/// use) and fails with a message pointing at the Taskfile rather than a raw
/// connection error, since "no cluster running" is the overwhelmingly likely
/// cause here.
pub async fn test_client() -> anyhow::Result<Client> {
    Client::try_default().await.context(
        "couldn't reach a Kubernetes cluster. Run `task cluster:up` (and `task crds:apply`) \
         first, or `task test:integration` to do both plus run this suite",
    )
}

/// Monotonic-enough per-process suffix so parallel tests never collide on a
/// name, without pulling in a UUID dependency for it.
pub fn unique_name(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{}-{n}", std::process::id())
}

/// A throwaway namespace, deleted best-effort when dropped. Each test that
/// creates namespaced objects
/// ([`MirroredVolume`](mirrorvol_api::MirroredVolume)/[`BackendNode`](mirrorvol_api::BackendNode)
/// are both namespaced) should get its own, so tests can run concurrently
/// and a failed test's leftovers don't confuse the next run — `kind delete
/// cluster`/`task cluster:down` is the real cleanup backstop either way.
pub struct TestNamespace {
    client: Client,
    pub name: String,
}

impl TestNamespace {
    pub async fn create(client: Client, prefix: &str) -> anyhow::Result<Self> {
        let name = unique_name(prefix);
        let api: Api<Namespace> = Api::all(client.clone());
        let ns: Namespace = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": { "name": name },
        }))?;
        api.create(&PostParams::default(), &ns)
            .await
            .with_context(|| format!("creating namespace {name}"))?;
        Ok(Self { client, name })
    }
}

impl Drop for TestNamespace {
    fn drop(&mut self) {
        // Best-effort, fire-and-forget: Drop can't be async, and a failed
        // cleanup here shouldn't fail the test that already ran. Requires
        // an active Tokio runtime.
        let api: Api<Namespace> = Api::all(self.client.clone());
        let name = self.name.clone();
        tokio::spawn(async move {
            let _ = api.delete(&name, &DeleteParams::default()).await;
        });
    }
}

/// Reusable CRD fixtures — tests build on these instead of hand-rolling
/// JSON per test.
pub mod fixtures {
    use k8s_openapi::api::apps::v1::Deployment;
    use mirrorvol_api::{
        BackendNode, BackendNodeSpec, BackendNodeStatus, MirroredVolume, MirroredVolumeSpec,
        StorageSpec, WorkloadRef,
    };

    pub fn mirrored_volume(
        name: &str,
        candidate_nodes: &[&str],
        active_node: &str,
    ) -> MirroredVolume {
        MirroredVolume::new(
            name,
            MirroredVolumeSpec {
                workload: WorkloadRef {
                    kind: "Deployment".to_string(),
                    name: "example-app".to_string(),
                    volume_name: "config".to_string(),
                    mount_path: "/config".to_string(),
                },
                storage: StorageSpec {
                    storage_class_name: "local-path".to_string(),
                    replica_path_template: "/data/{claim}".to_string(),
                    // A real, valid PVC spec.
                    claim_template: serde_json::json!({
                        "accessModes": ["ReadWriteOnce"],
                        "resources": { "requests": { "storage": "64Mi" } },
                    }),
                    pull_only: false,
                    ignore_patterns: vec![],
                },
                candidate_nodes: candidate_nodes.iter().map(|s| s.to_string()).collect(),
                desired_active_node: active_node.to_string(),
                operation_timeout_seconds: 900,
                backend: mirrorvol_api::backend::SYNCTHING.to_string(),
                consistency: mirrorvol_api::Consistency::Strict,
            },
        )
    }

    pub fn backend_node(name: &str, node: &str, backend: &str) -> BackendNode {
        BackendNode::new(
            name,
            BackendNodeSpec {
                node: node.to_string(),
                backend: backend.to_string(),
            },
        )
    }

    pub fn healthy_status(detail: serde_json::Value) -> BackendNodeStatus {
        BackendNodeStatus {
            healthy: true,
            device_id: Some("DEVICE-TEST".to_string()),
            identity_generation: 1,
            device_cert_source: Some("self-managed".to_string()),
            detail,
        }
    }

    /// A minimal, real app Deployment
    pub fn app_deployment(name: &str, namespace: &str) -> Deployment {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": { "name": name, "namespace": namespace },
            "spec": {
                "replicas": 0,
                "selector": { "matchLabels": { "app": name } },
                "template": {
                    "metadata": { "labels": { "app": name } },
                    "spec": {
                        "containers": [{
                            "name": name,
                            "image": "busybox:1.36",
                            "command": ["sleep", "3600"],
                        }],
                    },
                },
            },
        }))
        .expect("static Deployment JSON is always valid")
    }
}

/// A real Syncthing instance, run via `docker run`.
pub struct SyncthingContainer {
    name: String,
    pub base_url: String,
    pub api_key: String,
}

impl SyncthingContainer {
    pub async fn start() -> anyhow::Result<Self> {
        let name = unique_name("itest-syncthing");
        let api_key = unique_name("api-key");

        let status = std::process::Command::new("docker")
            .args([
                "run",
                "-d",
                "--rm",
                "--name",
                &name,
                "-e",
                &format!("STGUIAPIKEY={api_key}"),
                "-e",
                "STGUIADDRESS=0.0.0.0:8384",
                "-e",
                "STNOUPGRADE=1",
                "-p",
                "127.0.0.1::8384",
                "syncthing/syncthing:latest",
            ])
            .status()
            .context("running `docker run` for a test Syncthing instance — is Docker running?")?;
        anyhow::ensure!(
            status.success(),
            "docker run failed for the test Syncthing instance"
        );

        let port = Self::published_port(&name)?;
        let container = Self {
            name,
            base_url: format!("http://127.0.0.1:{port}"),
            api_key,
        };

        // The container being "started" and Syncthing being ready to serve
        // requests are different moments. Wait for the REST API to
        // actually answer before handing this back to the caller.
        let http = reqwest::Client::new();
        let base_url = container.base_url.clone();
        let api_key = container.api_key.clone();
        let ready = wait_until(Duration::from_secs(30), Duration::from_millis(300), || {
            let http = http.clone();
            let base_url = base_url.clone();
            let api_key = api_key.clone();
            async move {
                http.get(format!("{base_url}/rest/system/status"))
                    .header("X-API-Key", api_key)
                    .send()
                    .await
                    .map(|r| r.status().is_success())
                    .unwrap_or(false)
            }
        })
        .await;
        anyhow::ensure!(
            ready,
            "test Syncthing instance never became reachable at {}",
            container.base_url
        );

        Ok(container)
    }

    fn published_port(name: &str) -> anyhow::Result<u16> {
        let output = std::process::Command::new("docker")
            .args(["port", name, "8384/tcp"])
            .output()
            .context("running `docker port`")?;
        anyhow::ensure!(
            output.status.success(),
            "docker port failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout)?;
        // Expected shape: "0.0.0.0:54321\n"
        text.trim()
            .rsplit(':')
            .next()
            .and_then(|p| p.parse::<u16>().ok())
            .ok_or_else(|| anyhow::anyhow!("couldn't parse `docker port` output: {text:?}"))
    }
}

impl Drop for SyncthingContainer {
    fn drop(&mut self) {
        // Drop can't be async, and `docker rm -f` is fast enough
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.name])
            .output();
    }
}

/// Polls `check` every `interval` up to `timeout`, for assertions against
/// eventually-consistent state (watch propagation, status subresource
/// visibility after a patch) where a single immediate read would be a race,
/// not a real assertion of what the system does.
pub async fn wait_until<F, Fut>(
    timeout: std::time::Duration,
    interval: std::time::Duration,
    mut check: F,
) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    loop {
        if check().await {
            return true;
        }
        if start.elapsed() >= timeout {
            return false;
        }
        tokio::time::sleep(interval).await;
    }
}

/// Re-exported so test files don't need their own `mirrorvol_api` dependency
/// declaration just to name common types.
pub use mirrorvol_api as api;

/// Real candidate node names from the live cluster (not hardcoded — KinD's
/// are `<cluster>-worker[N]`). Sorted so callers get a stable pick of
/// "first"/"second" candidate across runs.
pub async fn candidate_node_names(client: &Client) -> anyhow::Result<Vec<String>> {
    let nodes: Api<Node> = Api::all(client.clone());
    let list = nodes
        .list(&ListParams::default().labels(mirrorvol_api::naming::CANDIDATE_LABEL))
        .await
        .context("listing candidate nodes")?;
    let mut names: Vec<String> = list
        .items
        .into_iter()
        .filter_map(|node| node.metadata.name)
        .collect();
    names.sort();
    Ok(names)
}

/// The `mirrorvol-syncthing` DaemonSet Pod actually scheduled onto `node`
/// for tests that need to reach that specific node's own Syncthing instance
/// (via [`PortForward`]) or `kubectl exec` into its data directory.
pub async fn syncthing_pod_for_node(
    client: &Client,
    namespace: &str,
    node: &str,
) -> anyhow::Result<String> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let list = pods
        .list(
            &ListParams::default()
                .labels(&format!(
                    "app.kubernetes.io/name={}",
                    mirrorvol_api::naming::SYNCTHING_POD_APP_NAME
                ))
                .fields(&format!("spec.nodeName={node}")),
        )
        .await
        .with_context(|| format!("listing mirrorvol-syncthing pods on node {node}"))?;
    list.items
        .into_iter()
        .find_map(|pod| pod.metadata.name)
        .ok_or_else(|| anyhow::anyhow!("no mirrorvol-syncthing pod found on node {node}"))
}

/// A `kubectl port-forward` to one Pod's port, kept alive for the life of
/// this value. Killed on drop.
pub struct PortForward {
    child: std::process::Child,
    local_port: u16,
}

impl PortForward {
    pub async fn start(namespace: &str, pod: &str, remote_port: u16) -> anyhow::Result<Self> {
        let mut child = std::process::Command::new("kubectl")
            .args([
                "port-forward",
                "-n",
                namespace,
                &format!("pod/{pod}"),
                // Local port 0 asks the OS for a free one
                &format!("0:{remote_port}"),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("spawning `kubectl port-forward` — is kubectl on PATH and KUBECONFIG set?")?;
        let stdout = child.stdout.take().expect("piped stdout");
        let (tx, rx) = std::sync::mpsc::channel();
        // A dedicated thread, not just a one-shot read: kubectl keeps this
        // process alive and may write more to stdout/stderr later (e.g. on
        // a dropped connection); leaving the pipe un-drained risks kubectl
        // blocking on a full pipe buffer once this value is otherwise idle.
        std::thread::spawn(move || {
            let mut sent = false;
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if !sent {
                    if let Some(port) = parse_forwarded_port(&line) {
                        sent = true;
                        let _ = tx.send(port);
                    }
                }
            }
        });
        let local_port = rx
            .recv_timeout(Duration::from_secs(10))
            .context("kubectl port-forward never reported a bound local port")?;
        let forward = Self { child, local_port };
        let http = reqwest::Client::new();
        let base_url = forward.base_url();
        let ready = wait_until(Duration::from_secs(15), Duration::from_millis(200), || {
            let http = http.clone();
            let base_url = base_url.clone();
            async move { http.get(&base_url).send().await.is_ok() }
        })
        .await;
        anyhow::ensure!(
            ready,
            "kubectl port-forward to {pod}:{remote_port} (local {local_port}) never became reachable"
        );
        Ok(forward)
    }

    pub fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.local_port)
    }
}

impl Drop for PortForward {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Whether a Pod has reached `Running` — shared by every test that waits on
/// kubelet actually starting a workload's containers, which only happens
/// after every `NodeStageVolume`/`NodePublishVolume` call for its volumes
/// has succeeded.
pub async fn pod_is_running(client: &Client, namespace: &str, name: &str) -> bool {
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    pods.get(name)
        .await
        .ok()
        .and_then(|pod| pod.status)
        .and_then(|status| status.phase)
        .is_some_and(|phase| phase == "Running")
}

/// Whether a PVC has reached `Bound` — i.e. `CreateVolume` (delegated
/// through `mirrorvol-csi` or otherwise) completed for real.
pub async fn pvc_is_bound(client: &Client, namespace: &str, name: &str) -> bool {
    let claims: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), namespace);
    claims
        .get(name)
        .await
        .ok()
        .and_then(|claim| claim.status)
        .and_then(|status| status.phase)
        .is_some_and(|phase| phase == "Bound")
}

/// Best-effort event dump for a Pod — used to show *why* a mount is stuck
/// (or, for the CSI positive path, to confirm *why* it succeeded) rather
/// than asserting on Pod phase alone, which can't distinguish "correctly
/// gated" from "something else entirely is broken".
pub async fn pod_events(client: &Client, namespace: &str, pod_name: &str) -> Vec<String> {
    let events: Api<Event> = Api::namespaced(client.clone(), namespace);
    events
        .list(&ListParams::default())
        .await
        .map(|list| {
            list.items
                .into_iter()
                .filter(|event| {
                    event
                        .involved_object
                        .name
                        .as_deref()
                        .is_some_and(|name| name == pod_name)
                })
                .filter_map(|event| {
                    let reason = event.reason.unwrap_or_default();
                    let message = event.message.unwrap_or_default();
                    (!reason.is_empty() || !message.is_empty())
                        .then(|| format!("{reason}: {message}"))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The CSI driver name (`PersistentVolume.spec.csi.driver`) backing a bound
/// PVC, if any — the structural signal that a volume really was provisioned
/// through `mirrorvol-csi` (`mirrorvol.csi.homelab.internal`) and not some
/// other path, without scraping logs for it.
pub async fn bound_pv_csi_driver(
    client: &Client,
    namespace: &str,
    claim_name: &str,
) -> Option<String> {
    let claims: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), namespace);
    let pv_name = claims.get(claim_name).await.ok()?.spec?.volume_name?;
    let pvs: Api<PersistentVolume> = Api::all(client.clone());
    Some(pvs.get(&pv_name).await.ok()?.spec?.csi?.driver)
}

/// [`RESOLVED_PATH_ATTRIBUTE`](mirrorvol_api::naming::RESOLVED_PATH_ATTRIBUTE)
/// off a bound PVC's own [`PersistentVolume`], if `mirrorvol-csi` resolved
/// one — mirrors `mirrorvol-agent`'s own `resolved_underlying_path` so
/// tests can assert against the real directory Syncthing is actually
/// using.
pub async fn resolved_underlying_path(
    client: &Client,
    namespace: &str,
    claim_name: &str,
) -> Option<String> {
    let claims: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), namespace);
    let pv_name = claims.get(claim_name).await.ok()?.spec?.volume_name?;
    let pvs: Api<PersistentVolume> = Api::all(client.clone());
    pvs.get(&pv_name)
        .await
        .ok()?
        .spec?
        .csi?
        .volume_attributes?
        .get(mirrorvol_api::naming::RESOLVED_PATH_ATTRIBUTE)
        .cloned()
}

fn parse_forwarded_port(line: &str) -> Option<u16> {
    // Expected shape: "Forwarding from 127.0.0.1:39303 -> 8384"
    line.split("127.0.0.1:")
        .nth(1)?
        .split(char::is_whitespace)
        .next()?
        .parse()
        .ok()
}
