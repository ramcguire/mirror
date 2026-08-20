//! CSI Node service, thin overlay that delegates mount/unmount to the
//! underlying driver named by the volume's encoded ID.
//!
//! `NodeStageVolume` is the one RPC with real logic beyond delegation: once
//! the underlying driver's own `NodeStageVolume` succeeds, it attaches the
//! backend named in `volume_context["mirrorvol.io/backend"]`.
//!
//! `NodePublishVolume` publishes the underlying driver's staged root as-is.

use std::sync::Arc;

use mirrorvol_backend::rsync::{RsyncBackend, SystemRsyncRunner};
use mirrorvol_backend::syncthing::{LocalEndpoint, SyncthingBackend};
use mirrorvol_backend::{CompletionStatus, LocalBackend, LockState};
use tonic::{Request, Response, Status};

use crate::csi::v1::node_server::Node as NodeService;
use crate::csi::v1::node_service_capability::{rpc, Rpc, Type as CapabilityType};
use crate::csi::v1::*;
use crate::proxy::{self, params, VolumeRef};

pub struct Node {
    node_name: String,
}

impl Node {
    pub fn new(node_name: String) -> Self {
        Self { node_name }
    }
}

/// Attaches the backend named in `volume_context` to a volume the
/// underlying driver just staged. `local_path` must be the real, resolved
/// per-candidate replica directory `mirrorvol-agent` itself operates on
/// (`mirrorvol.io/resolvedPath`, when set — see `node_stage_volume`'s own
/// resolution below) — **not** necessarily kubelet's own
/// `staging_target_path` verbatim. Those two only coincide by construction
/// when the underlying driver's own staging convention happens to match
/// `replica_path_template`; a real `underlyingPathTemplate` deployment
/// (the recommended `mirrorvol-syncthing` StorageClass shape — see
/// `deploy/csi/storageclass.yaml`) routes each candidate's real data
/// through a driver-chosen path `mirrorvol-agent` discovers via that same
/// attribute, which can differ from whatever ephemeral directory kubelet's
/// own CSI staging plumbing happens to bind-mount its result at. Passing
/// the wrong one here doesn't just misread stale data — for a filesystem
/// check like [`LocalBackend::lock_state`], it silently reads an empty/
/// unrelated directory and reports `Absent` regardless of the real
/// on-disk state, which `verify_local_writer` cannot distinguish from a
/// genuine crash-window contradiction.
async fn attach_backend(backend: &str, volume_id: &str, local_path: &str) -> Result<(), Status> {
    match backend {
        "" => Ok(()), // no backend named — nothing to enforce
        mirrorvol_api::backend::SYNCTHING => attach_syncthing(volume_id, local_path).await,
        mirrorvol_api::backend::RSYNC => attach_rsync(volume_id, local_path).await,
        other => Err(Status::unimplemented(format!(
            "mirrorvol-csi has no backend attach logic for {other:?} yet"
        ))),
    }
}

/// Reads a file named `volume_id` inside the
/// [`CONSISTENCY_DIR`](mirrorvol_api::naming::env::CONSISTENCY_DIR)
/// directory — written by this node's `mirrorvol-agent` (see
/// `write_consistency_signal`), the only channel this crate has for a
/// volume's `spec.consistency`.
fn volume_is_best_effort(volume_id: &str) -> bool {
    let dir = std::env::var(mirrorvol_api::naming::env::CONSISTENCY_DIR)
        .unwrap_or_else(|_| mirrorvol_api::naming::env::CONSISTENCY_DIR_DEFAULT.to_owned());
    std::fs::read_to_string(std::path::Path::new(&dir).join(volume_id))
        .is_ok_and(|contents| contents == mirrorvol_api::Consistency::BestEffort.as_str())
}

/// Resolves [`CONSISTENCY_DIR`](mirrorvol_api::naming::env::CONSISTENCY_DIR)
/// once per call site — kept separate from the functions below so they can
/// take the directory as a plain parameter instead of reading the process
/// environment internally, which makes them directly unit-testable (env
/// vars are global process state; `cargo test` runs a binary's tests in
/// parallel by default, so a function that reads one internally can't
/// safely be exercised with different values from more than one test).
fn consistency_dir() -> std::path::PathBuf {
    std::path::PathBuf::from(
        std::env::var(mirrorvol_api::naming::env::CONSISTENCY_DIR)
            .unwrap_or_else(|_| mirrorvol_api::naming::env::CONSISTENCY_DIR_DEFAULT.to_owned()),
    )
}

/// Reads the last verdict this node's `mirrorvol-agent` relayed for
/// `volume_id` (see `write_consistency_signal`'s writer-agreement write in
/// `mirrorvol-agent/src/main.rs`) — the controller's own computed
/// [`writer_agreement`](mirrorvol_api::writer_agreement), which this
/// process has no Kubernetes API access to read directly. Only ever true
/// on an exact `Contradiction` read; anything else (missing file, a race
/// before this node's agent has reconciled the volume even once, an
/// unreadable file) fails toward *not* adding this supplementary veto —
/// the live local check below (`verify_local_writer`) remains the
/// authoritative gate either way, so a missing relay can't itself block an
/// otherwise-healthy attach.
fn relayed_writer_agreement_is_contradiction(dir: &std::path::Path, volume_id: &str) -> bool {
    std::fs::read_to_string(
        dir.join(mirrorvol_api::naming::writer_agreement_signal_file_name(
            volume_id,
        )),
    )
    .is_ok_and(|contents| contents == mirrorvol_api::writer_agreement::CONTRADICTION)
}

/// `strict` only — `bestEffort` callers skip this entirely
/// (`volume_is_best_effort`). Live and local, re-checked here at the
/// moment kubelet is about to let the workload start, on top of (not
/// instead of) the agent's own continuous per-candidate reconcile:
/// completion, backend role, *and* — since a crash between flipping the
/// role and creating the writer lock can leave `is_writer()` true with no
/// lock ever created — that the local lock actually backs the claimed
/// role. Additionally consults the controller's own relayed verdict, which
/// catches what a purely local read structurally can't: whether anything
/// in the CR currently authorizes this node as writer at all. Shared by
/// both backends so this checklist can't drift between them.
async fn verify_local_writer(
    backend: &dyn LocalBackend,
    volume_id: &str,
    consistency_dir: &std::path::Path,
) -> Result<(), Status> {
    let ready = backend
        .completion(volume_id)
        .await
        .map_err(|error| Status::unavailable(format!("checking {volume_id} completion: {error}")))?
        .ready();
    if !ready {
        return Err(Status::failed_precondition(format!(
            "{volume_id} is not fully synced yet"
        )));
    }
    let is_writer = backend.is_writer(volume_id).await.map_err(|error| {
        Status::unavailable(format!("checking {volume_id} writer role: {error}"))
    })?;
    if !is_writer {
        return Err(Status::failed_precondition(format!(
            "{volume_id} is not this node's writer role yet"
        )));
    }
    let lock_state = backend.lock_state(volume_id).await.map_err(|error| {
        Status::unavailable(format!("checking {volume_id} lock state: {error}"))
    })?;
    if matches!(lock_state, LockState::Absent) {
        return Err(Status::failed_precondition(format!(
            "{volume_id} is this node's writer role but no local writer lock is present"
        )));
    }
    if relayed_writer_agreement_is_contradiction(consistency_dir, volume_id) {
        return Err(Status::failed_precondition(format!(
            "{volume_id}: controller reports no grant currently authorizes this node as writer"
        )));
    }
    Ok(())
}

async fn attach_syncthing(volume_id: &str, local_path: &str) -> Result<(), Status> {
    // `bestEffort` never gates scheduling/attaching. Every folder defaults
    // to Send & Receive there, so both checks below would be close to meaningless
    // anyway.
    if volume_is_best_effort(volume_id) {
        return Ok(());
    }
    // This driver runs in the syncthing sidecar's own Pod, so `localhost`
    // always means "my own sibling".
    let endpoint = LocalEndpoint::from_env()
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    // `data_root` itself is irrelevant here — this call never invokes
    // `ensure_replica`, so `with_known_replica_path` is what actually makes
    // `lock_state`'s directory lookup resolve to the real one; see that
    // method's own doc comment for why the plain constructor's fallback
    // can't be relied on for a bare volume_id alone.
    let backend =
        SyncthingBackend::new(endpoint, "").with_known_replica_path(volume_id, local_path);
    verify_local_writer(&backend, volume_id, &consistency_dir()).await
}

/// `strict` only, same shape as [`attach_syncthing`] — `bestEffort` skips
/// this gate entirely. Runs one more synchronous full pull + verification
/// pass (`RsyncBackend::completion`, the same call `mirrorvol-agent`'s own
/// reconcile loop uses) right at the moment kubelet is about to let the
/// workload start, on top of (not instead of) the agent's own continuous
/// per-candidate reconcile.
async fn attach_rsync(volume_id: &str, local_path: &str) -> Result<(), Status> {
    if volume_is_best_effort(volume_id) {
        return Ok(());
    }
    let secret_file = std::env::var(mirrorvol_backend::rsync::env::RSYNC_SECRET_FILE).ok();
    let mut backend = RsyncBackend::new(Arc::new(SystemRsyncRunner), local_path);
    // This process pulls too (the completion check below), so it needs
    // the same mandatory tunnel mirrorvol-agent uses.
    if let Some(secret_file) = secret_file {
        backend = backend
            .with_secret_file(secret_file.clone())
            .with_stunnel(Arc::new(
                mirrorvol_backend::rsync::SystemStunnelClient::new(
                    mirrorvol_backend::rsync::STUNNEL_PORT,
                    secret_file,
                ),
            ));
    }
    // This process has no Kubernetes API access, so it can't resolve
    // `status.active` itself — read back the address `mirrorvol-agent`'s
    // reconcile loop already resolved and shared, same channel as the
    // consistency signal.
    let dir = consistency_dir();
    let active_peer_address = std::fs::read_to_string(dir.join(
        mirrorvol_api::naming::active_peer_signal_file_name(volume_id),
    ))
    .ok()
    .filter(|contents| !contents.is_empty());
    backend
        .ensure_replica(&mirrorvol_backend::ReplicaConfig {
            volume_id: volume_id.to_owned(),
            local_path: local_path.to_owned(),
            active_peer_address,
            ..Default::default()
        })
        .await
        .map_err(|error| Status::unavailable(format!("configuring {volume_id}: {error}")))?;
    verify_local_writer(&backend, volume_id, &dir).await
}

#[tonic::async_trait]
impl NodeService for Node {
    async fn node_stage_volume(
        &self,
        request: Request<NodeStageVolumeRequest>,
    ) -> Result<Response<NodeStageVolumeResponse>, Status> {
        let mut req = request.into_inner();
        let volume_ref = VolumeRef::decode(&req.volume_id)?;
        req.volume_id = volume_ref.underlying_id;

        let backend = req
            .volume_context
            .get(params::BACKEND)
            .cloned()
            .unwrap_or_default();
        let backend_volume_id = req
            .volume_context
            .get(params::VOLUME_ID)
            .cloned()
            .unwrap_or_else(|| req.volume_id.clone());
        // Prefer the same resolved real data path mirrorvol-agent itself
        // discovered and operates on (round-tripped here via
        // volume_context, exactly as CSI propagates CreateVolume's
        // volume_context back on every later NodeStageVolume call — no
        // extra Kubernetes API access needed) — see attach_backend's own
        // doc comment for why this must not just be staging_target_path
        // verbatim whenever underlyingPathTemplate resolved one.
        let local_path = req
            .volume_context
            .get(mirrorvol_api::naming::RESOLVED_PATH_ATTRIBUTE)
            .cloned()
            .unwrap_or_else(|| req.staging_target_path.clone());

        let mut client = proxy::node_client(&volume_ref.underlying_driver).await?;
        let response = client.node_stage_volume(req).await?;

        attach_backend(&backend, &backend_volume_id, &local_path).await?;

        Ok(response)
    }

    async fn node_unstage_volume(
        &self,
        request: Request<NodeUnstageVolumeRequest>,
    ) -> Result<Response<NodeUnstageVolumeResponse>, Status> {
        let mut req = request.into_inner();
        let volume_ref = VolumeRef::decode(&req.volume_id)?;
        req.volume_id = volume_ref.underlying_id;
        let mut client = proxy::node_client(&volume_ref.underlying_driver).await?;
        client.node_unstage_volume(req).await
    }

    async fn node_publish_volume(
        &self,
        request: Request<NodePublishVolumeRequest>,
    ) -> Result<Response<NodePublishVolumeResponse>, Status> {
        let mut req = request.into_inner();
        let volume_ref = VolumeRef::decode(&req.volume_id)?;
        req.volume_id = volume_ref.underlying_id;
        let mut client = proxy::node_client(&volume_ref.underlying_driver).await?;
        client.node_publish_volume(req).await
    }

    async fn node_unpublish_volume(
        &self,
        request: Request<NodeUnpublishVolumeRequest>,
    ) -> Result<Response<NodeUnpublishVolumeResponse>, Status> {
        let mut req = request.into_inner();
        let volume_ref = VolumeRef::decode(&req.volume_id)?;
        req.volume_id = volume_ref.underlying_id;
        let mut client = proxy::node_client(&volume_ref.underlying_driver).await?;
        client.node_unpublish_volume(req).await
    }

    async fn node_get_volume_stats(
        &self,
        request: Request<NodeGetVolumeStatsRequest>,
    ) -> Result<Response<NodeGetVolumeStatsResponse>, Status> {
        let mut req = request.into_inner();
        let volume_ref = VolumeRef::decode(&req.volume_id)?;
        req.volume_id = volume_ref.underlying_id;
        let mut client = proxy::node_client(&volume_ref.underlying_driver).await?;
        client.node_get_volume_stats(req).await
    }

    async fn node_expand_volume(
        &self,
        request: Request<NodeExpandVolumeRequest>,
    ) -> Result<Response<NodeExpandVolumeResponse>, Status> {
        let mut req = request.into_inner();
        let volume_ref = VolumeRef::decode(&req.volume_id)?;
        req.volume_id = volume_ref.underlying_id;
        let mut client = proxy::node_client(&volume_ref.underlying_driver).await?;
        client.node_expand_volume(req).await
    }

    async fn node_get_capabilities(
        &self,
        _request: Request<NodeGetCapabilitiesRequest>,
    ) -> Result<Response<NodeGetCapabilitiesResponse>, Status> {
        Ok(Response::new(NodeGetCapabilitiesResponse {
            capabilities: vec![NodeServiceCapability {
                r#type: Some(CapabilityType::Rpc(Rpc {
                    r#type: rpc::Type::StageUnstageVolume as i32,
                })),
            }],
        }))
    }

    async fn node_get_info(
        &self,
        _request: Request<NodeGetInfoRequest>,
    ) -> Result<Response<NodeGetInfoResponse>, Status> {
        Ok(Response::new(NodeGetInfoResponse {
            node_id: self.node_name.clone(),
            max_volumes_per_node: 0,
            accessible_topology: None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{relayed_writer_agreement_is_contradiction, verify_local_writer};
    use mirrorvol_backend::{FakeBackend, LockState};

    const VOLUME_ID: &str = "vol-a";

    fn write_relay(dir: &std::path::Path, value: &str) {
        std::fs::write(
            dir.join(mirrorvol_api::naming::writer_agreement_signal_file_name(
                VOLUME_ID,
            )),
            value,
        )
        .expect("write relay file");
    }

    #[test]
    fn veto_is_true_only_on_an_exact_contradiction_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_relay(dir.path(), mirrorvol_api::writer_agreement::CONTRADICTION);
        assert!(relayed_writer_agreement_is_contradiction(
            dir.path(),
            VOLUME_ID
        ));
    }

    #[test]
    fn veto_is_false_on_converged_or_pending() {
        let dir = tempfile::tempdir().expect("tempdir");
        for value in [
            mirrorvol_api::writer_agreement::CONVERGED,
            mirrorvol_api::writer_agreement::PENDING,
        ] {
            write_relay(dir.path(), value);
            assert!(!relayed_writer_agreement_is_contradiction(
                dir.path(),
                VOLUME_ID
            ));
        }
    }

    #[test]
    fn veto_is_false_on_a_missing_relay_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Never written — a race before this node's agent has reconciled
        // the volume even once, or a volume this node isn't a candidate
        // for. Must fail toward *not* blocking.
        assert!(!relayed_writer_agreement_is_contradiction(
            dir.path(),
            VOLUME_ID
        ));
    }

    #[tokio::test]
    async fn verify_local_writer_fails_closed_when_completion_is_not_ready() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = FakeBackend::default();
        // completion defaults to not-ready with nothing set.
        let result = verify_local_writer(&backend, VOLUME_ID, dir.path()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn verify_local_writer_fails_closed_when_not_the_writer() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = FakeBackend::default();
        backend.set_completion(VOLUME_ID, true);
        // writer defaults to false with nothing set.
        let result = verify_local_writer(&backend, VOLUME_ID, dir.path()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn verify_local_writer_fails_closed_when_the_local_lock_is_absent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = FakeBackend::default();
        backend.set_completion(VOLUME_ID, true);
        backend.set_writer(VOLUME_ID, true);
        backend.set_lock_state(VOLUME_ID, LockState::Absent);
        // The crash-window case: role flipped, lock never created.
        let result = verify_local_writer(&backend, VOLUME_ID, dir.path()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn verify_local_writer_succeeds_with_no_relay_file_present() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = FakeBackend::default();
        backend.set_completion(VOLUME_ID, true);
        backend.set_writer(VOLUME_ID, true);
        backend.set_lock_state(VOLUME_ID, LockState::Present { epoch: Some(1) });
        let result = verify_local_writer(&backend, VOLUME_ID, dir.path()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn verify_local_writer_succeeds_when_the_relay_says_converged() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_relay(dir.path(), mirrorvol_api::writer_agreement::CONVERGED);
        let backend = FakeBackend::default();
        backend.set_completion(VOLUME_ID, true);
        backend.set_writer(VOLUME_ID, true);
        backend.set_lock_state(VOLUME_ID, LockState::Present { epoch: Some(1) });
        let result = verify_local_writer(&backend, VOLUME_ID, dir.path()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn verify_local_writer_fails_closed_when_the_relay_says_contradiction() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_relay(dir.path(), mirrorvol_api::writer_agreement::CONTRADICTION);
        let backend = FakeBackend::default();
        backend.set_completion(VOLUME_ID, true);
        backend.set_writer(VOLUME_ID, true);
        backend.set_lock_state(VOLUME_ID, LockState::Present { epoch: Some(1) });
        // Every local check passes — the relayed controller verdict is the
        // only thing that should still block this attach.
        let result = verify_local_writer(&backend, VOLUME_ID, dir.path()).await;
        assert!(result.is_err());
    }
}
