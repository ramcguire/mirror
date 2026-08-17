//! CSI Node service, thin overlay that delegates mount/unmount to the
//! underlying driver named by the volume's encoded ID.
//!
//! `NodeStageVolume` is the one RPC with real logic beyond delegation: once
//! the underlying driver's own `NodeStageVolume` succeeds, it attaches the
//! backend named in `volume_context["mirrorvol.io/backend"]`.
//!
//! `NodePublishVolume` publishes the underlying driver's staged root as-is.

use mirrorvol_backend::syncthing::{LocalEndpoint, SyncthingBackend};
use mirrorvol_backend::{CompletionStatus, LocalBackend};
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
/// underlying driver just staged.
async fn attach_backend(backend: &str, volume_id: &str, staged_path: &str) -> Result<(), Status> {
    match backend {
        "" => Ok(()), // no backend named — nothing to enforce
        mirrorvol_api::backend::SYNCTHING => attach_syncthing(volume_id, staged_path).await,
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

async fn attach_syncthing(volume_id: &str, staged_path: &str) -> Result<(), Status> {
    // `bestEffort` never gates scheduling/attaching. Every folder defaults
    // to Send & Receive there, so both checks below would be close to meaningless
    // anyway.
    if volume_is_best_effort(volume_id) {
        return Ok(());
    }
    // This driver runs in the syncthing sidecar's own Pod, so `localhost`
    // always means "my own sibling".
    let base_url = std::env::var(mirrorvol_backend::syncthing::env::SYNCTHING_BASE_URL)
        .unwrap_or_else(|_| {
            mirrorvol_backend::syncthing::env::SYNCTHING_BASE_URL_DEFAULT.to_owned()
        });
    let api_key = mirrorvol_backend::syncthing::api_key_from_env(
        mirrorvol_backend::syncthing::env::SYNCTHING_API_KEY,
        mirrorvol_backend::syncthing::env::SYNCTHING_API_KEY_FILE,
    )
    .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let backend = SyncthingBackend::new(LocalEndpoint { base_url, api_key }, staged_path);

    // Live and local, enforced again here at the moment kubelet is about
    // to let the workload start.
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
    Ok(())
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
        let staging_target_path = req.staging_target_path.clone();

        let mut client = proxy::node_client(&volume_ref.underlying_driver).await?;
        let response = client.node_stage_volume(req).await?;

        attach_backend(&backend, &backend_volume_id, &staging_target_path).await?;

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
