//! CSI Controller service (thin overlay). `CreateVolume` is the one RPC
//! with real logic (strip mirrorvol's own reserved parameters, delegate
//! the rest, mint an ID that remembers which underlying driver owns it).
//! Every RPC that names an existing volume decodes that ID and forwards
//! unmodified to the same underlying driver. RPCs this overlay has no way
//! to route without a volume_id to decode (`ListVolumes`, `GetCapacity`,
//! snapshotting) return `UNIMPLEMENTED` — spec-compliant, and honest: a
//! generic proxy genuinely can't guess which underlying driver a
//! volume-less call is about.

use tonic::{Request, Response, Status};

use crate::csi::v1::controller_server::Controller as ControllerService;
use crate::csi::v1::controller_service_capability::{rpc, Rpc, Type as CapabilityType};
use crate::csi::v1::*;
use crate::proxy::{self, params, VolumeRef};

pub struct Controller {
    node_name: String,
}

impl Controller {
    pub fn new(node_name: String) -> Self {
        Self { node_name }
    }
}

/// Mirrorvol's reserved keys, pulled out of a `CreateVolumeRequest`'s
/// `parameters`. Everything left in [`forwarded`](Self::forwarded) goes to
/// the underlying driver untouched. See [`params`].
struct ReservedParams {
    forwarded: std::collections::HashMap<String, String>,
    underlying_driver: Option<String>,
    backend: Option<String>,
    volume_id: Option<String>,
    underlying_path_template: Option<String>,
}

/// Resolves `mirrorvol.io/volumeId` given `claim_name`: the literal PVC
/// object name (`csi.storage.k8s.io/pvc/name`) when available, else the
/// opaque `CreateVolumeRequest.Name` as a last resort. See
/// [`create_volume`](Controller::create_volume).
fn resolve_volume_id(explicit: Option<String>, node_name: &str, claim_name: &str) -> String {
    explicit.unwrap_or_else(|| {
        mirrorvol_api::naming::strip_replica_claim_suffix(claim_name, node_name)
            .unwrap_or(claim_name)
            .to_owned()
    })
}

fn split_reserved_params(parameters: std::collections::HashMap<String, String>) -> ReservedParams {
    let mut result = ReservedParams {
        forwarded: std::collections::HashMap::new(),
        underlying_driver: None,
        backend: None,
        volume_id: None,
        underlying_path_template: None,
    };
    for (key, value) in parameters {
        match key.as_str() {
            params::UNDERLYING_DRIVER => result.underlying_driver = Some(value),
            params::BACKEND => result.backend = Some(value),
            params::VOLUME_ID => result.volume_id = Some(value),
            params::UNDERLYING_PATH_TEMPLATE => result.underlying_path_template = Some(value),
            _ => {
                result.forwarded.insert(key, value);
            }
        }
    }
    result
}

/// Renders `mirrorvol.io/underlyingPathTemplate` against the delegated
/// driver's own `CreateVolume` response — generic `{token}` substitution,
/// with no driver-specific knowledge of its own. `{volumeId}` is the
/// underlying driver's returned `volume_id`; `{context.<key>}` is any key
/// in its returned `volume_context`. See
/// [`params::UNDERLYING_PATH_TEMPLATE`].
fn render_underlying_path_template(
    template: &str,
    underlying_volume_id: &str,
    underlying_context: &std::collections::HashMap<String, String>,
) -> String {
    let mut resolved = template.replace("{volumeId}", underlying_volume_id);
    for (key, value) in underlying_context {
        resolved = resolved.replace(&format!("{{context.{key}}}"), value);
    }
    resolved
}

#[tonic::async_trait]
impl ControllerService for Controller {
    async fn create_volume(
        &self,
        request: Request<CreateVolumeRequest>,
    ) -> Result<Response<CreateVolumeResponse>, Status> {
        let mut req = request.into_inner();
        // The literal PVC name (`req.name` is an opaque "pvc-"+UID string
        // external-provisioner generates). Left in `forwarded` afterward
        // since some underlying drivers may need or want it.
        let claim_name = req.parameters.get(params::EXTRA_METADATA_PVC_NAME).cloned();
        let reserved = split_reserved_params(std::mem::take(&mut req.parameters));
        let underlying_driver = reserved.underlying_driver.ok_or_else(|| {
            Status::invalid_argument(format!(
                "StorageClass is missing the required {:?} parameter",
                params::UNDERLYING_DRIVER
            ))
        })?;
        // Falls back to mirrorvol-controller's replica-claim-name
        // convention ("{volume_id}-{node}") when the StorageClass doesn't
        // say otherwise, stripping this Controller's known node suffix.
        // Falls back to the raw CO-generated name for anything else.
        let volume_id = resolve_volume_id(
            reserved.volume_id,
            &self.node_name,
            claim_name.as_deref().unwrap_or(&req.name),
        );
        let backend = reserved.backend;
        req.parameters = reserved.forwarded;

        let mut client = proxy::controller_client(&underlying_driver).await?;
        let mut response = client.create_volume(req).await?.into_inner();
        let mut volume = response.volume.ok_or_else(|| {
            Status::internal("underlying driver's CreateVolume returned no volume")
        })?;

        // Rendered against the *underlying* driver's volume_id/context.
        // Must happen before either gets overwritten below.
        let resolved_path = reserved.underlying_path_template.map(|template| {
            render_underlying_path_template(&template, &volume.volume_id, &volume.volume_context)
        });

        volume.volume_id = VolumeRef::encode(&underlying_driver, &volume.volume_id);
        volume
            .volume_context
            .insert(params::BACKEND.to_owned(), backend.unwrap_or_default());
        volume
            .volume_context
            .insert(params::VOLUME_ID.to_owned(), volume_id);
        volume
            .volume_context
            .insert(params::UNDERLYING_DRIVER.to_owned(), underlying_driver);
        if let Some(resolved_path) = resolved_path {
            volume.volume_context.insert(
                mirrorvol_api::naming::RESOLVED_PATH_ATTRIBUTE.to_owned(),
                resolved_path,
            );
        }
        response.volume = Some(volume);
        Ok(Response::new(response))
    }

    async fn delete_volume(
        &self,
        request: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        let mut req = request.into_inner();
        let volume_ref = VolumeRef::decode(&req.volume_id)?;
        req.volume_id = volume_ref.underlying_id;
        let mut client = proxy::controller_client(&volume_ref.underlying_driver).await?;
        client.delete_volume(req).await
    }

    async fn controller_publish_volume(
        &self,
        request: Request<ControllerPublishVolumeRequest>,
    ) -> Result<Response<ControllerPublishVolumeResponse>, Status> {
        let mut req = request.into_inner();
        let volume_ref = VolumeRef::decode(&req.volume_id)?;
        req.volume_id = volume_ref.underlying_id;
        let mut client = proxy::controller_client(&volume_ref.underlying_driver).await?;
        client.controller_publish_volume(req).await
    }

    async fn controller_unpublish_volume(
        &self,
        request: Request<ControllerUnpublishVolumeRequest>,
    ) -> Result<Response<ControllerUnpublishVolumeResponse>, Status> {
        let mut req = request.into_inner();
        let volume_ref = VolumeRef::decode(&req.volume_id)?;
        req.volume_id = volume_ref.underlying_id;
        let mut client = proxy::controller_client(&volume_ref.underlying_driver).await?;
        client.controller_unpublish_volume(req).await
    }

    async fn validate_volume_capabilities(
        &self,
        request: Request<ValidateVolumeCapabilitiesRequest>,
    ) -> Result<Response<ValidateVolumeCapabilitiesResponse>, Status> {
        let mut req = request.into_inner();
        let volume_ref = VolumeRef::decode(&req.volume_id)?;
        req.volume_id = volume_ref.underlying_id;
        let mut client = proxy::controller_client(&volume_ref.underlying_driver).await?;
        client.validate_volume_capabilities(req).await
    }

    async fn list_volumes(
        &self,
        _request: Request<ListVolumesRequest>,
    ) -> Result<Response<ListVolumesResponse>, Status> {
        Err(Status::unimplemented(
            "ListVolumes has no volume_id to route by — this overlay can't know which \
             underlying driver(s) to ask without one",
        ))
    }

    async fn get_capacity(
        &self,
        _request: Request<GetCapacityRequest>,
    ) -> Result<Response<GetCapacityResponse>, Status> {
        Err(Status::unimplemented(
            "GetCapacity has no volume_id to route by — same limitation as ListVolumes",
        ))
    }

    async fn controller_get_capabilities(
        &self,
        _request: Request<ControllerGetCapabilitiesRequest>,
    ) -> Result<Response<ControllerGetCapabilitiesResponse>, Status> {
        Ok(Response::new(ControllerGetCapabilitiesResponse {
            capabilities: vec![ControllerServiceCapability {
                r#type: Some(CapabilityType::Rpc(Rpc {
                    r#type: rpc::Type::CreateDeleteVolume as i32,
                })),
            }],
        }))
    }

    async fn create_snapshot(
        &self,
        _request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        Err(Status::unimplemented(
            "snapshotting is not part of this overlay's scope",
        ))
    }

    async fn delete_snapshot(
        &self,
        _request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        Err(Status::unimplemented(
            "snapshotting is not part of this overlay's scope",
        ))
    }

    async fn list_snapshots(
        &self,
        _request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        Err(Status::unimplemented(
            "snapshotting is not part of this overlay's scope",
        ))
    }

    async fn controller_expand_volume(
        &self,
        request: Request<ControllerExpandVolumeRequest>,
    ) -> Result<Response<ControllerExpandVolumeResponse>, Status> {
        let mut req = request.into_inner();
        let volume_ref = VolumeRef::decode(&req.volume_id)?;
        req.volume_id = volume_ref.underlying_id;
        let mut client = proxy::controller_client(&volume_ref.underlying_driver).await?;
        client.controller_expand_volume(req).await
    }

    async fn controller_get_volume(
        &self,
        request: Request<ControllerGetVolumeRequest>,
    ) -> Result<Response<ControllerGetVolumeResponse>, Status> {
        let mut req = request.into_inner();
        let volume_ref = VolumeRef::decode(&req.volume_id)?;
        req.volume_id = volume_ref.underlying_id.clone();
        let mut client = proxy::controller_client(&volume_ref.underlying_driver).await?;
        let mut response = client.controller_get_volume(req).await?.into_inner();
        if let Some(volume) = response.volume.as_mut() {
            volume.volume_id =
                VolumeRef::encode(&volume_ref.underlying_driver, &volume_ref.underlying_id);
        }
        Ok(Response::new(response))
    }

    async fn controller_modify_volume(
        &self,
        request: Request<ControllerModifyVolumeRequest>,
    ) -> Result<Response<ControllerModifyVolumeResponse>, Status> {
        let mut req = request.into_inner();
        let volume_ref = VolumeRef::decode(&req.volume_id)?;
        req.volume_id = volume_ref.underlying_id;
        let mut client = proxy::controller_client(&volume_ref.underlying_driver).await?;
        client.controller_modify_volume(req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_volume_id_prefers_an_explicit_override() {
        assert_eq!(
            resolve_volume_id(Some("custom".to_owned()), "node-a", "app-node-a"),
            "custom"
        );
    }

    #[test]
    fn resolve_volume_id_recovers_the_shared_id_from_a_replica_claim_name() {
        assert_eq!(resolve_volume_id(None, "node-a", "app-node-a"), "app");
        // Same volume_id, different candidate node == same recovered ID.
        assert_eq!(resolve_volume_id(None, "node-b", "app-node-b"), "app");
    }

    #[test]
    fn resolve_volume_id_falls_back_to_the_raw_name_for_a_non_replica_pvc() {
        // A PVC this StorageClass provisions outside the MirroredVolume
        // control plane (or naming convention) never matches this node's
        // suffix. Falls back to the CO-generated name unchanged.
        assert_eq!(resolve_volume_id(None, "node-a", "pvc-1234"), "pvc-1234");
    }

    #[test]
    fn render_underlying_path_template_substitutes_the_underlying_volume_id() {
        assert_eq!(
            render_underlying_path_template(
                "/data/{volumeId}",
                "csi-hostpath-generated-uuid",
                &std::collections::HashMap::new(),
            ),
            "/data/csi-hostpath-generated-uuid"
        );
    }

    #[test]
    fn render_underlying_path_template_substitutes_underlying_context_keys() {
        let context = std::collections::HashMap::from([("zone".to_owned(), "az1".to_owned())]);
        assert_eq!(
            render_underlying_path_template("/data/{context.zone}/{volumeId}", "vol1", &context),
            "/data/az1/vol1"
        );
    }

    #[test]
    fn render_underlying_path_template_leaves_unmatched_tokens_untouched() {
        // A manifest referencing a context key the underlying driver never
        // returns just doesn't get substituted.
        assert_eq!(
            render_underlying_path_template(
                "/data/{context.missing}",
                "vol1",
                &std::collections::HashMap::new(),
            ),
            "/data/{context.missing}"
        );
    }

    #[test]
    fn split_reserved_params_pulls_out_only_the_reserved_keys() {
        let parameters = std::collections::HashMap::from([
            (
                params::UNDERLYING_DRIVER.to_owned(),
                "csi-driver-host-path".to_owned(),
            ),
            (
                params::BACKEND.to_owned(),
                mirrorvol_api::backend::SYNCTHING.to_owned(),
            ),
            (
                params::VOLUME_ID.to_owned(),
                "example-app-config".to_owned(),
            ),
            ("fsType".to_owned(), "ext4".to_owned()),
        ]);
        let split = split_reserved_params(parameters);
        assert_eq!(
            split.underlying_driver.as_deref(),
            Some("csi-driver-host-path")
        );
        assert_eq!(
            split.backend.as_deref(),
            Some(mirrorvol_api::backend::SYNCTHING)
        );
        assert_eq!(split.volume_id.as_deref(), Some("example-app-config"));
        // Only mirrorvol's own reserved keys are pulled out — everything
        // else is exactly what the underlying driver should still see.
        assert_eq!(
            split.forwarded,
            std::collections::HashMap::from([("fsType".to_owned(), "ext4".to_owned())])
        );
    }

    #[test]
    fn split_reserved_params_is_a_no_op_with_no_reserved_keys() {
        let parameters =
            std::collections::HashMap::from([("fsType".to_owned(), "ext4".to_owned())]);
        let split = split_reserved_params(parameters.clone());
        assert!(split.underlying_driver.is_none());
        assert!(split.backend.is_none());
        assert!(split.volume_id.is_none());
        assert_eq!(split.forwarded, parameters);
    }
}
