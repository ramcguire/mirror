//! CSI Identity service

use tonic::{Request, Response, Status};

use crate::csi::v1::identity_server::Identity as IdentityService;
use crate::csi::v1::plugin_capability::{service, Service, Type as CapabilityType};
use crate::csi::v1::{
    GetPluginCapabilitiesRequest, GetPluginCapabilitiesResponse, GetPluginInfoRequest,
    GetPluginInfoResponse, PluginCapability, ProbeRequest, ProbeResponse,
};

/// This CSI driver's own name, registered with kubelet (see
/// `deploy/csi/csidriver.yaml`) — distinct from the CRDs'
/// [`API_GROUP`](mirrorvol_api::API_GROUP) (`homelab.internal`), which it
/// only shares a domain suffix with by convention, not by any code-level
/// relationship.
const DRIVER_NAME: &str = "mirrorvol.csi.homelab.internal";

pub struct Identity;

#[tonic::async_trait]
impl IdentityService for Identity {
    async fn get_plugin_info(
        &self,
        _request: Request<GetPluginInfoRequest>,
    ) -> Result<Response<GetPluginInfoResponse>, Status> {
        Ok(Response::new(GetPluginInfoResponse {
            name: DRIVER_NAME.to_owned(),
            vendor_version: env!("CARGO_PKG_VERSION").to_owned(),
            manifest: Default::default(),
        }))
    }

    async fn get_plugin_capabilities(
        &self,
        _request: Request<GetPluginCapabilitiesRequest>,
    ) -> Result<Response<GetPluginCapabilitiesResponse>, Status> {
        // No VOLUME_ACCESSIBILITY_CONSTRAINTS here: this overlay never
        // decides node placement itself. `--node-deployment` already
        // pins where its own Controller service runs, and topology
        // decisions for the underlying driver are the underlying driver's
        // own concern (forwarded through, not reinterpreted).
        Ok(Response::new(GetPluginCapabilitiesResponse {
            capabilities: vec![PluginCapability {
                r#type: Some(CapabilityType::Service(Service {
                    r#type: service::Type::ControllerService as i32,
                })),
            }],
        }))
    }

    async fn probe(
        &self,
        _request: Request<ProbeRequest>,
    ) -> Result<Response<ProbeResponse>, Status> {
        Ok(Response::new(ProbeResponse { ready: Some(true) }))
    }
}
