//! Generic delegation to an underlying CSI driver's socket, plus the
//! volume-ID encoding that makes delegation possible without a lookup
//! table: `CreateVolume` mints an ID that also encodes which underlying
//! driver owns it, and every later RPC decodes that ID to know which
//! socket to forward to.

use tonic::transport::{Channel, Endpoint, Uri};
use tonic::Status;

use crate::csi::v1::controller_client::ControllerClient;
use crate::csi::v1::node_client::NodeClient;

/// Kubelet's standard per-driver socket location — the convention nearly
/// every CSI driver's deploy manifest uses for its own socket. This is
/// the one convention `mirrorvol-csi` is allowed to assume about an
/// underlying driver.
fn socket_path(underlying_driver: &str) -> std::path::PathBuf {
    std::path::Path::new("/var/lib/kubelet/plugins")
        .join(underlying_driver)
        .join("csi.sock")
}

#[cfg(unix)]
async fn connect(underlying_driver: &str) -> Result<Channel, Status> {
    let path = socket_path(underlying_driver);
    // The URI below is never actually dialed; `connect_with_connector`
    // always uses the UnixStream the closure returns instead. Any
    // well-formed placeholder satisfies Endpoint's constructor.
    Endpoint::try_from("http://[::]:50051")
        .expect("static placeholder URI is always valid")
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let path = path.clone();
            async move {
                tokio::net::UnixStream::connect(path)
                    .await
                    .map(hyper_util::rt::TokioIo::new)
            }
        }))
        .await
        .map_err(|error| {
            Status::unavailable(format!(
                "connecting to underlying CSI driver {underlying_driver}: {error}"
            ))
        })
}

#[cfg(not(unix))]
async fn connect(_underlying_driver: &str) -> Result<Channel, Status> {
    Err(Status::unimplemented(
        "mirrorvol-csi requires Unix domain sockets",
    ))
}

pub async fn controller_client(
    underlying_driver: &str,
) -> Result<ControllerClient<Channel>, Status> {
    Ok(ControllerClient::new(connect(underlying_driver).await?))
}

pub async fn node_client(underlying_driver: &str) -> Result<NodeClient<Channel>, Status> {
    Ok(NodeClient::new(connect(underlying_driver).await?))
}

/// A decoded `mirrorvol-csi` volume ID: which underlying driver owns this
/// volume, and what its `volume_id` actually is.
pub struct VolumeRef {
    pub underlying_driver: String,
    pub underlying_id: String,
}

/// The delimiter assumes an underlying driver's name (a CSI driver
/// name, which the spec requires to be RFC 1035 domain-name-shaped) and
/// never contains `~`. Worth hardening later.
const DELIMITER: char = '~';

impl VolumeRef {
    pub fn encode(underlying_driver: &str, underlying_id: &str) -> String {
        format!("{underlying_driver}{DELIMITER}{underlying_id}")
    }

    pub fn decode(volume_id: &str) -> Result<Self, Status> {
        volume_id
            .split_once(DELIMITER)
            .map(|(underlying_driver, underlying_id)| Self {
                underlying_driver: underlying_driver.to_owned(),
                underlying_id: underlying_id.to_owned(),
            })
            .ok_or_else(|| {
                Status::invalid_argument(format!(
                    "volume_id {volume_id:?} was not minted by mirrorvol-csi (missing {DELIMITER:?} delimiter)"
                ))
            })
    }
}

/// The reserved `StorageClass`/`CreateVolumeRequest.parameters` keys this
/// overlay understands. Everything else is forwarded to the underlying
/// driver untouched.
pub mod params {
    pub const UNDERLYING_DRIVER: &str = "mirrorvol.io/underlyingDriver";
    pub const BACKEND: &str = "mirrorvol.io/backend";
    /// The identity this volume's backend should use. Defaults to
    /// `CreateVolumeRequest.name` when absent. A StorageClass can override it
    /// since `name` is usually an opaque `pvc-<uid>` string.
    pub const VOLUME_ID: &str = "mirrorvol.io/volumeId";
    /// Operator-authored template for the underlying driver's real on-disk
    /// path, e.g. `/data/{volumeId}`. Only meaningful when a directory/
    /// hostPath-passthrough underlying driver's storage root coincides
    /// with `MIRRORVOL_DATA_ROOT`. Absent by default; when present,
    /// `Controller::create_volume` renders it against the delegated
    /// driver's response and writes the result to
    /// [`RESOLVED_PATH_ATTRIBUTE`](mirrorvol_api::naming::RESOLVED_PATH_ATTRIBUTE).
    pub const UNDERLYING_PATH_TEMPLATE: &str = "mirrorvol.io/underlyingPathTemplate";
    /// Standard `csi-provisioner` `--extra-create-metadata` key carrying
    /// the literal PVC object name (unlike `CreateVolumeRequest.Name`,
    /// which is `"pvc-" + PVC.UID`). Only present when the sidecar enables
    /// that flag. Not a `mirrorvol.io/*` key — defined by
    /// `external-provisioner` itself.
    pub const EXTRA_METADATA_PVC_NAME: &str = "csi.storage.k8s.io/pvc/name";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_minted_id_round_trips_through_decode() {
        let id = VolumeRef::encode("csi-driver-host-path", "pvc-1234");
        let decoded = VolumeRef::decode(&id).expect("decode");
        assert_eq!(decoded.underlying_driver, "csi-driver-host-path");
        assert_eq!(decoded.underlying_id, "pvc-1234");
    }

    #[test]
    fn decode_only_splits_on_the_first_delimiter() {
        // The underlying ID is free to contain the delimiter itself.
        // `split_once` only ever separates the driver name (the part this
        // encoding controls) from everything after it.
        let decoded = VolumeRef::decode("host-path~pvc-1234~extra").expect("decode");
        assert_eq!(decoded.underlying_driver, "host-path");
        assert_eq!(decoded.underlying_id, "pvc-1234~extra");
    }

    #[test]
    fn decode_rejects_an_id_this_driver_never_minted() {
        assert!(VolumeRef::decode("pvc-1234-no-delimiter").is_err());
    }
}
