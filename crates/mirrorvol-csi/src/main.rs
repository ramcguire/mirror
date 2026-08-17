//! CSI overlay driver. Wraps an existing (preferably node-local)
//! CSI driver, delegating provisioning/mounting to it, and attaching
//! a mirrorvol backend once `NodeStageVolume`'s delegated mount succeeds.
#![cfg_attr(not(unix), allow(dead_code, unused_imports))]

mod controller;
mod identity;
mod node;
mod proxy;

pub mod csi {
    pub mod v1 {
        tonic::include_proto!("csi.v1");
    }
}

pub(crate) const CSI_NAME: &str = env!("CARGO_PKG_NAME");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();
    run().await
}

/// Unix-domain sockets don't exist on Windows; gated so this still compiles
/// on non-Unix. Imports live inside the function so they don't warn as
/// unused on a non-Unix `cargo check`.
#[cfg(unix)]
async fn run() -> anyhow::Result<()> {
    use csi::v1::controller_server::ControllerServer;
    use csi::v1::identity_server::IdentityServer;
    use csi::v1::node_server::NodeServer;
    use tonic::transport::Server;

    // "unix://" prefix matches the convention every CSI CO (Kubernetes
    // included) uses for CSI_ENDPOINT — stripped here since
    // tokio::net::UnixListener wants a bare filesystem path.
    let endpoint =
        std::env::var("CSI_ENDPOINT").unwrap_or_else(|_| "unix:///csi/csi.sock".to_owned());
    let socket_path = endpoint
        .strip_prefix("unix://")
        .unwrap_or(&endpoint)
        .to_owned();
    let node_name = std::env::var(mirrorvol_api::naming::env::NODE_NAME)?;

    if let Some(parent) = std::path::Path::new(&socket_path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    // A stale socket from a previous crash left the file behind — bind
    // fails with AddrInUse otherwise.
    let _ = std::fs::remove_file(&socket_path);
    let listener = tokio::net::UnixListener::bind(&socket_path)?;
    let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);

    tracing::info!(socket = %socket_path, %node_name, "mirrorvol-csi listening");

    Server::builder()
        .add_service(IdentityServer::new(identity::Identity))
        .add_service(ControllerServer::new(controller::Controller::new(
            node_name.clone(),
        )))
        .add_service(NodeServer::new(node::Node::new(node_name)))
        .serve_with_incoming(incoming)
        .await?;
    Ok(())
}

#[cfg(not(unix))]
async fn run() -> anyhow::Result<()> {
    anyhow::bail!("mirrorvol-csi requires Unix domain sockets")
}
