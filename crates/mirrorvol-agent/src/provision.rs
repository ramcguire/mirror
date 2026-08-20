//! `mirrorvol-agent provision` — initContainer step run before this node's
//! backend daemon(s) start. Get-or-creates each configured backend's
//! secret(s) [`Secret`](k8s_openapi::api::core::v1::Secret) and writes them
//! to a shared `emptyDir` file, since a DaemonSet's pod template can't
//! point `secretKeyRef.name` at a per-node Secret. Syncthing's API key
//! always runs; `rsync`'s module secret and per-node identity token are
//! opt-in, gated the same way `mirrorvol-agent run` enables the backend
//! itself — on [`rsync::env::RSYNC_SECRET_FILE`] being set at all — so a
//! Pod that doesn't run `rsyncd` doesn't provision secrets nothing reads.
//!
//! Three secrets, three different ownership scopes:
//! - Syncthing's per-node API key and `rsync`'s per-node identity token are
//!   each owned by that node's own `Node` object.
//! - `rsync`'s module password is genuinely shared across every candidate
//!   (a puller needs the *source* node's credential), so it's owned by the
//!   `mirrorvol-controller` `Deployment` instead — the object that actually
//!   represents "this operator installation exists," not an arbitrary node
//!   that happened to win the create race.
//!
//! Cert-manager device-cert provisioning is not implemented yet; this only
//! handles pre-shared secrets/tokens.

use k8s_openapi::api::core::v1::Secret;
use kube::api::{Api, ObjectMeta, PostParams};
use kube::Client;
use mirrorvol_api::naming;
use mirrorvol_backend::rsync;

const API_KEY_SECRET_KEY: &str = "apikey";
const RSYNC_SECRET_KEY: &str = "secret";
const RSYNC_IDENTITY_SECRET_KEY: &str = "deviceId";

pub async fn run() -> anyhow::Result<()> {
    let node = std::env::var(naming::env::NODE_NAME)?;
    let namespace = std::env::var(naming::env::POD_NAMESPACE)
        .unwrap_or_else(|_| naming::env::POD_NAMESPACE_DEFAULT.to_owned());
    let client = Client::try_default().await?;
    let node_owner =
        crate::owner_references("v1", "Node", &node, crate::node_uid(&client, &node).await?);

    let out_path =
        std::env::var("MIRRORVOL_APIKEY_FILE").unwrap_or_else(|_| "/shared/apikey".to_owned());
    let api_key = get_or_create_secret_value(
        &client,
        &namespace,
        &naming::per_node_secret_name(&node),
        API_KEY_SECRET_KEY,
        node_owner.clone(),
    )
    .await?;
    write_secret_file(&out_path, &api_key)?;
    tracing::info!(%node, path = %out_path, "provisioned per-node Syncthing API key");

    if let Ok(rsync_out_path) = std::env::var(rsync::env::RSYNC_SECRET_FILE) {
        // Shared across every node, not per-node — owned by the controller
        // Deployment, not this (or any) node.
        let deployment_owner = crate::owner_references(
            "apps/v1",
            "Deployment",
            naming::CONTROLLER_DEPLOYMENT_NAME,
            crate::controller_deployment_uid(&client, &namespace).await?,
        );
        let rsync_secret = get_or_create_secret_value(
            &client,
            &namespace,
            naming::RSYNC_SHARED_SECRET_NAME,
            RSYNC_SECRET_KEY,
            deployment_owner,
        )
        .await?;
        write_secret_file(&rsync_out_path, &rsync_secret)?;
        tracing::info!(%node, path = %rsync_out_path, "provisioned shared rsync module secret");

        // Per-node, unlike the module secret above — this node's own
        // device identity, used as `BackendNode.status.deviceId` instead
        // of the node name (see `main.rs`'s rsync `register_backend_node`
        // call). Only written when the module secret path is also
        // configured, matching the same "rsyncd is actually enabled here"
        // gate.
        if let Ok(identity_out_path) = std::env::var(rsync::env::RSYNC_IDENTITY_FILE) {
            let identity = get_or_create_secret_value(
                &client,
                &namespace,
                &naming::per_node_rsync_identity_secret_name(&node),
                RSYNC_IDENTITY_SECRET_KEY,
                node_owner,
            )
            .await?;
            write_secret_file(&identity_out_path, &identity)?;
            tracing::info!(%node, path = %identity_out_path, "provisioned per-node rsync identity token");
        }
    }
    Ok(())
}

/// Reuses an already-provisioned value across restarts. Only ever
/// generates a fresh one the first time this Secret doesn't exist yet.
/// Generic over which secret (`name`/`key`/`owner_references`) so
/// Syncthing's API key, rsync's module secret, and rsync's per-node
/// identity token all share this get-or-create-with-owner logic instead of
/// duplicating it per secret. `owner_references` is precomputed by the
/// caller (via [`crate::owner_references`]) since the right owner scope
/// differs per secret — per-node for the first and third, install-wide for
/// the second — not something this function should decide.
async fn get_or_create_secret_value(
    client: &Client,
    namespace: &str,
    name: &str,
    key: &str,
    owner_references: Vec<k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference>,
) -> anyhow::Result<String> {
    let api = Api::<Secret>::namespaced(client.clone(), namespace);

    if let Some(existing) = api.get_opt(name).await? {
        if let Some(value) = existing
            .data
            .as_ref()
            .and_then(|data| data.get(key))
            .map(|bytes| String::from_utf8_lossy(&bytes.0).into_owned())
        {
            return Ok(value);
        }
    }

    let value = generate_secret_value()?;
    let secret = Secret {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            owner_references: if owner_references.is_empty() {
                None
            } else {
                Some(owner_references)
            },
            ..Default::default()
        },
        string_data: Some(std::collections::BTreeMap::from([(
            key.to_owned(),
            value.clone(),
        )])),
        type_: Some("Opaque".to_owned()),
        ..Default::default()
    };

    match api.create(&PostParams::default(), &secret).await {
        Ok(_) => Ok(value),
        // Lost a race with another provision run — read back what it wrote.
        Err(kube::Error::Api(response)) if response.code == 409 => {
            let existing = api.get(name).await?;
            existing
                .data
                .as_ref()
                .and_then(|data| data.get(key))
                .map(|bytes| String::from_utf8_lossy(&bytes.0).into_owned())
                .ok_or_else(|| anyhow::anyhow!("Secret {name} exists but has no {key} key"))
        }
        Err(error) => Err(error.into()),
    }
}

/// 16 random bytes, hex-encoded. Reads `/dev/urandom` directly rather than
/// pulling in a `rand` dependency — this binary only ever runs as a Linux
/// container.
fn generate_secret_value() -> anyhow::Result<String> {
    use std::io::Read;
    let mut bytes = [0u8; 16];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

/// Unix-only file permissions (mode 0600).
#[cfg(unix)]
fn write_secret_file(path: &str, value: &str) -> anyhow::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    if let Some(parent) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(value.as_bytes())?;
    Ok(())
}

#[cfg(not(unix))]
fn write_secret_file(path: &str, value: &str) -> anyhow::Result<()> {
    use std::io::Write;
    if let Some(parent) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::File::create(path)?.write_all(value.as_bytes())?;
    Ok(())
}
