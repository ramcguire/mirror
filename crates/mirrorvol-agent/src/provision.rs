//! `mirrorvol-agent provision` — initContainer step run before `syncthing`
//! starts. Get-or-creates this node's Syncthing API key
//! [`Secret`](k8s_openapi::api::core::v1::Secret) and writes it to a
//! shared `emptyDir` file, since a DaemonSet's pod template can't point
//! `secretKeyRef.name` at a per-node Secret.
//!
//! Cert-manager device-cert provisioning is not implemented yet; this only handles
//! the API key.

use k8s_openapi::api::core::v1::Secret;
use kube::api::{Api, ObjectMeta, PostParams};
use kube::Client;
use mirrorvol_api::naming;

const API_KEY_SECRET_KEY: &str = "apikey";

pub async fn run() -> anyhow::Result<()> {
    let node = std::env::var(naming::env::NODE_NAME)?;
    let namespace = std::env::var(naming::env::POD_NAMESPACE)
        .unwrap_or_else(|_| naming::env::POD_NAMESPACE_DEFAULT.to_owned());
    let out_path =
        std::env::var("MIRRORVOL_APIKEY_FILE").unwrap_or_else(|_| "/shared/apikey".to_owned());

    let client = Client::try_default().await?;
    let api_key = get_or_create_api_key(&client, &namespace, &node).await?;

    write_api_key_file(&out_path, &api_key)?;
    tracing::info!(%node, path = %out_path, "provisioned per-node Syncthing API key");
    Ok(())
}

/// Reuses an already-provisioned key across restarts. Only ever
/// generates a fresh one the first time this node's Secret
/// doesn't exist yet.
async fn get_or_create_api_key(
    client: &Client,
    namespace: &str,
    node: &str,
) -> anyhow::Result<String> {
    let api = Api::<Secret>::namespaced(client.clone(), namespace);
    let name = mirrorvol_api::naming::per_node_secret_name(node);

    if let Some(existing) = api.get_opt(&name).await? {
        if let Some(value) = existing
            .data
            .as_ref()
            .and_then(|data| data.get(API_KEY_SECRET_KEY))
            .map(|bytes| String::from_utf8_lossy(&bytes.0).into_owned())
        {
            return Ok(value);
        }
    }

    let api_key = generate_api_key()?;
    // Owned by the Node, not the Pod
    let owner_references = match crate::node_uid(client, node).await? {
        Some(uid) => vec![
            k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
                api_version: "v1".to_owned(),
                kind: "Node".to_owned(),
                name: node.to_owned(),
                uid,
                controller: Some(true),
                block_owner_deletion: Some(true),
            },
        ],
        None => Vec::new(),
    };
    let secret = Secret {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            owner_references: if owner_references.is_empty() {
                None
            } else {
                Some(owner_references)
            },
            ..Default::default()
        },
        string_data: Some(std::collections::BTreeMap::from([(
            API_KEY_SECRET_KEY.to_owned(),
            api_key.clone(),
        )])),
        type_: Some("Opaque".to_owned()),
        ..Default::default()
    };

    match api.create(&PostParams::default(), &secret).await {
        Ok(_) => Ok(api_key),
        // Lost a race with another provision run — read back what it wrote.
        Err(kube::Error::Api(response)) if response.code == 409 => {
            let existing = api.get(&name).await?;
            existing
                .data
                .as_ref()
                .and_then(|data| data.get(API_KEY_SECRET_KEY))
                .map(|bytes| String::from_utf8_lossy(&bytes.0).into_owned())
                .ok_or_else(|| {
                    anyhow::anyhow!("Secret {name} exists but has no {API_KEY_SECRET_KEY} key")
                })
        }
        Err(error) => Err(error.into()),
    }
}

/// 16 random bytes, hex-encoded. Reads `/dev/urandom` directly rather than
/// pulling in a `rand` dependency — this binary only ever runs as a Linux
/// container.
fn generate_api_key() -> anyhow::Result<String> {
    use std::io::Read;
    let mut bytes = [0u8; 16];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

/// Unix-only file permissions (mode 0600).
#[cfg(unix)]
fn write_api_key_file(path: &str, api_key: &str) -> anyhow::Result<()> {
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
    file.write_all(api_key.as_bytes())?;
    Ok(())
}

#[cfg(not(unix))]
fn write_api_key_file(path: &str, api_key: &str) -> anyhow::Result<()> {
    use std::io::Write;
    if let Some(parent) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::File::create(path)?.write_all(api_key.as_bytes())?;
    Ok(())
}
