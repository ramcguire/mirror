//! Integration checks against real Syncthing instances (via Docker), not a
//! wiremock stand-in — for real-protocol behavior a mock can't catch. See
//! `mirrorvol_itest::SyncthingContainer`'s doc comment for why this exists.

use mirrorvol_backend::syncthing::{fetch_device_id, LocalEndpoint, SyncthingBackend};
use mirrorvol_backend::{LocalBackend, ReplicaConfig};
use mirrorvol_itest::SyncthingContainer;

fn replica_config(peer_device_id: &str) -> ReplicaConfig {
    ReplicaConfig {
        volume_id: "itest-volume".to_owned(),
        // Lives inside the Syncthing container's own filesystem, not this
        // test process's — ensure_replica only ever sends it as config, it
        // never reads/writes it itself.
        local_path: "/tmp/itest-volume".to_owned(),
        peer_device_ids: vec![peer_device_id.to_owned()],
        // No addresses: these are ephemeral Docker hosts, not stable
        // candidate nodes — fine, since these tests only assert REST-level
        // device/folder registration, not a real sync round-trip.
        peer_addresses: Default::default(),
        generation: 1,
        ignore_patterns: vec![],
        active_peer_address: None,
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn ensure_replica_registers_an_unknown_peer_device_with_a_real_syncthing() {
    let writer = SyncthingContainer::start()
        .await
        .expect("start writer Syncthing");
    let peer = SyncthingContainer::start()
        .await
        .expect("start peer Syncthing");

    let http = reqwest::Client::new();
    let peer_device_id = fetch_device_id(&http, &peer.base_url, &peer.api_key)
        .await
        .expect("peer device id");

    // Before: the writer's real Syncthing instance has never heard of this
    // device — `ensure_replica`'s whole job here is to fix that itself,
    // it's not something a folder's `devices` list can do on its own.
    let before = http
        .get(format!(
            "{}/rest/config/devices/{peer_device_id}",
            writer.base_url
        ))
        .header("X-API-Key", &writer.api_key)
        .send()
        .await
        .expect("query devices before");
    assert_eq!(before.status(), reqwest::StatusCode::NOT_FOUND);

    let data_root = tempfile::tempdir().expect("tempdir");
    let backend = SyncthingBackend::new(
        LocalEndpoint {
            base_url: writer.base_url.clone(),
            api_key: writer.api_key.clone(),
        },
        data_root.path(),
    );
    backend
        .ensure_replica(&replica_config(&peer_device_id))
        .await
        .expect("ensure_replica");

    let after = http
        .get(format!(
            "{}/rest/config/devices/{peer_device_id}",
            writer.base_url
        ))
        .header("X-API-Key", &writer.api_key)
        .send()
        .await
        .expect("query devices after");
    assert!(
        after.status().is_success(),
        "peer device was not registered"
    );

    let folder: serde_json::Value = http
        .get(format!(
            "{}/rest/config/folders/itest-volume",
            writer.base_url
        ))
        .header("X-API-Key", &writer.api_key)
        .send()
        .await
        .expect("query folder")
        .json()
        .await
        .expect("folder json");
    assert_eq!(folder["type"], "receiveonly");
    assert!(
        folder["devices"]
            .as_array()
            .expect("devices array")
            .iter()
            .any(|device| device["deviceID"] == peer_device_id),
        "folder does not reference the peer device: {folder}"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn ensure_replica_does_not_overwrite_an_already_registered_device() {
    let writer = SyncthingContainer::start()
        .await
        .expect("start writer Syncthing");
    let peer = SyncthingContainer::start()
        .await
        .expect("start peer Syncthing");

    let http = reqwest::Client::new();
    let peer_device_id = fetch_device_id(&http, &peer.base_url, &peer.api_key)
        .await
        .expect("peer device id");

    // Register the device by hand first, with an operator-chosen name.
    // ensure_replica must leave it alone on its own subsequent call, not
    // clobber it — that's the whole reason it GETs before it PUTs.
    let put = http
        .put(format!(
            "{}/rest/config/devices/{peer_device_id}",
            writer.base_url
        ))
        .header("X-API-Key", &writer.api_key)
        .json(&serde_json::json!({
            "deviceID": peer_device_id,
            "name": "operator-named-peer",
        }))
        .send()
        .await
        .expect("manual device registration");
    assert!(put.status().is_success());

    let data_root = tempfile::tempdir().expect("tempdir");
    let backend = SyncthingBackend::new(
        LocalEndpoint {
            base_url: writer.base_url.clone(),
            api_key: writer.api_key.clone(),
        },
        data_root.path(),
    );
    backend
        .ensure_replica(&replica_config(&peer_device_id))
        .await
        .expect("ensure_replica");

    let device: serde_json::Value = http
        .get(format!(
            "{}/rest/config/devices/{peer_device_id}",
            writer.base_url
        ))
        .header("X-API-Key", &writer.api_key)
        .send()
        .await
        .expect("query device")
        .json()
        .await
        .expect("device json");
    assert_eq!(device["name"], "operator-named-peer");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn set_ignore_patterns_is_accepted_by_a_real_syncthing_instance() {
    // A wiremock stand-in mirrors whatever HTTP verb the code under test
    // sends — it can't catch a wrong verb, only a real server can (this
    // caught PUT vs the real POST/rest/db/ignores requires, discovered via
    // a real e2e run against a real Syncthing instance, not this test
    // originally — added after the fact so a future regression here fails
    // fast, in seconds, not only after a full e2e cycle).
    let writer = SyncthingContainer::start()
        .await
        .expect("start writer Syncthing");
    let peer = SyncthingContainer::start()
        .await
        .expect("start peer Syncthing");
    let http = reqwest::Client::new();
    let peer_device_id = fetch_device_id(&http, &peer.base_url, &peer.api_key)
        .await
        .expect("peer device id");

    let data_root = tempfile::tempdir().expect("tempdir");
    let backend = SyncthingBackend::new(
        LocalEndpoint {
            base_url: writer.base_url.clone(),
            api_key: writer.api_key.clone(),
        },
        data_root.path(),
    );
    // The folder must exist before /rest/db/ignores will accept anything
    // for it — same prerequisite ensure_replica's own real-cluster callers
    // always satisfy first.
    backend
        .ensure_replica(&replica_config(&peer_device_id))
        .await
        .expect("ensure_replica");

    backend
        .set_ignore_patterns("itest-volume", &["*.tmp".to_owned(), "cache/".to_owned()])
        .await
        .expect("set_ignore_patterns");

    let ignores: serde_json::Value = http
        .get(format!(
            "{}/rest/db/ignores?folder=itest-volume",
            writer.base_url
        ))
        .header("X-API-Key", &writer.api_key)
        .send()
        .await
        .expect("query ignores")
        .json()
        .await
        .expect("ignores json");
    assert_eq!(ignores["ignore"], serde_json::json!(["*.tmp", "cache/"]));
}
