//! Integration checks against a real `rsyncd` + `stunnel` pair (via
//! Docker), not a mocked `RsyncRunner`/`TunnelClient` — proves the tunnel
//! is actually required, not merely configured. See [`RsyncdContainer`].
//!
//! [`RsyncdContainer`]: mirrorvol_itest::RsyncdContainer

use std::sync::Arc;

use mirrorvol_backend::rsync::{RsyncBackend, SystemRsyncRunner, SystemStunnelClient};
use mirrorvol_backend::{LocalBackend, ReplicaConfig};
use mirrorvol_itest::RsyncdContainer;

fn replica_config(local_path: &str, source_address: &str) -> ReplicaConfig {
    ReplicaConfig {
        volume_id: RsyncdContainer::MODULE.to_owned(),
        local_path: local_path.to_owned(),
        active_peer_address: Some(source_address.to_owned()),
        generation: 1,
        ..Default::default()
    }
}

/// The real, end-to-end happy path this whole feature exists for: a pull
/// through a real `stunnel` client tunnel to a real `stunnel` server
/// fronting a real `rsyncd`, landing real bytes on disk, and `completion`
/// only reporting ready once they match exactly.
#[tokio::test]
#[ignore = "requires Docker, rsync, and stunnel"]
async fn completion_pulls_real_files_through_a_real_stunnel_tunnel() {
    let server = RsyncdContainer::start()
        .await
        .expect("start rsyncd+stunnel");
    let dest = tempfile::tempdir().expect("tempdir");
    let secret_file = dest.path().join("secret");
    std::fs::write(&secret_file, &server.secret).expect("write secret file");

    let backend = RsyncBackend::new(Arc::new(SystemRsyncRunner), dest.path())
        .with_secret_file(&secret_file)
        .with_stunnel(Arc::new(SystemStunnelClient::new(
            server.tls_port,
            &secret_file,
        )));

    backend
        .ensure_replica(&replica_config(
            dest.path().to_str().expect("utf8 tempdir path"),
            "127.0.0.1",
        ))
        .await
        .expect("ensure_replica");

    let ready = backend
        .completion(RsyncdContainer::MODULE)
        .await
        .expect("completion")
        .ready();
    assert!(
        ready,
        "completion should report ready after a clean full pull"
    );

    let pulled = std::fs::read_to_string(dest.path().join(RsyncdContainer::SEED_FILE))
        .expect("read pulled file");
    assert_eq!(pulled, RsyncdContainer::SEED_CONTENTS);
}

/// The negative half of the same guarantee: `rsyncd`'s own port is never
/// published by the container at all (see `RsyncdContainer`'s doc
/// comment) — proving there is no plaintext fallback for a client to fall
/// back to, structurally, not just by omission in this backend's own
/// argument-building.
#[tokio::test]
#[ignore = "requires Docker"]
async fn rsyncd_itself_is_never_reachable_without_the_tunnel() {
    let server = RsyncdContainer::start()
        .await
        .expect("start rsyncd+stunnel");
    assert!(
        !server.has_published_port(873),
        "rsyncd's plaintext port must never be published — only stunnel's TLS port should be reachable"
    );
}

/// A client that connects straight to the published port without speaking
/// TLS/PSK first (i.e. as if the tunnel step were skipped) must not get
/// anything usable back — proves the published port is genuinely a TLS
/// tunnel endpoint, not a permissive proxy that happens to also accept
/// bare rsync traffic.
#[tokio::test]
#[ignore = "requires Docker, rsync"]
async fn a_pull_that_skips_the_tunnel_and_hits_the_published_port_directly_fails() {
    let server = RsyncdContainer::start()
        .await
        .expect("start rsyncd+stunnel");
    let dest = tempfile::tempdir().expect("tempdir");
    let secret_file = dest.path().join("secret");
    std::fs::write(&secret_file, &server.secret).expect("write secret file");

    // No `.with_stunnel(...)` — this backend will build a plain
    // `rsync://` URL straight at the published (TLS) port, exactly what a
    // misconfigured/downgraded client would do.
    let backend =
        RsyncBackend::new(Arc::new(SystemRsyncRunner), dest.path()).with_secret_file(&secret_file);

    backend
        .ensure_replica(&replica_config(
            dest.path().to_str().expect("utf8 tempdir path"),
            &format!("127.0.0.1:{}", server.tls_port),
        ))
        .await
        .expect("ensure_replica");

    let result = backend.completion(RsyncdContainer::MODULE).await;
    let ready = result.map(|status| status.ready()).unwrap_or(false);
    assert!(
        !ready,
        "a plaintext rsync attempt against the TLS port must never succeed"
    );
    assert!(
        !dest.path().join(RsyncdContainer::SEED_FILE).exists(),
        "no file content should have transferred without a real tunnel"
    );
}
