use std::time::Duration;

use kube::api::{Api, DeleteParams, PostParams};
use mirrorvol_api::MirroredVolume;
use mirrorvol_itest::{
    candidate_node_names, fixtures, test_client, unique_name, wait_until, TestNamespace,
};

const NAMESPACE: &str = "mirrorvol";

const CONDITION_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_secs(1);

async fn condition_appears(
    volumes: &Api<MirroredVolume>,
    name: &str,
    type_: &str,
    reason: &str,
) -> bool {
    wait_until(CONDITION_TIMEOUT, POLL_INTERVAL, || async {
        volumes
            .get(name)
            .await
            .ok()
            .and_then(|volume| volume.status)
            .is_some_and(|status| {
                status
                    .conditions
                    .iter()
                    .any(|condition| condition.type_ == type_ && condition.reason == reason)
            })
    })
    .await
}

#[tokio::test]
#[ignore = "requires a fully deployed cluster (task deploy:full / task test:e2e)"]
async fn the_real_controller_surfaces_an_admission_rejection_on_status() {
    let client = test_client().await.expect("cluster reachable");
    let namespace = TestNamespace::create(client.clone(), "itest-admission")
        .await
        .expect("create namespace");

    Api::<k8s_openapi::api::apps::v1::Deployment>::namespaced(client.clone(), &namespace.name)
        .create(
            &PostParams::default(),
            &fixtures::app_deployment("example-app", &namespace.name),
        )
        .await
        .expect("create target app deployment");

    let volumes = Api::<MirroredVolume>::namespaced(client, &namespace.name);

    volumes
        .create(
            &PostParams::default(),
            &fixtures::mirrored_volume("too-few-candidates", &["node-a"], "node-a"),
        )
        .await
        .expect("create invalid volume");

    assert!(
        condition_appears(
            &volumes,
            "too-few-candidates",
            "AdmissionBlocked",
            "InsufficientCandidates"
        )
        .await,
        "the real controller never patched an AdmissionBlocked condition onto \
         status within {CONDITION_TIMEOUT:?}"
    );

    // Never acted on: no promotion attempt leaves a trace either.
    let volume = volumes.get("too-few-candidates").await.expect("get volume");
    let status = volume.status.expect("status");
    assert!(status.active.is_none());
    assert!(status.operation.is_none());
}

#[tokio::test]
#[ignore = "requires a fully deployed cluster (task deploy:full / task test:e2e)"]
async fn the_real_controller_rejects_a_sibling_volume_owning_the_same_workload_volume() {
    let client = test_client().await.expect("cluster reachable");

    let app_name = unique_name("e2e-reconcile-conditions-app");
    let first_name = unique_name("e2e-reconcile-conditions-first");
    let second_name = unique_name("e2e-reconcile-conditions-second");

    Api::<k8s_openapi::api::apps::v1::Deployment>::namespaced(client.clone(), NAMESPACE)
        .create(
            &PostParams::default(),
            &fixtures::app_deployment(&app_name, NAMESPACE),
        )
        .await
        .expect("create target app deployment");

    struct Cleanup {
        client: kube::Client,
        app_name: String,
        first_name: String,
        second_name: String,
    }
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let client = self.client.clone();
            let (app_name, first_name, second_name) = (
                self.app_name.clone(),
                self.first_name.clone(),
                self.second_name.clone(),
            );
            tokio::spawn(async move {
                let _ = Api::<k8s_openapi::api::apps::v1::Deployment>::namespaced(
                    client.clone(),
                    NAMESPACE,
                )
                .delete(&app_name, &DeleteParams::default())
                .await;
                let volumes = Api::<MirroredVolume>::namespaced(client, NAMESPACE);
                let _ = volumes.delete(&first_name, &DeleteParams::default()).await;
                let _ = volumes.delete(&second_name, &DeleteParams::default()).await;
            });
        }
    }
    let _cleanup = Cleanup {
        client: client.clone(),
        app_name: app_name.clone(),
        first_name: first_name.clone(),
        second_name: second_name.clone(),
    };

    let volumes = Api::<MirroredVolume>::namespaced(client.clone(), NAMESPACE);

    let nodes = candidate_node_names(&client)
        .await
        .expect("list candidate nodes");
    let candidates: Vec<&str> = nodes.iter().take(2).map(String::as_str).collect();
    assert!(
        candidates.len() == 2,
        "need at least two real candidate nodes, found {}",
        candidates.len()
    );

    // Two MirroredVolumes naming the same (workload.name, workload.volumeName)
    // pair
    let mut first = fixtures::mirrored_volume(&first_name, &candidates, candidates[0]);
    first.spec.workload.name = app_name.clone();
    volumes
        .create(&PostParams::default(), &first)
        .await
        .expect("create first volume");
    let mut second = fixtures::mirrored_volume(&second_name, &candidates, candidates[0]);
    second.spec.workload.name = app_name.clone();
    volumes
        .create(&PostParams::default(), &second)
        .await
        .expect("create second volume");

    assert!(
        condition_appears(
            &volumes,
            &second_name,
            "AdmissionBlocked",
            "OwnedByAnotherVolume"
        )
        .await,
        "the real controller never patched an AdmissionBlocked condition onto \
         the sibling volume within {CONDITION_TIMEOUT:?}"
    );
}
