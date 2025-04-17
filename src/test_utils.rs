use async_trait::async_trait;
use google_cloud_gax::grpc::Status;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_token::{TokenSource, TokenSourceProvider};

use crate::{
    consts::*, failover_manager::FailoverManager, google::compute::ComputeClient,
    google::pubsub::PubSubClient, Environment,
};

use rand::{distributions::Alphanumeric, Rng};
use std::fs;
use std::sync::Arc;

#[derive(Debug)]
pub struct FakeTokenSourceProvider {}

#[derive(Debug)]
pub struct FakeTokenSource {}

impl TokenSourceProvider for FakeTokenSourceProvider {
    fn token_source(&self) -> Arc<dyn TokenSource> {
        Arc::new(FakeTokenSource {})
    }
}

#[async_trait]
impl TokenSource for FakeTokenSource {
    async fn token(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let random_string: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();

        Ok(format!("Bearer {0}", random_string))
    }
}

pub async fn dns_mock_server(project_id: &str) -> mockito::ServerGuard {
    // Request a new server from the pool
    let mut server = mockito::Server::new_async().await;

    // unfiltered
    server
        .mock(
            "GET",
            format!(
                "/dns/v1/projects/{}/managedZones/base-postgres-persistence-dns-core-network-onesignal/rrsets",
                project_id
            )
            .as_str(),
        )
        .with_status(200)
        .with_body(
            fs::read_to_string("src/fixtures/dns/simple.json")
                .unwrap()
                .as_str(),
        )
        .create();

    server
        .mock(
            "GET",
            "/dns/v1/projects/shared-infra-onesignal/managedZones/base-postgres-persistence-test-dns-core-network-onesignal/rrsets?name=rw.x50.app.postgres.persistence.test.onesignal.lan.&type=A",
        )
        .with_status(200)
        .with_body(
            fs::read_to_string("src/fixtures/dns/simple_test.json")
                .unwrap()
                .as_str(),
        )
        .create();

    // With filter
    server
        .mock(
            "GET",
            format!(
                "/dns/v1/projects/{}/managedZones/base-postgres-persistence-dns-core-network-onesignal/rrsets?name=ro.p00.x00.subscription.postgres.persistence.onesignal.lan.&type=A",
                project_id
            )
            .as_str(),
        )
        .with_status(200)
        .with_body(
            fs::read_to_string("src/fixtures/dns/with_name.json")
                .unwrap()
                .as_str(),
        )
        .create();
    // With filter

    server
        .mock(
            "GET",
            format!(
                "/dns/v1/projects/{}/managedZones/base-postgres-persistence-test-dns-core-network-onesignal/rrsets?name=rw.x00.subscription.postgres.persistence.test.onesignal.lan.&type=A",
                project_id
            )
            .as_str(),
        )
        .with_status(200)
        .with_body(
            fs::read_to_string("src/fixtures/dns/with_name_localhost.json")
                .unwrap()
                .as_str(),
        )
        .create();

    server
        .mock(
            "PATCH",
            format!(
                "/dns/v1/projects/{}/managedZones/base-postgres-persistence-staging-dns-core-network-onesignal/rrsets/test-dns-api.app.postgres.persistence.staging.onesignal.lan./A",
                project_id,
            )
            .as_str(),
        )
        .with_status(200)
        .with_body(
            fs::read_to_string("src/fixtures/dns/update_response.json")
                .unwrap()
                .as_str(),
        )
        .create();

    server
        .mock(
            "PATCH",
            format!(
                "/dns/v1/projects/{}/managedZones/base-postgres-persistence-test-dns-core-network-onesignal/rrsets/rw.x00.subscription.postgres.persistence.test.onesignal.lan./A",
                project_id,
            )
            .as_str(),
        )
        .with_status(200)
        .with_body(
            fs::read_to_string("src/fixtures/dns/update_response.json")
                .unwrap()
                .as_str(),
        )
        .create();

    server
}

pub async fn compute_mock_server() -> mockito::ServerGuard {
    // Request a new server from the pool
    let mut server = mockito::Server::new_async().await;

    // unfiltered
    let _ = server
        .mock(
            "GET",
            format!(
                "/compute/v1/projects/{}/zones/europe-west4-a/instances",
                COMPUTE_PROJECT_ID
            )
            .as_str(),
        )
        .with_status(200)
        .with_body(
            fs::read_to_string("src/fixtures/compute/simple.json")
                .unwrap()
                .as_str(),
        )
        .create();

    // filtered with is a potgres server
    let zones = vec!["europe-west4-a", "europe-west4-b", "europe-west4-c"];
    let postgres_server = "(labels.ansible_environment:production)%20AND%20(labels.ansible_group:instance_postgres_persistence)";
    for zone in zones {
        let fixture_data = fs::read_to_string(format!(
            "src/fixtures/compute/postgres_servers-{}.json",
            zone
        ))
        .expect("Unable to read fixture file");
        let _ = server
            .mock(
                "GET",
                format!(
                    "/compute/v1/projects/{}/zones/{}/instances?filter={}",
                    COMPUTE_PROJECT_ID, zone, postgres_server
                )
                .as_str(),
            )
            .with_status(200)
            .with_body(fixture_data.as_str())
            .create();

        let fixture_data =
            fs::read_to_string(format!("src/fixtures/compute/get_instance-{}.json", zone))
                .expect("Unable to read fixture file");

        let status = if zone == "europe-west4-b" { 200 } else { 404 };
        let _ = server
            .mock(
                "GET",
                format!(
                    "/compute/v1/projects/{}/zones/{}/instances/a179c06f-p00-xf0-sub-postgres-persistence-onesignal",
                    COMPUTE_PROJECT_ID,
                    zone,
                )
                .as_str(),
            )
            .with_status(status)
            .with_body(fixture_data.as_str())
            .create();

        let fixture_data = fs::read_to_string(format!(
            "src/fixtures/compute/get_instance-missing-instance-{}.json",
            zone
        ))
        .expect("Unable to read fixture file");

        let _ = server
            .mock(
                "GET",
                format!(
                    "/compute/v1/projects/{}/zones/{}/instances/missing-instance",
                    COMPUTE_PROJECT_ID, zone,
                )
                .as_str(),
            )
            .with_status(404)
            .with_body(fixture_data.as_str())
            .create();
    }
    let fixture_data =
        fs::read_to_string("src/fixtures/compute/get_instance-localhost-europe-west4-b.json")
            .expect("Unable to read fixture file");

    let _ = server
        .mock(
            "GET",
            format!(
                "/compute/v1/projects/{}/zones/europe-west4-b/instances/0001-test-onesignal",
                COMPUTE_PROJECT_ID,
            )
            .as_str(),
        )
        .with_status(200)
        .with_body(fixture_data.as_str())
        .create();

    let fixture_data =
        fs::read_to_string("src/fixtures/compute/get_instance-primary-europe-west4-b.json")
            .expect("Unable to read fixture file");

    let _ = server
            .mock(
                "GET",
                format!(
                    "/compute/v1/projects/{}/zones/{}/instances/a26389e1-x50-app-postgres-persistence-test-onesignal",
                    COMPUTE_PROJECT_ID,
                    "europe-west4-b",
                )
                .as_str(),
            )
            .with_status(200)
            .with_body(fixture_data.as_str())
            .create();
    server
}

// Publish a message to a local pubsub emulator
// using a random topic that will be returned.
pub async fn publish_to_local_pubsub(
    client: &PubSubClient,
    topic_name: &str,
    msg: PubsubMessage,
) -> Result<(), Status> {
    // A random topic name.

    let topic = client.inner.topic(topic_name);
    if !topic.exists(None).await? {
        topic.create(None, None).await?;
    }

    // Start publisher.
    let mut publisher = topic.new_publisher(None);

    // Send a message. There are also `publish_bulk` and `publish_immediately` methods.
    publisher.publish_immediately(vec![msg], None).await?;

    // The get method blocks until a server-generated ID or an error is returned for the published message.
    publisher.shutdown().await;

    Ok(())
}

pub fn get_pubsub_message(fixture: &str) -> PubsubMessage {
    let msg = fs::read_to_string(format!("src/fixtures/pubsub/{}.json", fixture)).unwrap();

    PubsubMessage {
        data: msg.into_bytes(),
        ordering_key: "order".into(),
        ..Default::default()
    }
}

pub struct FakeFailoverManagerWrapper {
    pub failover_manager: FailoverManager<FakeTokenSourceProvider>,
    _compute_mock_server: mockito::ServerGuard,
    pub subscription_name: String,
    pub topic_name: String,
}

impl FakeFailoverManagerWrapper {
    pub async fn new(environment: Environment, dry_run: bool) -> Self {
        let mut compute = ComputeClient::new(FakeTokenSourceProvider {});
        let compute_mock_server = compute_mock_server().await;
        compute.set_base_url(&compute_mock_server.url());

        let subscription_name = format!("test-subscription-{}", rand::thread_rng().gen::<u64>());
        let topic_name = format!("test-topic-{}", rand::thread_rng().gen::<u64>());
        let pubsub_client = PubSubClient::new(FakeTokenSourceProvider {}).await.unwrap();
        pubsub_client
            .create_subscription(&subscription_name, &topic_name)
            .await
            .unwrap();

        let failover_manager = FailoverManager::new(
            compute,
            pubsub_client,
            dry_run,
            environment,
            "db-password".to_string(),
        );

        FakeFailoverManagerWrapper {
            failover_manager,
            _compute_mock_server: compute_mock_server,
            subscription_name,
            topic_name,
        }
    }
}
