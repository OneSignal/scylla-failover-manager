use std::env::var;
use valuable::Valuable;

use google_cloud_auth::token::DefaultTokenSourceProvider;
use google_cloud_gax::conn::Environment;
use google_cloud_gax::grpc::Status;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::subscription::SubscriptionConfig;
use google_cloud_token::TokenSourceProvider;

#[derive(Debug, serde::Deserialize, Valuable)]
pub struct ProtoPayload {
    #[serde(rename = "resourceName")]
    pub resource_name: String,
}

#[derive(Debug, serde::Deserialize, Valuable)]
pub struct Operation {
    pub producer: String,
}

#[derive(Debug, serde::Deserialize, Valuable)]
pub struct LogEntry {
    #[serde(rename = "protoPayload")]
    proto_payload: ProtoPayload,
    pub operation: Operation,
}

impl LogEntry {
    pub fn instance_name(&self) -> Option<String> {
        self.proto_payload
            .resource_name
            .split('/')
            .last()
            .map(|v| v.to_string())
    }
    pub fn zone(&self) -> Option<String> {
        self.proto_payload
            .resource_name
            .split('/')
            .nth(3)
            .map(|v| v.to_string())
    }
}

#[derive(Debug)]
pub enum PubSubError {
    SerdeError(serde_json::Error),
    NoMessage,
    ACKError(google_cloud_gax::grpc::Status),
}

#[derive(Clone)]
pub struct PubSubClient {
    pub inner: Client,
}

impl PubSubClient {
    pub async fn new<T: TokenSourceProvider + 'static>(
        token_provider: T,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let pubsub_config = ClientConfig {
            project_id: Some(crate::consts::PUBSUB_PROJECT_ID.to_string()),
            environment: match var("PUBSUB_EMULATOR_HOST").ok() {
                Some(v) => Environment::Emulator(v),
                None => Environment::GoogleCloud(Box::new(token_provider)),
            },
            ..Default::default()
        };

        let client = Client::new(pubsub_config).await?;
        Ok(Self { inner: client })
    }

    pub async fn default_token_source_provider(
    ) -> Result<DefaultTokenSourceProvider, google_cloud_auth::error::Error> {
        let scopes: [&str; 2] = [
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/pubsub",
        ];

        let auth_config = google_cloud_auth::project::Config::default()
            .with_audience("https://pubsub.googleapis.com/")
            .with_scopes(&scopes);

        DefaultTokenSourceProvider::new(auth_config).await
    }

    pub async fn create_subscription(&self, subscription: &str, topic: &str) -> Result<(), Status> {
        let topic = self.inner.topic(topic);

        if !topic.exists(None).await? {
            topic.create(None, None).await?;
        }

        let config = SubscriptionConfig {
            enable_exactly_once_delivery: true,
            enable_message_ordering: true,
            ..Default::default()
        };
        let subscription = self.inner.subscription(subscription);
        if !subscription.exists(None).await? {
            subscription
                .create(topic.fully_qualified_name(), config, None)
                .await?;
        }
        Ok(())
    }

    pub async fn delete_subscription(&self, subscription: &str) -> Result<(), Status> {
        let subscription = self.inner.subscription(subscription);
        subscription.delete(None).await?;
        Ok(())
    }

    pub async fn read(&self, subscription: &str) -> Result<LogEntry, PubSubError> {
        let subscription = self.inner.subscription(subscription);
        match subscription.pull(1, None).await.unwrap().pop() {
            Some(message) => {
                if let Err(e) = message.ack().await {
                    return Err(PubSubError::ACKError(e));
                }
                match serde_json::from_slice::<LogEntry>(&message.message.data) {
                    Ok(log_entry) => Ok(log_entry),
                    Err(e) => Err(PubSubError::SerdeError(e)),
                }
            }
            None => Err(PubSubError::NoMessage),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::*;
    use rand::Rng;

    #[tokio::test]
    async fn test_pubsub_client() {
        let client = PubSubClient::new(FakeTokenSourceProvider {}).await.unwrap();

        let subscription_name = format!("test-subscription-{}", rand::thread_rng().gen::<u64>());
        let topic_name = format!("test-topic-{}", rand::thread_rng().gen::<u64>());

        client
            .create_subscription(&subscription_name, &topic_name)
            .await
            .unwrap();

        publish_to_local_pubsub(
            &client,
            &topic_name,
            get_pubsub_message("host_error_log_entry"),
        )
        .await
        .unwrap();

        let data = client.read(&subscription_name).await;
        assert!(data.is_ok());
        assert_eq!(
            data.unwrap().operation.producer,
            "compute.instances.hostError"
        );
    }
}
