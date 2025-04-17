use crate::google::{
    compute::{ComputeClient, Instance},
    pubsub::{PubSubClient, PubSubError},
};
use postgres_cluster::tokio_postgres;
use valuable::Valuable;

use crate::Environment;

use google_cloud_token::TokenSourceProvider;
use logging_base::{error, info, warn};

pub enum FailOverManagerError {
    ComputeError(Box<dyn std::error::Error + Send + Sync>),
    DNSError(Box<dyn std::error::Error + Send + Sync>),
    HostNotFound(String),
    HostNotPrimary,
    EnvironmentDiffer,
    EnvironmentNotFound,
    PubSubError,
    LogProcessingError,
    AlreadyPromoted,
    NoMessageReceived,
    PostgresError(tokio_postgres::Error),
}

impl std::fmt::Debug for FailOverManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FailOverManagerError::ComputeError(e) => write!(f, "ComputeError: {}", e),
            FailOverManagerError::DNSError(e) => write!(f, "DNSError: {}", e),
            FailOverManagerError::HostNotFound(e) => write!(f, "HostNotFound: {}", e),
            FailOverManagerError::HostNotPrimary => write!(f, "HostNotPrimary"),
            FailOverManagerError::EnvironmentDiffer => write!(f, "EnvironmentDiffer"),
            FailOverManagerError::EnvironmentNotFound => write!(f, "EnvironmentNotFound"),
            FailOverManagerError::PostgresError(e) => write!(f, "PostgresError: {}", e),
            FailOverManagerError::PubSubError => write!(f, "PubSubError"),
            FailOverManagerError::LogProcessingError => write!(f, "LogProcessingError"),
            FailOverManagerError::AlreadyPromoted => write!(f, "AlreadyPromoted"),
            FailOverManagerError::NoMessageReceived => write!(f, "NoMessageReceived"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HostError {
    pub instance_name: String,
    pub zone: String,
}

pub struct FailoverManager<T: TokenSourceProvider> {
    compute_client: ComputeClient<T>,
    pubsub_client: PubSubClient,
    dry_run: bool,
    environment: Environment,
    db_password: String,
}

impl<T: TokenSourceProvider> FailoverManager<T> {
    /// Creates a new instance of FailoverManager.
    /// # Arguments
    /// * `compute_client` - A ComputeClient instance.
    /// * `dns_client` - A DNSClient instance.
    /// * `pubsub_client` - A PubSubClient instance.
    /// * `dry_run` - A boolean indicating if the operation should be executed or not.
    /// * `environment` - The environment to operate on.
    /// * `db_password` - The password to use for the database.
    /// # Returns
    /// A FailoverManager instance.
    pub fn new(
        compute_client: ComputeClient<T>,
        pubsub_client: PubSubClient,
        dry_run: bool,
        environment: Environment,
        db_password: String,
    ) -> Self {
        FailoverManager {
            compute_client,
            pubsub_client,
            dry_run,
            environment,
            db_password,
        }
    }

    /// Reads from the pubsub subscription and returns the host error event.
    /// if found or error otherwise.
    /// # Arguments
    /// * `subscription_name` - The name of the subscription to read from.
    /// # Returns
    /// A HostError instance if found or error otherwise.
    pub async fn fetch_host_error(
        &self,
        subscription_name: &str,
    ) -> Result<HostError, FailOverManagerError> {
        let log_entry = match self.pubsub_client.read(subscription_name).await {
            Ok(entry) => entry,
            Err(error) => {
                match error {
                    PubSubError::SerdeError(e) => {
                        error!(exception = e.to_string(), "Failed to deserialize message.");
                    }
                    PubSubError::NoMessage => {
                        return Err(FailOverManagerError::NoMessageReceived);
                    }
                    PubSubError::ACKError(e) => {
                        error!(exception = e.to_string(), "Failed to acknowledge message.");
                    }
                };

                return Err(FailOverManagerError::PubSubError);
            }
        };

        info!(msg = format!("{:?}", log_entry), "Received message.");
        let instance_name = match log_entry.instance_name() {
            Some(instance_name) => instance_name,
            None => {
                error!("Instance name not found in log entry. Ignoring event");
                return Err(FailOverManagerError::LogProcessingError);
            }
        };
        let zone = match log_entry.zone() {
            Some(zone) => zone,
            None => {
                error!("Zone not found in log entry. Ignoring event");
                return Err(FailOverManagerError::LogProcessingError);
            }
        };
        if log_entry.operation.producer != "compute.instances.hostError" {
            warn!(
                log_entry = log_entry.as_value(),
                "Event received is not a host error event. Ignoring event"
            );
            return Err(FailOverManagerError::LogProcessingError);
        }
        Ok(HostError {
            instance_name,
            zone,
        })
    }

    /// Process the host error event.
    pub async fn process_host_error(
        &self,
        host_error: HostError,
    ) -> Result<(), FailOverManagerError> {
        let zone = host_error.zone.clone();
        let instance_name = host_error.instance_name.clone();
        let failed_instance = self.fetch_db_node(&instance_name, &zone).await?;
        // let failed_instance_group = self.fetch_instance_group(&failed_instance).await?;

        info!(
            "Processing host error for instance: {} in zone {}",
            instance_name, zone
        );

        Ok(())
    }

    // Returns a DBNode based on the instance and zone specified. This function
    // will return an error if the instance is not found, if the instance is not
    // in the correct environment, or if the instance is not a primary.
    async fn fetch_db_node(
        &self,
        instance_name: &str,
        zone: &str,
    ) -> Result<Instance, FailOverManagerError> {
        let instance = match self
            .compute_client
            .get_instance(instance_name, &[zone])
            .await
        {
            Ok(Some(instance)) => instance,
            Ok(None) => {
                error!(
                    instance = instance_name,
                    "Instance {} not found", instance_name
                );
                return Err(FailOverManagerError::HostNotFound(
                    instance_name.to_string(),
                ));
            }
            Err(e) => {
                error!(
                    error = e,
                    instance = instance_name,
                    "Error getting instance {}: {}",
                    instance_name,
                    e
                );
                return Err(FailOverManagerError::ComputeError(e));
            }
        };

        info!("labels:");
        for (key, value) in &instance.labels {
            info!("{}: {}", key, value);
        }

        // TODO: set up label pulling for scylla labels
        // match instance.labels.get("scylla.onesignal.io/environment") {
        //     Some(env) => {
        //         if self.environment.to_string() == "production" && env != "production" {
        //             error!(
        //                 instance = instance_name,
        //                 environment = env,
        //                 "Instance {} is in environment {}, not {}.",
        //                 instance_name,
        //                 env,
        //                 self.environment
        //             );
        //             return Err(FailOverManagerError::EnvironmentDiffer);
        //         }
        //     }
        //     None => {
        //         error!(
        //             instance = instance_name,
        //             "Instance {} has no environment label. Ignoring.", instance_name
        //         );
        //         return Err(FailOverManagerError::EnvironmentNotFound);
        //     }
        // };

        Ok(instance)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::*;

    #[tokio::test]
    async fn test_process_host_error_missing_instance() {
        let result = FakeFailoverManagerWrapper::new(Environment::Test, true)
            .await
            .failover_manager
            .process_host_error(HostError {
                instance_name: String::from("missing-instance"),
                zone: String::from("europe-west4-b"),
            })
            .await;
        let result = result.unwrap_err();
        assert!(matches!(result, FailOverManagerError::HostNotFound(_)));
    }

    #[tokio::test]
    async fn test_process_host_error_wrong_environment() {
        let result = FakeFailoverManagerWrapper::new(Environment::Staging, true)
            .await
            .failover_manager
            .process_host_error(HostError {
                instance_name: String::from("a179c06f-p00-xf0-sub-postgres-persistence-onesignal"),
                zone: String::from("europe-west4-b"),
            })
            .await;

        assert!(matches!(
            result,
            Err(FailOverManagerError::EnvironmentDiffer)
        ));
    }

    #[tokio::test]
    async fn test_process_host_error_is_not_primary() {
        let result = FakeFailoverManagerWrapper::new(Environment::Test, true)
            .await
            .failover_manager
            .process_host_error(HostError {
                instance_name: String::from("a179c06f-p00-xf0-sub-postgres-persistence-onesignal"),
                zone: String::from("europe-west4-b"),
            })
            .await;

        assert!(matches!(result, Err(FailOverManagerError::HostNotPrimary)));
    }

    #[tokio::test]
    async fn test_process_host_error_ok() {
        let result = FakeFailoverManagerWrapper::new(Environment::Test, true)
            .await
            .failover_manager
            .process_host_error(HostError {
                instance_name: String::from("0001-test-onesignal"),
                zone: String::from("europe-west4-b"),
            })
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_fetch_host_error() {
        let failover_manager_server =
            FakeFailoverManagerWrapper::new(Environment::Test, true).await;
        let pubsub_client = PubSubClient::new(FakeTokenSourceProvider {}).await.unwrap();

        publish_to_local_pubsub(
            &pubsub_client,
            &failover_manager_server.topic_name,
            get_pubsub_message("host_error_log_entry"),
        )
        .await
        .unwrap();

        let result = failover_manager_server
            .failover_manager
            .fetch_host_error(&failover_manager_server.subscription_name)
            .await;

        assert!(result.is_ok());

        let result = result.unwrap();
        assert_eq!(
            &result.instance_name,
            "e340142f-p00-xe0-sub-postgres-persistence-onesignal"
        );
        assert_eq!(&result.zone, "europe-west4-b");
    }

    #[tokio::test]
    async fn test_fetch_host_error_invalid_log_entry() {
        let failover_manager_server =
            FakeFailoverManagerWrapper::new(Environment::Test, true).await;
        let pubsub_client = PubSubClient::new(FakeTokenSourceProvider {}).await.unwrap();

        publish_to_local_pubsub(
            &pubsub_client,
            &failover_manager_server.topic_name,
            get_pubsub_message("invalid_host_entry"),
        )
        .await
        .unwrap();

        let result = failover_manager_server
            .failover_manager
            .fetch_host_error(&failover_manager_server.subscription_name)
            .await;

        assert!(result.is_err());
        assert!(matches!(result, Err(FailOverManagerError::PubSubError)));
    }

    #[tokio::test]
    async fn test_fetch_host_error_not_a_host_error() {
        let failover_manager_server =
            FakeFailoverManagerWrapper::new(Environment::Test, true).await;
        let pubsub_client = PubSubClient::new(FakeTokenSourceProvider {}).await.unwrap();

        publish_to_local_pubsub(
            &pubsub_client,
            &failover_manager_server.topic_name,
            get_pubsub_message("not_a_host_error"),
        )
        .await
        .unwrap();

        let result = failover_manager_server
            .failover_manager
            .fetch_host_error(&failover_manager_server.subscription_name)
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(FailOverManagerError::LogProcessingError)
        ));
    }
}
