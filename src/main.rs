use clap::Parser;
use rand::Rng;
use scylla_failover_manager::{
    google::{compute::ComputeClient, pubsub::PubSubClient},
    Environment,
};
use valuable::Valuable;

use google_cloud_auth::token::DefaultTokenSourceProvider;
use scylla_failover_manager::failover_manager::FailoverManager;
use std::sync::{Arc, Mutex};
use tokio::signal::unix::{signal, SignalKind};
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the topic to subscribe to for listenting to host error messages.
    #[arg(short, long, default_value = "persistence-scylla-host-error")]
    topic: String,

    /// Environment
    #[arg(short, long, default_value = "staging")]
    environment: Environment,

    /// Dry run
    #[arg(short, long)]
    dry_run: bool,

    /// Prometheus listen address
    #[arg(long, default_value = "0.0.0.0:9090")]
    prometheus_listen_addr: String,
}

#[derive(Debug, Clone, Valuable)]
struct Config {
    topic: String,
    environment: Environment,
    dry_run: bool,
    prometheus_listen_addr: String,
}

impl Config {
    pub fn new(
        topic: String,
        environment: Environment,
        dry_run: bool,
        prometheus_listen_addr: String,
    ) -> Self {
        Self {
            topic,
            environment,
            dry_run,
            prometheus_listen_addr,
        }
    }
}

impl From<Args> for Config {
    fn from(args: Args) -> Self {
        Self::new(
            args.topic,
            args.environment,
            args.dry_run,
            args.prometheus_listen_addr,
        )
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config: Config = Args::parse().into();

    // The RUST_TRACING environment variable is used to control the verbosity of
    // the tracing library in logging_base.
    let _logging_builder = logging_base::Builder::new()
        .prometheus_listen_addr(&config.prometheus_listen_addr)
        .init();

    info!(
        config = config.as_value(),
        "Starting scylla failover manager."
    );

    // Clients
    let pubsub_client = pubsub_client().await?;
    let compute_client_instance = compute_client().await?;

    let subscription_name = format!(
        "scylla-failover-manager-{}-{}",
        config.environment,
        rand::thread_rng().gen::<u32>()
    );

    // Cleanup handler, this is a simple struct that will handle the cleanup of the subscription
    // so the service does not leave any resources behind.
    let cleanup_handler =
        CleanupHandler::new(pubsub_client.clone(), subscription_name.clone()).await?;

    match pubsub_client
        .create_subscription(&subscription_name, &config.topic)
        .await
    {
        Ok(_) => {
            info!(subscription = subscription_name, "Subscription created");
        }
        Err(status) => {
            error!(
                subscription = subscription_name,
                status = status.to_string(),
                "Failed to create subscription."
            );
            Err(status.message())?;
        }
    };

    let failover_manager = FailoverManager::new(
        compute_client_instance,
        pubsub_client,
        config.dry_run,
        config.environment.clone(),
        "not_used".to_string(),
    );

    let main_task = tokio::spawn({
        let token = cleanup_handler.token();
        async move {
            // Main loop
            // we simply listen to the subscription for host error messages and process them indefinitely.
            // If we receive a cancellation signal we will break out of the loop and shutdown.
            loop {
                tokio::select! {
                    _ = token.cancelled() => {
                        warn!("Received cancellation signal, shutting down");
                        break;
                    }

                    result = failover_manager.fetch_host_error(&subscription_name) => {
                        let host_error = match result {
                            Ok(host_error) => host_error,
                            Err(error) => {
                                match error {
                                    scylla_failover_manager::failover_manager::FailOverManagerError::NoMessageReceived => continue,
                                    _ => {
                                        error!(exception = ?error, "Failed to fetch host error.");
                                        continue
                                    },
                                };
                            }
                        };
                        info!(instance_name = host_error.instance_name.clone(), zone = host_error.zone.clone(), "Processing host error.");
                        match failover_manager.process_host_error(host_error).await {
                            Ok(_) => {
                                info!("Successfully processed host error");
                            },
                            Err(e) => {
                                error!(error = ?e, "Failed to process host error");
                            }
                        }
                    }
                };
            }
        }
    });

    {
        cleanup_handler
            .main_task_handle
            .lock()
            .unwrap()
            .replace(main_task);
    }

    cleanup_handler.join().await?;

    Ok(())
}

// Returns a pubsub client with the default token source provider.
// or an error if the client could not be created.
async fn pubsub_client() -> Result<PubSubClient, Box<dyn std::error::Error>> {
    let pubsub_default_token_source_provider = match PubSubClient::default_token_source_provider()
        .await
    {
        Ok(token_source_provider) => token_source_provider,
        Err(e) => {
            error!(exception = ?e, "Auth failed: failed to create pubsub token source provider.");
            return Err(e.into());
        }
    };

    let pubsub_client = match PubSubClient::new(pubsub_default_token_source_provider).await {
        Ok(client) => client,
        Err(e) => {
            error!(
                exception = e,
                "Auth failed: Failed to create pubsub client."
            );
            return Err(e);
        }
    };

    Ok(pubsub_client)
}

// Returns a compute_client with the default token source provider.
// or an error if the client could not be created.
async fn compute_client(
) -> Result<ComputeClient<DefaultTokenSourceProvider>, Box<dyn std::error::Error>> {
    let compute_token_provider =
        match ComputeClient::<DefaultTokenSourceProvider>::default_token_source_provider().await {
            Ok(token_provider) => token_provider,
            Err(e) => {
                error!(exception = ?e, "Failed to create compute token source provider.");
                return Err(e.into());
            }
        };
    let compute_client = ComputeClient::new(compute_token_provider);

    Ok(compute_client)
}

struct CleanupHandler {
    pubsub_client: PubSubClient,
    subscription_name: String,
    pub main_task_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    token: CancellationToken,
    join_handles: Vec<JoinHandle<()>>,
}

impl CleanupHandler {
    pub async fn join(self) -> Result<(), Box<dyn std::error::Error>> {
        for handle in self.join_handles.into_iter() {
            handle.await?;
        }
        Ok(())
    }

    pub async fn new(
        pubsub_client: PubSubClient,
        subscription_name: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Create a CancellationToken
        let token = CancellationToken::new();

        let mut cleanup_handler = Self {
            pubsub_client,
            subscription_name,
            token,
            main_task_handle: Arc::new(Mutex::new(None)),
            join_handles: vec![],
        };

        // Signal handlers for cleanup
        cleanup_handler
            .setup_signal_handler(SignalKind::interrupt())
            .await?;
        cleanup_handler
            .setup_signal_handler(SignalKind::terminate())
            .await?;

        Ok(cleanup_handler)
    }

    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    async fn setup_signal_handler(
        &mut self,
        signal_kind: SignalKind,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut signal = match signal(signal_kind) {
            Ok(signal) => signal,
            Err(e) => {
                return Err(Box::new(e));
            }
        };

        let task = tokio::task::spawn({
            let pubsub_client = self.pubsub_client.clone();
            let subscription_name = self.subscription_name.to_string();
            let token = self.token.clone();
            let main_task_handle = self.main_task_handle.clone();
            async move {
                loop {
                    if main_task_handle.lock().unwrap().is_none() {
                        warn!("Waiting for subscribtion to be created.");
                        _ = tokio::time::sleep(Duration::from_secs(2)).await;
                        continue;
                    }

                    // Delete subscription
                    tokio::select! {
                        _ = token.cancelled() => {
                            break
                        }
                        result = signal.recv() => {
                            match result {
                                Some(()) => {
                                    warn!("Received signal {:?}, shutting down", signal_kind);

                                    // Cancel main loop
                                    token.cancel();

                                    warn!("Deleting subscription: {}", subscription_name);
                                    match pubsub_client.delete_subscription(&subscription_name).await {
                                        Ok(()) => {
                                            info!("Subscription deleted: {}", subscription_name);
                                        },
                                        Err(e) => {
                                            error!("Failed to delete subscription: {} (possibly not created)", e);
                                        }
                                    };
                                },
                                None => error!("Stream terminated before receiving signal"),
                            }
                        }
                    }
                }
            }
        });

        self.join_handles.push(task);

        Ok(())
    }
}
