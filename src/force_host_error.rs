use clap::Parser;
use google_cloud_gax::conn::Environment;

use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::client::{Client, ClientConfig};
use scylla_failover_manager::google::pubsub::PubSubClient;
use serde_json::json;
use tracing::info;

use std::env::var;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the topic to send messages to
    #[arg(short, long, default_value = "persistence-scylla-host-error")]
    topic: String,

    /// Dry run
    #[arg(short, long)]
    dry_run: bool,

    /// Host to mark as failed
    host: String,

    /// Zone of the host
    zone: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The RUST_TRACING environment variable is used to control the verbosity of
    // the tracing library in logging_base.
    let _logging_builder = logging_base::Builder::new().init();

    let args = Args::parse();

    let pubsub_config = ClientConfig {
        project_id: Some(scylla_failover_manager::consts::PUBSUB_PROJECT_ID.to_string()),
        environment: match var("PUBSUB_EMULATOR_HOST").ok() {
            Some(v) => Environment::Emulator(v),
            None => {
                let token_provider = match PubSubClient::default_token_source_provider().await {
                    Ok(token_source_provider) => token_source_provider,
                    Err(e) => {
                        return Err(format!("Auth failed {}", e).into());
                    }
                };

                Environment::GoogleCloud(Box::new(token_provider))
            }
        },
        ..Default::default()
    };

    let pubsub_client = Client::new(pubsub_config).await?;

    let topic = pubsub_client.topic(&args.topic);
    if !topic.exists(None).await? {
        topic.create(None, None).await?;
    }

    // Start publisher.
    let mut publisher = topic.new_publisher(None);

    let msg = json!(
        {
            "protoPayload": {
                "@type": "type.googleapis.com/google.cloud.audit.AuditLog",
                "status": {
                    "message": "Instance terminated by Compute Engine."
                },
                "authenticationInfo": {
                    "principalEmail": "system@google.com"
                },
                "serviceName": "compute.googleapis.com",
                "methodName": "compute.instances.hostError",
                "resourceName": format!("projects/{}/zones/{}/instances/{}", scylla_failover_manager::consts::PUBSUB_PROJECT_ID.to_string(), args.zone, args.host),
                "request": {
                    "@type": "type.googleapis.com/compute.instances.hostError"
                }
            },
            "insertId": "-6mrd68di0ag",
            "resource": {
                "type": "gce_instance",
                "labels": {
                    "instance_id": "4162442934972324426",
                    "project_id": "args.project",
                    "zone": args.zone,
                }
            },
            "timestamp": "2024-09-05T21:52:57.411466Z",
            "severity": "INFO",
            "labels": {
                "compute.googleapis.com/root_trigger_id": "08f90cb5-f728-4655-8b3e-521a02db78d3"
            },
            "logName": "projects/args.project/logs/cloudaudit.googleapis.com%2Fsystem_event",
            "operation": {
                "id": "systemevent-1725573174254-621665015daf3-be49e4f2-3a389b3f",
                "producer": "compute.instances.hostError",
                "first": true,
                "last": true
            },
            "receiveTimestamp": "2024-09-05T21:52:57.896194566Z"
        }
    );

    let pubsub_message = PubsubMessage {
        data: msg.to_string().into_bytes(),
        // TODO: determine why I can't publish messages with ordering_key
        // ordering_key: "order".into(),
        ..Default::default()
    };

    if args.dry_run {
        info!(message = msg.to_string(), "Would have sent message");
        return Ok(());
    }
    info!(message = msg.to_string(), "Sending message");
    publisher
        .publish_immediately(vec![pubsub_message], None)
        .await?;

    // The get method blocks until a server-generated ID or an error is returned for the published message.
    publisher.shutdown().await;

    Ok(())
}
