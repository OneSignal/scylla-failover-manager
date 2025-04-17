use google_cloud_auth::token::DefaultTokenSourceProvider;
use google_cloud_token::TokenSourceProvider;
use logging_base::{error, info, warn};
use std::collections::HashMap;

#[derive(Debug)]
pub struct ComputeClient<T: TokenSourceProvider> {
    token_provider: T,
    reqwest_client: reqwest::Client,
    base_url: String,
    project_id: String,
}

/// An GCP Instance, only the fields we care about
#[derive(Debug, serde::Deserialize)]
pub struct Instance {
    pub id: String,
    pub name: String,
    pub labels: HashMap<String, String>,
    pub metadata: Option<Metadata>,

    #[serde(rename = "networkInterfaces")]
    pub network_interfaces: Vec<NetworkInterface>,
}

#[derive(Debug, serde::Deserialize)]
pub struct Metadata {
    pub items: Option<Vec<MetadataItem>>,
}

#[derive(Debug, serde::Deserialize)]
pub struct MetadataItem {
    pub key: String,
    pub value: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
pub struct NetworkInterface {
    #[serde(rename = "networkIP")]
    pub network_ip: String,
}

impl ComputeClient<DefaultTokenSourceProvider> {
    pub async fn default_token_source_provider(
    ) -> Result<DefaultTokenSourceProvider, google_cloud_auth::error::Error> {
        let scopes: [&str; 3] = [
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/ndev.clouddns.readwrite",
            "https://www.googleapis.com/auth/pubsub",
        ];
        let auth_config = google_cloud_auth::project::Config::default()
            .with_audience("https://compute.googleapis.com/")
            .with_scopes(&scopes);

        DefaultTokenSourceProvider::new(auth_config).await
    }
}

impl<T: TokenSourceProvider> ComputeClient<T> {
    pub fn new(token_provider: T) -> Self {
        Self {
            token_provider,
            project_id: crate::consts::COMPUTE_PROJECT_ID.to_string(),
            base_url: "https://compute.googleapis.com".to_string(),
            reqwest_client: reqwest::Client::new(),
        }
    }

    /// Will return an instance if it exists in one of the zones
    /// provided, otherwise None.
    pub async fn get_instance(
        &self,
        instance_name: &str,
        zones: &[&str],
    ) -> Result<Option<Instance>, Box<dyn std::error::Error + Send + Sync>> {
        let mut instance: Option<Instance> = None;
        for zone in zones {
            let url = format!(
                "{}/compute/v1/projects/{}/zones/{}/instances/{}",
                self.base_url, self.project_id, zone, instance_name
            );

            info!("Fetching instance from url: {}", url);

            let response = self
                .reqwest_client
                .get(&url)
                .header(
                    "Authorization",
                    &self.token_provider.token_source().token().await?,
                )
                .send()
                .await?;
            let status = response.status();

            let body = response.text().await?;

            if status == reqwest::StatusCode::NOT_FOUND {
                continue;
            }

            info!("{}", body);

            match serde_json::from_str(&body) {
                Ok(i) => {
                    instance = Some(i);
                    break;
                }
                Err(e) => {
                    error!(
                        "Error parsing response: {}. Status: {}. Response: {}",
                        e, status, body
                    );
                    return Err(Box::new(e));
                }
            };
        }

        Ok(instance)
    }

    pub async fn delete_instance(
        &self,
        instance_name: &str,
        zones: &[&str],
    ) -> Result<Option<Instance>, Box<dyn std::error::Error + Send + Sync>> {
        let mut instance: Option<Instance> = None;
        for zone in zones {
            let url = format!(
                "{}/compute/v1/projects/{}/zones/{}/instances/{}",
                self.base_url, self.project_id, zone, instance_name
            );

            info!("Deleting instance at url: {}", url);

            let response = self
                .reqwest_client
                .get(&url)
                .header(
                    "Authorization",
                    &self.token_provider.token_source().token().await?,
                )
                .send()
                .await?;
            let status = response.status();

            let body = response.text().await?;

            if status == reqwest::StatusCode::NOT_FOUND {
                continue;
            }

            info!("{}", body);

            match serde_json::from_str(&body) {
                Ok(i) => {
                    instance = Some(i);
                    break;
                }
                Err(e) => {
                    error!(
                        "Error parsing response: {}. Status: {}. Response: {}",
                        e, status, body
                    );
                    return Err(Box::new(e));
                }
            };
        }

        Ok(instance)
    }

    /// Fetch the instances in a zone given their project id and a query filter
    /// pagination is not implemented as maxResults is 500 and we don't expect having 1000
    /// postgres servers.
    pub async fn list_instances(
        &self,
        zones: &[&str],
        filter: &[&str],
    ) -> Result<Vec<Instance>, Box<dyn std::error::Error + Send + Sync>> {
        #[derive(Debug, serde::Deserialize)]
        struct InstancesResponse {
            items: Option<Vec<Instance>>,
        }

        let mut items: Vec<Instance> = Vec::new();

        for zone in zones {
            let url = if filter.is_empty() {
                format!(
                    "{}/compute/v1/projects/{}/zones/{}/instances",
                    self.base_url, self.project_id, zone
                )
            } else {
                // we put parenthesis around the filters and add "and" between them
                let filter = filter
                    .iter()
                    .map(|f| format!("({})", f))
                    .collect::<Vec<String>>()
                    .join(" AND ");
                format!(
                    "{}/compute/v1/projects/{}/zones/{}/instances?filter={}",
                    self.base_url, self.project_id, zone, filter
                )
            };

            let response = self
                .reqwest_client
                .get(&url)
                .header(
                    "Authorization",
                    &self.token_provider.token_source().token().await?,
                )
                .send()
                .await?;

            let status = response.status();
            let body = response.text().await?;

            let instances: InstancesResponse = match serde_json::from_str(&body) {
                Ok(instances) => instances,
                Err(e) => {
                    error!(
                        "Error parsing response: {}. Status: {}. Response: {}",
                        e, status, body
                    );
                    return Err(Box::new(e));
                }
            };

            if let Some(instances) = instances.items {
                items.extend(instances);
            }
        }

        Ok(items)
    }

    // This is for testing so we can use a mock server
    #[allow(dead_code)]
    pub fn set_base_url(&mut self, base_url: &str) {
        self.base_url = base_url.to_string();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consts::*;
    use crate::test_utils::*;

    #[tokio::test]
    async fn test_get_instance() {
        let instance = "gke-test-cluster-onesig-scylla-old-v1-967bcf8d-0rim";
        let zone = "europe-west4-b";
        let mut compute = ComputeClient::new(
            ComputeClient::default_token_source_provider()
                .await
                .unwrap(),
        );

        let instance = compute.get_instance(instance, &[zone]).await.unwrap();

        assert_eq!(instance.unwrap().id, "3513668990839866896");
    }

    #[tokio::test]
    async fn test_get_instance_group() {
        let instance = "gke-test-cluster-onesig-scylla-old-v1-967bcf8d-0rim";
        let zone = "europe-west4-b";
        let mut compute = ComputeClient::new(
            ComputeClient::default_token_source_provider()
                .await
                .unwrap(),
        );

        let instance = compute.get_instance(instance, &[zone]).await.unwrap();

        if let Some(instance) = instance {
            if let Some(metadata_items) = instance.metadata {
                for item in metadata_items.items.unwrap_or_default() {
                    println!("Key: {}, Value: {:?}", item.key, item.value,);
                }
            } else {
                println!("No metadata items found for the instance.");
            }
        } else {
            println!("Instance not found.");
        }
    }
}
