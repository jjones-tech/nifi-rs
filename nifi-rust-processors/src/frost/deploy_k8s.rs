//! DeployToK8s Processor
//!
//! Deploys built images to Kubernetes (updates Deployments, StatefulSets, etc.)

use super::attributes;
use k8s_openapi::api::apps::v1::Deployment;
use kube::{
    api::{Api, Patch, PatchParams},
    Client,
};
use nifi_rust_api::prelude::*;
use serde_json::json;
use tracing::{error, info, warn};

/// Processor that deploys images to K8s
pub struct DeployToK8s {
    /// K8s client
    k8s_client: Option<Client>,
    /// Default namespace for deployments
    namespace: String,
    /// Deployment strategy
    strategy: DeployStrategy,
}

#[derive(Debug, Clone, Default)]
enum DeployStrategy {
    #[default]
    UpdateImage,
    ApplyManifest,
}

impl Default for DeployToK8s {
    fn default() -> Self {
        Self {
            k8s_client: None,
            namespace: "default".to_string(),
            strategy: DeployStrategy::UpdateImage,
        }
    }
}

#[async_trait]
impl Processor for DeployToK8s {
    fn get_type(&self) -> &'static str {
        "org.apache.nifi.frost.DeployToK8s"
    }

    fn get_description(&self) -> &'static str {
        "Deploys built container images to Kubernetes by updating Deployments"
    }

    fn get_property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![
            PropertyDescriptor::new("k8s.namespace")
                .display_name("Kubernetes Namespace")
                .description("Default namespace for deployments")
                .required(true)
                .default_value(PropertyValue::String("default".to_string()))
                .build(),
            PropertyDescriptor::new("deploy.strategy")
                .display_name("Deploy Strategy")
                .description("How to deploy: update-image (patch image tag) or apply-manifest (full manifest)")
                .required(false)
                .default_value(PropertyValue::String("update-image".to_string()))
                .allowable_values(vec!["update-image", "apply-manifest"])
                .build(),
        ]
    }

    fn get_relationships(&self) -> Vec<Relationship> {
        vec![Relationship::SUCCESS, Relationship::FAILURE]
    }

    async fn on_scheduled(&mut self, context: &ProcessContext) -> Result<(), ProcessorError> {
        // Initialize K8s client
        self.k8s_client = Some(
            Client::try_default()
                .await
                .map_err(|e| ProcessorError::Other(format!("Failed to create K8s client: {}", e)))?,
        );

        // Get configuration
        if let Some(ns) = context.get_property_string("k8s.namespace") {
            self.namespace = ns.to_string();
        }

        if let Some(strategy) = context.get_property_string("deploy.strategy") {
            self.strategy = match strategy {
                "apply-manifest" => DeployStrategy::ApplyManifest,
                _ => DeployStrategy::UpdateImage,
            };
        }

        info!(
            namespace = %self.namespace,
            strategy = ?self.strategy,
            "DeployToK8s scheduled"
        );

        Ok(())
    }

    async fn on_trigger(
        &mut self,
        _context: &ProcessContext,
        session: &mut ProcessSession,
    ) -> Result<(), ProcessorError> {
        let client = self.k8s_client.as_ref().ok_or_else(|| {
            ProcessorError::NotInitialized("K8s client not initialized".to_string())
        })?;

        while let Some(mut flowfile) = session.get() {
            match &self.strategy {
                DeployStrategy::UpdateImage => {
                    self.deploy_update_image(client, session, &mut flowfile).await?;
                }
                DeployStrategy::ApplyManifest => {
                    self.deploy_apply_manifest(client, session, &mut flowfile).await?;
                }
            }
        }

        Ok(())
    }

    async fn on_stopped(&mut self, _context: &ProcessContext) -> Result<(), ProcessorError> {
        self.k8s_client = None;
        info!("DeployToK8s stopped");
        Ok(())
    }
}

impl DeployToK8s {
    async fn deploy_update_image(
        &self,
        client: &Client,
        session: &mut ProcessSession,
        flowfile: &mut FlowFile,
    ) -> Result<(), ProcessorError> {
        // Get image from attribute
        let image = flowfile
            .get_attribute(attributes::FROST_BUILD_IMAGE)
            .ok_or_else(|| {
                ProcessorError::InvalidConfig("Missing frost.build.image attribute".to_string())
            })?
            .to_string();

        // Parse deploy config from content
        let deploy_config: DeployConfig = if let Some(content) = flowfile.content() {
            serde_json::from_slice(content).map_err(|e| {
                ProcessorError::Serialization(format!("Failed to parse deploy config: {}", e))
            })?
        } else {
            return Err(ProcessorError::InvalidConfig(
                "Missing deployment configuration in FlowFile content".to_string(),
            ));
        };

        let namespace = deploy_config
            .namespace
            .as_deref()
            .unwrap_or(&self.namespace);

        let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);

        // Patch the deployment to update the image
        let patch = json!({
            "spec": {
                "template": {
                    "spec": {
                        "containers": [{
                            "name": deploy_config.container_name.as_deref().unwrap_or("main"),
                            "image": image
                        }]
                    }
                }
            }
        });

        match deployments
            .patch(
                &deploy_config.deployment_name,
                &PatchParams::apply("frost-nifi"),
                &Patch::Strategic(patch),
            )
            .await
        {
            Ok(patched) => {
                let name = patched.metadata.name.unwrap_or_default();
                info!(
                    deployment = %name,
                    image = %image,
                    namespace = %namespace,
                    "Deployment updated"
                );

                flowfile.put_attribute(attributes::FROST_DEPLOY_STATUS, "success");
                session.transfer(flowfile.clone(), Relationship::SUCCESS);
            }
            Err(e) => {
                error!(
                    error = %e,
                    deployment = %deploy_config.deployment_name,
                    "Failed to update deployment"
                );

                flowfile.put_attribute(attributes::FROST_DEPLOY_STATUS, "failed");
                session.transfer(flowfile.clone(), Relationship::FAILURE);
            }
        }

        Ok(())
    }

    async fn deploy_apply_manifest(
        &self,
        client: &Client,
        session: &mut ProcessSession,
        flowfile: &mut FlowFile,
    ) -> Result<(), ProcessorError> {
        // Get manifest from FlowFile content
        let content = flowfile.content().ok_or_else(|| {
            ProcessorError::InvalidConfig("Missing manifest in FlowFile content".to_string())
        })?;

        // Try to parse as a Deployment
        let deployment: Deployment = serde_json::from_slice(content).map_err(|e| {
            ProcessorError::Serialization(format!("Failed to parse deployment manifest: {}", e))
        })?;

        let namespace = deployment
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| self.namespace.clone());

        let name = deployment
            .metadata
            .name
            .clone()
            .ok_or_else(|| ProcessorError::InvalidConfig("Deployment missing name".to_string()))?;

        let deployments: Api<Deployment> = Api::namespaced(client.clone(), &namespace);

        // Apply the deployment
        match deployments
            .patch(&name, &PatchParams::apply("frost-nifi"), &Patch::Apply(deployment))
            .await
        {
            Ok(applied) => {
                let name = applied.metadata.name.unwrap_or_default();
                info!(
                    deployment = %name,
                    namespace = %namespace,
                    "Deployment applied"
                );

                flowfile.put_attribute(attributes::FROST_DEPLOY_STATUS, "success");
                session.transfer(flowfile.clone(), Relationship::SUCCESS);
            }
            Err(e) => {
                error!(error = %e, "Failed to apply deployment");
                flowfile.put_attribute(attributes::FROST_DEPLOY_STATUS, "failed");
                session.transfer(flowfile.clone(), Relationship::FAILURE);
            }
        }

        Ok(())
    }
}

/// Deployment configuration for update-image strategy
#[derive(Debug, Clone, serde::Deserialize)]
struct DeployConfig {
    /// Name of the deployment to update
    deployment_name: String,
    /// Namespace (optional, uses processor default)
    namespace: Option<String>,
    /// Container name to update (optional, defaults to "main")
    container_name: Option<String>,
}

// Register the processor
nifi_rust_api::register_processor!(DeployToK8s);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_processor_metadata() {
        let processor = DeployToK8s::default();
        assert_eq!(processor.get_type(), "org.apache.nifi.frost.DeployToK8s");
        assert!(!processor.get_property_descriptors().is_empty());
        assert_eq!(processor.get_relationships().len(), 2);
    }
}
