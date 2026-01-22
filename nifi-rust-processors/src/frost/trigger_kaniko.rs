//! TriggerKanikoBuild Processor
//!
//! Triggers kaniko builds in Kubernetes based on FlowFile content.

use super::attributes;
use k8s_openapi::api::batch::v1::{Job as K8sJob, JobSpec};
use k8s_openapi::api::core::v1::{Container, EnvVar, PodSpec, PodTemplateSpec, Volume, VolumeMount, SecretVolumeSource};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::{api::Api, api::PostParams, Client};
use nifi_rust_api::prelude::*;
use std::collections::BTreeMap;
use tracing::{error, info};

/// Processor that triggers kaniko builds in K8s
pub struct TriggerKanikoBuild {
    /// K8s client
    k8s_client: Option<Client>,
    /// Namespace for kaniko jobs
    namespace: String,
    /// Docker registry URL
    registry: String,
    /// Docker config secret name
    docker_secret: String,
    /// Git credentials secret name
    git_secret: String,
}

impl Default for TriggerKanikoBuild {
    fn default() -> Self {
        Self {
            k8s_client: None,
            namespace: "frost-works".to_string(),
            registry: "registry.digitalocean.com/frost-os".to_string(),
            docker_secret: "docker-registry-creds".to_string(),
            git_secret: "git-creds".to_string(),
        }
    }
}

#[async_trait]
impl Processor for TriggerKanikoBuild {
    fn get_type(&self) -> &'static str {
        "org.apache.nifi.frost.TriggerKanikoBuild"
    }

    fn get_description(&self) -> &'static str {
        "Triggers kaniko container image builds in Kubernetes"
    }

    fn get_property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![
            PropertyDescriptor::new("k8s.namespace")
                .display_name("Kubernetes Namespace")
                .description("Namespace where kaniko jobs will be created")
                .required(true)
                .default_value(PropertyValue::String("frost-works".to_string()))
                .build(),
            PropertyDescriptor::new("docker.registry")
                .display_name("Docker Registry")
                .description("Docker registry URL for built images")
                .required(true)
                .build(),
            PropertyDescriptor::new("docker.secret")
                .display_name("Docker Secret Name")
                .description("K8s secret name containing docker registry credentials")
                .required(true)
                .default_value(PropertyValue::String("docker-registry-creds".to_string()))
                .build(),
            PropertyDescriptor::new("git.secret")
                .display_name("Git Secret Name")
                .description("K8s secret name containing git credentials")
                .required(false)
                .default_value(PropertyValue::String("git-creds".to_string()))
                .build(),
        ]
    }

    fn get_relationships(&self) -> Vec<Relationship> {
        vec![
            Relationship::SUCCESS,
            Relationship::FAILURE,
            Relationship::new("no_config", "FlowFiles that don't have build configuration"),
        ]
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

        if let Some(reg) = context.get_property_string("docker.registry") {
            self.registry = reg.to_string();
        }

        if let Some(secret) = context.get_property_string("docker.secret") {
            self.docker_secret = secret.to_string();
        }

        if let Some(secret) = context.get_property_string("git.secret") {
            self.git_secret = secret.to_string();
        }

        info!(
            namespace = %self.namespace,
            registry = %self.registry,
            "TriggerKanikoBuild scheduled"
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

        let jobs_api: Api<K8sJob> = Api::namespaced(client.clone(), &self.namespace);

        while let Some(mut flowfile) = session.get() {
            // Parse build configuration from FlowFile content
            let build_config: BuildConfig = if let Some(content) = flowfile.content() {
                match serde_json::from_slice(content) {
                    Ok(config) => config,
                    Err(e) => {
                        error!(error = %e, "Failed to parse build config");
                        session.transfer(
                            flowfile,
                            Relationship::new("no_config", "Missing build configuration"),
                        );
                        continue;
                    }
                }
            } else {
                session.transfer(
                    flowfile,
                    Relationship::new("no_config", "Missing build configuration"),
                );
                continue;
            };

            // Generate unique job name
            let job_name = format!(
                "kaniko-{}-{}",
                build_config
                    .image_name
                    .replace(['/', ':', '.'], "-")
                    .chars()
                    .take(40)
                    .collect::<String>(),
                uuid::Uuid::new_v4().to_string().chars().take(8).collect::<String>()
            );

            // Build the kaniko job
            let image_tag = format!(
                "{}/{}:{}",
                self.registry,
                build_config.image_name,
                build_config.git_sha.chars().take(8).collect::<String>()
            );

            let k8s_job = self.create_kaniko_job(
                &job_name,
                &build_config,
                &image_tag,
                flowfile.get_attribute(attributes::FROST_WORKSPACE_ID),
            );

            // Create the job in K8s
            match jobs_api.create(&PostParams::default(), &k8s_job).await {
                Ok(created) => {
                    let name = created.metadata.name.unwrap_or_else(|| job_name.clone());
                    info!(job = %name, image = %image_tag, "Kaniko job created");

                    flowfile.put_attribute(attributes::FROST_BUILD_JOB, &name);
                    flowfile.put_attribute(attributes::FROST_BUILD_IMAGE, &image_tag);
                    session.transfer(flowfile, Relationship::SUCCESS);
                }
                Err(e) => {
                    error!(error = %e, job = %job_name, "Failed to create kaniko job");
                    session.transfer(flowfile, Relationship::FAILURE);
                }
            }
        }

        Ok(())
    }

    async fn on_stopped(&mut self, _context: &ProcessContext) -> Result<(), ProcessorError> {
        self.k8s_client = None;
        info!("TriggerKanikoBuild stopped");
        Ok(())
    }
}

impl TriggerKanikoBuild {
    fn create_kaniko_job(
        &self,
        job_name: &str,
        config: &BuildConfig,
        image_tag: &str,
        workspace_id: Option<&str>,
    ) -> K8sJob {
        let mut labels = BTreeMap::new();
        labels.insert("frost-build/type".to_string(), "kaniko".to_string());
        labels.insert("frost-build/image".to_string(), config.image_name.clone());
        labels.insert("frost-build/config".to_string(), config.name.clone());
        labels.insert("frost-build/git-ref".to_string(), config.git_ref.clone());
        labels.insert(
            "frost-build/git-sha".to_string(),
            config.git_sha.chars().take(40).collect(),
        );

        if let Some(ws_id) = workspace_id {
            labels.insert("frost-build/workspace".to_string(), ws_id.to_string());
        }

        K8sJob {
            metadata: ObjectMeta {
                name: Some(job_name.to_string()),
                namespace: Some(self.namespace.clone()),
                labels: Some(labels),
                ..Default::default()
            },
            spec: Some(JobSpec {
                ttl_seconds_after_finished: Some(3600),
                backoff_limit: Some(0),
                template: PodTemplateSpec {
                    spec: Some(PodSpec {
                        restart_policy: Some("Never".to_string()),
                        containers: vec![Container {
                            name: "kaniko".to_string(),
                            image: Some(
                                "gcr.io/kaniko-project/executor:latest".to_string(),
                            ),
                            args: Some(vec![
                                format!("--context={}", config.git_repo),
                                format!("--dockerfile={}", config.dockerfile.as_deref().unwrap_or("Dockerfile")),
                                format!("--destination={}", image_tag),
                                "--cache=true".to_string(),
                            ]),
                            env: Some(vec![
                                EnvVar {
                                    name: "GIT_TOKEN".to_string(),
                                    value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                        secret_key_ref: Some(k8s_openapi::api::core::v1::SecretKeySelector {
                                            name: self.git_secret.clone(),
                                            key: "token".to_string(),
                                            optional: Some(true),
                                        }),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                            ]),
                            volume_mounts: Some(vec![VolumeMount {
                                name: "docker-config".to_string(),
                                mount_path: "/kaniko/.docker".to_string(),
                                ..Default::default()
                            }]),
                            ..Default::default()
                        }],
                        volumes: Some(vec![Volume {
                            name: "docker-config".to_string(),
                            secret: Some(SecretVolumeSource {
                                secret_name: Some(self.docker_secret.clone()),
                                items: Some(vec![k8s_openapi::api::core::v1::KeyToPath {
                                    key: ".dockerconfigjson".to_string(),
                                    path: "config.json".to_string(),
                                    ..Default::default()
                                }]),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }]),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}

/// Build configuration extracted from FlowFile content
#[derive(Debug, Clone, serde::Deserialize)]
struct BuildConfig {
    /// Build config name
    name: String,
    /// Git repository URL
    git_repo: String,
    /// Git ref (branch/tag)
    git_ref: String,
    /// Git SHA
    git_sha: String,
    /// Image name (without registry prefix)
    image_name: String,
    /// Dockerfile path (optional, defaults to "Dockerfile")
    dockerfile: Option<String>,
}

// Register the processor
nifi_rust_api::register_processor!(TriggerKanikoBuild);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_processor_metadata() {
        let processor = TriggerKanikoBuild::default();
        assert_eq!(
            processor.get_type(),
            "org.apache.nifi.frost.TriggerKanikoBuild"
        );
        assert!(!processor.get_property_descriptors().is_empty());
        assert_eq!(processor.get_relationships().len(), 3);
    }
}
