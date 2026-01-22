//! FROST NiFi Processors
//!
//! This crate provides NiFi processors for FROST CI/CD workflows:
//!
//! - `ConsumeFrostBus` - Consume jobs from frost-bus (Valkey/Redis)
//! - `PublishFrostBus` - Publish jobs to frost-bus
//! - `TriggerKanikoBuild` - Trigger kaniko builds in K8s
//! - `DeployToK8s` - Deploy built images to K8s

pub mod frost;

// Re-export processors for convenience
pub use frost::consume_frost_bus::ConsumeFrostBus;
pub use frost::deploy_k8s::DeployToK8s;
pub use frost::publish_frost_bus::PublishFrostBus;
pub use frost::trigger_kaniko::TriggerKanikoBuild;
