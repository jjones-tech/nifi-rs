//! SelfDeployGate Processor
//!
//! Safety gate for self-deployments (when NiFi deploys updates to itself).
//! Checks frost-bus queues are empty before allowing deployment to proceed.

use nifi_rust_api::prelude::*;
use redis::AsyncCommands;
use tracing::{info, warn};

/// Processor that gates self-deployments by checking frost-bus queue status
pub struct SelfDeployGate {
    /// Redis client for frost-bus queue checks
    redis_client: Option<redis::Client>,
    /// Deployment name to gate (e.g., "nifi-rs")
    gate_deployment: String,
    /// Quiesce period in seconds before allowing deploy
    quiesce_seconds: u64,
    /// Last check timestamp
    last_empty_check: Option<std::time::Instant>,
}

impl Default for SelfDeployGate {
    fn default() -> Self {
        Self {
            redis_client: None,
            gate_deployment: "nifi-rs".to_string(),
            quiesce_seconds: 30,
            last_empty_check: None,
        }
    }
}

#[async_trait]
impl Processor for SelfDeployGate {
    fn get_type(&self) -> &'static str {
        "org.apache.nifi.frost.SelfDeployGate"
    }

    fn get_description(&self) -> &'static str {
        "Safety gate for self-deployments - ensures frost-bus queues are empty before deploying updates to NiFi itself"
    }

    fn get_property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![
            PropertyDescriptor::new("frost.bus.url")
                .display_name("frost-bus URL")
                .description("Redis/Valkey URL for frost-bus queue checks")
                .required(true)
                .sensitive(true)
                .build(),
            PropertyDescriptor::new("gate.deployment")
                .display_name("Gate Deployment")
                .description("Deployment name to apply safety gate (e.g., 'nifi-rs')")
                .required(true)
                .default_value(PropertyValue::String("nifi-rs".to_string()))
                .build(),
            PropertyDescriptor::new("quiesce.seconds")
                .display_name("Quiesce Period")
                .description("Seconds to wait with empty queues before allowing deploy")
                .required(false)
                .default_value(PropertyValue::String("30".to_string()))
                .build(),
        ]
    }

    fn get_relationships(&self) -> Vec<Relationship> {
        vec![
            Relationship::SUCCESS,
            Relationship::new("wait")
                .description("FlowFiles sent here when queues are not empty - should be retried later"),
            Relationship::FAILURE,
        ]
    }

    async fn on_scheduled(&mut self, context: &ProcessContext) -> Result<(), ProcessorError> {
        // Get frost-bus URL
        let bus_url = context
            .get_property_string("frost.bus.url")
            .ok_or_else(|| ProcessorError::InvalidConfig("frost.bus.url is required".to_string()))?;

        // Initialize Redis client
        self.redis_client = Some(redis::Client::open(bus_url.as_ref()).map_err(|e| {
            ProcessorError::Other(format!("Failed to create Redis client: {}", e))
        })?);

        // Get gate deployment
        if let Some(deployment) = context.get_property_string("gate.deployment") {
            self.gate_deployment = deployment.to_string();
        }

        // Get quiesce period
        if let Some(seconds) = context.get_property_string("quiesce.seconds") {
            self.quiesce_seconds = seconds.parse().unwrap_or(30);
        }

        info!(
            gate_deployment = %self.gate_deployment,
            quiesce_seconds = self.quiesce_seconds,
            "SelfDeployGate scheduled"
        );

        Ok(())
    }

    async fn on_trigger(
        &mut self,
        _context: &ProcessContext,
        session: &mut ProcessSession,
    ) -> Result<(), ProcessorError> {
        let client = self.redis_client.as_ref().ok_or_else(|| {
            ProcessorError::NotInitialized("Redis client not initialized".to_string())
        })?;

        while let Some(mut flowfile) = session.get() {
            // Check if this is a self-deploy (deployment target matches gate)
            let deploy_target = flowfile
                .get_attribute("frost.deploy.target")
                .map(|s| s.to_string());

            let is_self_deploy = deploy_target
                .as_ref()
                .map(|t| t == &self.gate_deployment)
                .unwrap_or(false);

            if !is_self_deploy {
                // Not a self-deploy, pass through
                session.transfer(flowfile, Relationship::SUCCESS);
                continue;
            }

            // Check if queues are empty
            match self.check_queues_empty(client).await {
                Ok(true) => {
                    // Queues are empty - check quiesce period
                    let now = std::time::Instant::now();

                    match self.last_empty_check {
                        Some(last) if now.duration_since(last).as_secs() >= self.quiesce_seconds => {
                            // Quiesce period passed, allow deploy
                            info!(
                                deployment = %self.gate_deployment,
                                "Self-deploy gate passed after quiesce period"
                            );
                            self.last_empty_check = None;
                            session.transfer(flowfile, Relationship::SUCCESS);
                        }
                        Some(last) => {
                            // Still in quiesce period
                            let remaining = self.quiesce_seconds - now.duration_since(last).as_secs();
                            warn!(
                                deployment = %self.gate_deployment,
                                remaining_seconds = remaining,
                                "Self-deploy gate: still in quiesce period"
                            );
                            flowfile.put_attribute("frost.gate.reason", "quiesce_period");
                            flowfile.put_attribute("frost.gate.remaining_seconds", &remaining.to_string());
                            session.transfer(flowfile, Relationship::new("wait"));
                        }
                        None => {
                            // Start quiesce period
                            self.last_empty_check = Some(now);
                            warn!(
                                deployment = %self.gate_deployment,
                                quiesce_seconds = self.quiesce_seconds,
                                "Self-deploy gate: starting quiesce period"
                            );
                            flowfile.put_attribute("frost.gate.reason", "starting_quiesce");
                            session.transfer(flowfile, Relationship::new("wait"));
                        }
                    }
                }
                Ok(false) => {
                    // Queues not empty, reset quiesce timer
                    self.last_empty_check = None;
                    warn!(
                        deployment = %self.gate_deployment,
                        "Self-deploy gate: queues not empty, waiting"
                    );
                    flowfile.put_attribute("frost.gate.reason", "queues_not_empty");
                    session.transfer(flowfile, Relationship::new("wait"));
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        "Self-deploy gate: failed to check queues"
                    );
                    flowfile.put_attribute("frost.gate.error", &e.to_string());
                    session.transfer(flowfile, Relationship::FAILURE);
                }
            }
        }

        Ok(())
    }

    async fn on_stopped(&mut self, _context: &ProcessContext) -> Result<(), ProcessorError> {
        self.redis_client = None;
        self.last_empty_check = None;
        info!("SelfDeployGate stopped");
        Ok(())
    }
}

impl SelfDeployGate {
    /// Check if frost-bus queues are empty (no processing or pending jobs)
    async fn check_queues_empty(&self, client: &redis::Client) -> Result<bool, ProcessorError> {
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| ProcessorError::Other(format!("Redis connection failed: {}", e)))?;

        // Check processing queue
        let processing_count: i64 = conn
            .llen("frost-bus:*:processing")
            .await
            .unwrap_or(0);

        // Check pending queue
        let pending_count: i64 = conn
            .llen("frost-bus:*:pending")
            .await
            .unwrap_or(0);

        // Also check specific job type queues
        let pending_keys: Vec<String> = conn
            .keys("frost-bus:pending:*")
            .await
            .unwrap_or_default();

        let mut total_pending = 0i64;
        for key in pending_keys {
            let count: i64 = conn.llen(&key).await.unwrap_or(0);
            total_pending += count;
        }

        let is_empty = processing_count == 0 && pending_count == 0 && total_pending == 0;

        if !is_empty {
            info!(
                processing = processing_count,
                pending = pending_count + total_pending,
                "frost-bus queue status"
            );
        }

        Ok(is_empty)
    }
}

// Register the processor
nifi_rust_api::register_processor!(SelfDeployGate);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_processor_metadata() {
        let processor = SelfDeployGate::default();
        assert_eq!(processor.get_type(), "org.apache.nifi.frost.SelfDeployGate");
        assert!(!processor.get_property_descriptors().is_empty());
        assert_eq!(processor.get_relationships().len(), 3);
    }
}
