//! PublishFrostBus Processor
//!
//! Publishes jobs to frost-bus (Valkey/Redis) from FlowFiles.

use super::{attributes, FrostJob};
use nifi_rust_api::prelude::*;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use tracing::{error, info};

/// Processor that publishes jobs to frost-bus
pub struct PublishFrostBus {
    /// Redis connection
    connection: Option<ConnectionManager>,
    /// Queue name
    queue_name: String,
    /// Job type to publish as
    job_type: String,
}

impl Default for PublishFrostBus {
    fn default() -> Self {
        Self {
            connection: None,
            queue_name: "frost:jobs".to_string(),
            job_type: "generic".to_string(),
        }
    }
}

#[async_trait]
impl Processor for PublishFrostBus {
    fn get_type(&self) -> &'static str {
        "org.apache.nifi.frost.PublishFrostBus"
    }

    fn get_description(&self) -> &'static str {
        "Publishes FlowFiles as jobs to frost-bus (Valkey/Redis)"
    }

    fn get_property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![
            PropertyDescriptor::new("redis.url")
                .display_name("Redis URL")
                .description("Redis/Valkey connection URL (e.g., redis://localhost:6379)")
                .required(true)
                .build(),
            PropertyDescriptor::new("queue.name")
                .display_name("Queue Name")
                .description("Name of the frost-bus queue to publish to")
                .required(true)
                .default_value(PropertyValue::String("frost:jobs".to_string()))
                .build(),
            PropertyDescriptor::new("job.type")
                .display_name("Job Type")
                .description("Job type to publish as (or use attribute frost.job.type)")
                .required(false)
                .default_value(PropertyValue::String("generic".to_string()))
                .build(),
        ]
    }

    fn get_relationships(&self) -> Vec<Relationship> {
        vec![Relationship::SUCCESS, Relationship::FAILURE]
    }

    async fn on_scheduled(&mut self, context: &ProcessContext) -> Result<(), ProcessorError> {
        // Get Redis URL
        let redis_url = context
            .get_property_string("redis.url")
            .ok_or_else(|| ProcessorError::InvalidConfig("redis.url is required".to_string()))?;

        // Connect to Redis
        let client = redis::Client::open(redis_url)
            .map_err(|e| ProcessorError::InvalidConfig(format!("Invalid Redis URL: {}", e)))?;

        let connection = ConnectionManager::new(client)
            .await
            .map_err(|e| ProcessorError::Other(format!("Failed to connect to Redis: {}", e)))?;

        self.connection = Some(connection);

        // Get other config
        if let Some(queue) = context.get_property_string("queue.name") {
            self.queue_name = queue.to_string();
        }

        if let Some(job_type) = context.get_property_string("job.type") {
            self.job_type = job_type.to_string();
        }

        info!(
            queue = %self.queue_name,
            job_type = %self.job_type,
            "PublishFrostBus scheduled"
        );

        Ok(())
    }

    async fn on_trigger(
        &mut self,
        _context: &ProcessContext,
        session: &mut ProcessSession,
    ) -> Result<(), ProcessorError> {
        let conn = self.connection.as_mut().ok_or_else(|| {
            ProcessorError::NotInitialized("Redis connection not established".to_string())
        })?;

        while let Some(mut flowfile) = session.get() {
            // Get job type from attribute or use configured default
            let job_type = flowfile
                .get_attribute(attributes::FROST_JOB_TYPE)
                .map(|s| s.to_string())
                .unwrap_or_else(|| self.job_type.clone());

            // Parse content as JSON payload
            let payload: serde_json::Value = if let Some(content) = flowfile.content() {
                serde_json::from_slice(content)
                    .unwrap_or_else(|_| serde_json::json!({"raw": String::from_utf8_lossy(content).to_string()}))
            } else {
                serde_json::json!({})
            };

            // Create job
            let mut job = FrostJob::new(&job_type, payload);

            // Copy workspace/project from attributes if present
            if let Some(ws_id) = flowfile.get_attribute(attributes::FROST_WORKSPACE_ID) {
                if let Ok(id) = uuid::Uuid::parse_str(ws_id) {
                    job.workspace_id = Some(id);
                }
            }

            if let Some(proj_id) = flowfile.get_attribute(attributes::FROST_PROJECT_ID) {
                if let Ok(id) = uuid::Uuid::parse_str(proj_id) {
                    job.project_id = Some(id);
                }
            }

            // Serialize and publish
            let job_json = serde_json::to_string(&job)?;

            match conn.lpush::<_, _, ()>(&self.queue_name, &job_json).await {
                Ok(_) => {
                    // Set job ID attribute and transfer to success
                    flowfile.put_attribute(attributes::FROST_JOB_ID, job.id.to_string());
                    session.transfer(flowfile, Relationship::SUCCESS);
                    info!(job_id = %job.id, job_type = %job_type, "Published job to frost-bus");
                }
                Err(e) => {
                    error!(error = %e, "Failed to publish to frost-bus");
                    session.transfer(flowfile, Relationship::FAILURE);
                }
            }
        }

        Ok(())
    }

    async fn on_stopped(&mut self, _context: &ProcessContext) -> Result<(), ProcessorError> {
        self.connection = None;
        info!("PublishFrostBus stopped");
        Ok(())
    }
}

// Register the processor
nifi_rust_api::register_processor!(PublishFrostBus);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_processor_metadata() {
        let processor = PublishFrostBus::default();
        assert_eq!(
            processor.get_type(),
            "org.apache.nifi.frost.PublishFrostBus"
        );
        assert!(!processor.get_property_descriptors().is_empty());
        assert_eq!(processor.get_relationships().len(), 2);
    }
}
