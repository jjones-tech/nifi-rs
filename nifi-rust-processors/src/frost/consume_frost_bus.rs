//! ConsumeFrostBus Processor
//!
//! Consumes jobs from frost-bus (Valkey/Redis) and converts them to FlowFiles.

use super::{attributes, FrostJob};
use bytes::Bytes;
use nifi_rust_api::prelude::*;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use tracing::{error, info, warn};

/// Processor that consumes jobs from frost-bus
pub struct ConsumeFrostBus {
    /// Redis connection
    connection: Option<ConnectionManager>,
    /// Queue name
    queue_name: String,
    /// Job types to filter (empty = all)
    job_types: Vec<String>,
    /// Batch size per trigger
    batch_size: usize,
    /// Block timeout in milliseconds
    block_timeout_ms: u64,
}

impl Default for ConsumeFrostBus {
    fn default() -> Self {
        Self {
            connection: None,
            queue_name: "frost:jobs".to_string(),
            job_types: vec![],
            batch_size: 10,
            block_timeout_ms: 1000,
        }
    }
}

#[async_trait]
impl Processor for ConsumeFrostBus {
    fn get_type(&self) -> &'static str {
        "org.apache.nifi.frost.ConsumeFrostBus"
    }

    fn get_description(&self) -> &'static str {
        "Consumes jobs from frost-bus (Valkey/Redis) and creates FlowFiles for each job"
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
                .description("Name of the frost-bus queue to consume from")
                .required(true)
                .default_value(PropertyValue::String("frost:jobs".to_string()))
                .build(),
            PropertyDescriptor::new("job.types")
                .display_name("Job Types")
                .description("Comma-separated list of job types to consume (empty = all)")
                .required(false)
                .build(),
            PropertyDescriptor::new("batch.size")
                .display_name("Batch Size")
                .description("Maximum number of jobs to consume per trigger")
                .required(false)
                .default_value(PropertyValue::Integer(10))
                .build(),
            PropertyDescriptor::new("block.timeout.ms")
                .display_name("Block Timeout (ms)")
                .description("How long to wait for new jobs (milliseconds)")
                .required(false)
                .default_value(PropertyValue::Integer(1000))
                .build(),
        ]
    }

    fn get_relationships(&self) -> Vec<Relationship> {
        vec![Relationship::SUCCESS]
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

        if let Some(types) = context.get_property_string("job.types") {
            self.job_types = types
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }

        if let Some(batch) = context.get_property_int("batch.size") {
            self.batch_size = batch as usize;
        }

        if let Some(timeout) = context.get_property_int("block.timeout.ms") {
            self.block_timeout_ms = timeout as u64;
        }

        info!(
            queue = %self.queue_name,
            job_types = ?self.job_types,
            batch_size = self.batch_size,
            "ConsumeFrostBus scheduled"
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

        // Use BRPOP for blocking receive
        let timeout_secs = (self.block_timeout_ms as f64 / 1000.0).ceil();

        for _ in 0..self.batch_size {
            // Try to pop a job from the queue
            let result: Option<(String, String)> = conn
                .brpop(&self.queue_name, timeout_secs)
                .await
                .map_err(|e| ProcessorError::Processing(format!("Redis error: {}", e)))?;

            match result {
                Some((_queue, data)) => {
                    // Parse the job
                    let job: FrostJob = serde_json::from_str(&data).map_err(|e| {
                        ProcessorError::Serialization(format!("Failed to parse job: {}", e))
                    })?;

                    // Filter by job type if configured
                    if !self.job_types.is_empty() && !self.job_types.contains(&job.job_type) {
                        // Re-queue the job (push back to left side)
                        let _: () = conn
                            .lpush(&self.queue_name, &data)
                            .await
                            .map_err(|e| ProcessorError::Processing(format!("Failed to re-queue: {}", e)))?;
                        continue;
                    }

                    // Create FlowFile from job
                    let mut flowfile = session.create();

                    // Set attributes
                    flowfile.put_attribute(attributes::FROST_JOB_ID, job.id.to_string());
                    flowfile.put_attribute(attributes::FROST_JOB_TYPE, &job.job_type);

                    if let Some(ws_id) = job.workspace_id {
                        flowfile.put_attribute(attributes::FROST_WORKSPACE_ID, ws_id.to_string());
                    }

                    if let Some(proj_id) = job.project_id {
                        flowfile.put_attribute(attributes::FROST_PROJECT_ID, proj_id.to_string());
                    }

                    // Set content as JSON payload
                    let content = serde_json::to_vec(&job.payload)?;
                    session.write_content(&mut flowfile, Bytes::from(content));

                    session.transfer(flowfile, Relationship::SUCCESS);
                }
                None => {
                    // No more jobs available, stop batching
                    break;
                }
            }
        }

        Ok(())
    }

    async fn on_stopped(&mut self, _context: &ProcessContext) -> Result<(), ProcessorError> {
        self.connection = None;
        info!("ConsumeFrostBus stopped");
        Ok(())
    }
}

// Register the processor
nifi_rust_api::register_processor!(ConsumeFrostBus);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_processor_metadata() {
        let processor = ConsumeFrostBus::default();
        assert_eq!(
            processor.get_type(),
            "org.apache.nifi.frost.ConsumeFrostBus"
        );
        assert!(!processor.get_property_descriptors().is_empty());
        assert!(!processor.get_relationships().is_empty());
    }
}
