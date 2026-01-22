//! gRPC Server implementation for Rust NiFi processors

use crate::proto::{
    rust_processor_service_server::RustProcessorService, CreateProcessorRequest, Empty,
    FlowFileProto, HealthResponse, ProcessorHandle, ProcessorTypeInfo, ProcessorTypesResponse,
    PropertyDescriptor as ProtoPropertyDescriptor, RelationshipInfo, ScheduledRequest,
    ScheduledResponse, StoppedRequest, StoppedResponse, TransferTarget as ProtoTransferTarget,
    TriggerRequest, TriggerResponse,
};
use bytes::Bytes;
use nifi_rust_api::{
    get_registered_processors, FlowFile, ProcessContext, ProcessSession, Processor,
    ProcessorError, PropertyValue, Relationship,
};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info, warn};

/// Processor instance with its state
struct ProcessorInstance {
    processor: Box<dyn Processor>,
    context: ProcessContext,
}

/// The gRPC server implementation
pub struct NifiRustServer {
    /// Active processor instances
    processors: Arc<RwLock<HashMap<String, RwLock<ProcessorInstance>>>>,
}

impl NifiRustServer {
    pub fn new() -> Self {
        Self {
            processors: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Convert a Rust PropertyDescriptor to proto
    fn property_to_proto(
        prop: &nifi_rust_api::PropertyDescriptor,
    ) -> ProtoPropertyDescriptor {
        ProtoPropertyDescriptor {
            name: prop.name.to_string(),
            display_name: prop.display_name.to_string(),
            description: prop.description.to_string(),
            required: prop.required,
            default_value: prop
                .default_value
                .as_ref()
                .map(|v| match v {
                    PropertyValue::String(s) => s.clone(),
                    PropertyValue::Integer(i) => i.to_string(),
                    PropertyValue::Boolean(b) => b.to_string(),
                    PropertyValue::Duration(d) => d.as_millis().to_string(),
                })
                .unwrap_or_default(),
            sensitive: prop.sensitive,
            expression_language_supported: prop.expression_language_supported,
            allowable_values: prop
                .allowable_values
                .as_ref()
                .map(|v| v.iter().map(|s| s.to_string()).collect())
                .unwrap_or_default(),
        }
    }

    /// Convert a Rust Relationship to proto
    fn relationship_to_proto(rel: &Relationship) -> RelationshipInfo {
        RelationshipInfo {
            name: rel.name.to_string(),
            description: rel.description.to_string(),
            auto_terminate: rel.auto_terminate,
        }
    }

    /// Convert proto FlowFile to Rust FlowFile
    fn flowfile_from_proto(proto: FlowFileProto) -> FlowFile {
        let mut ff = FlowFile::new();
        if let Ok(id) = uuid::Uuid::parse_str(&proto.id) {
            ff.id = id;
        }
        ff.attributes = proto.attributes;
        if !proto.content.is_empty() {
            ff.set_content(Bytes::from(proto.content));
        }
        ff.size = proto.size;
        ff.entry_date = proto.entry_date;
        ff.lineage_start_date = proto.lineage_start_date;
        ff
    }

    /// Convert Rust FlowFile to proto
    fn flowfile_to_proto(ff: &FlowFile) -> FlowFileProto {
        FlowFileProto {
            id: ff.id.to_string(),
            attributes: ff.attributes.clone(),
            content: ff.content().map(|b| b.to_vec()).unwrap_or_default(),
            size: ff.size,
            entry_date: ff.entry_date,
            lineage_start_date: ff.lineage_start_date,
        }
    }

    /// Convert proto PropertyValue to Rust PropertyValue
    fn property_from_proto(
        pv: &crate::proto::PropertyValue,
    ) -> Option<(String, PropertyValue)> {
        use crate::proto::property_value::Value;

        let value = match &pv.value {
            Some(Value::StringValue(s)) => PropertyValue::String(s.clone()),
            Some(Value::IntValue(i)) => PropertyValue::Integer(*i),
            Some(Value::BoolValue(b)) => PropertyValue::Boolean(*b),
            Some(Value::DurationMillis(d)) => {
                PropertyValue::Duration(std::time::Duration::from_millis(*d as u64))
            }
            None => return None,
        };

        Some((pv.name.clone(), value))
    }
}

impl Default for NifiRustServer {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl RustProcessorService for NifiRustServer {
    async fn get_processor_types(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ProcessorTypesResponse>, Status> {
        let processor_types: Vec<ProcessorTypeInfo> = get_registered_processors()
            .into_iter()
            .map(|factory| {
                let processor = factory.create();
                ProcessorTypeInfo {
                    type_name: processor.get_type().to_string(),
                    description: processor.get_description().to_string(),
                    properties: processor
                        .get_property_descriptors()
                        .iter()
                        .map(Self::property_to_proto)
                        .collect(),
                    relationships: processor
                        .get_relationships()
                        .iter()
                        .map(Self::relationship_to_proto)
                        .collect(),
                }
            })
            .collect();

        info!(count = processor_types.len(), "Returning processor types");

        Ok(Response::new(ProcessorTypesResponse { processor_types }))
    }

    async fn create_processor(
        &self,
        request: Request<CreateProcessorRequest>,
    ) -> Result<Response<ProcessorHandle>, Status> {
        let req = request.into_inner();

        // Find the factory for this processor type
        let factory = get_registered_processors()
            .into_iter()
            .find(|f| {
                let p = f.create();
                p.get_type() == req.processor_type
            })
            .ok_or_else(|| {
                Status::not_found(format!("Processor type not found: {}", req.processor_type))
            })?;

        // Create the processor instance
        let processor = factory.create();
        let processor_type = processor.get_type().to_string();

        let context = ProcessContext::new(&req.processor_id, &req.processor_name);

        let instance = ProcessorInstance { processor, context };

        // Store it
        {
            let mut processors = self.processors.write().await;
            processors.insert(req.processor_id.clone(), RwLock::new(instance));
        }

        info!(
            processor_id = %req.processor_id,
            processor_type = %processor_type,
            "Created processor instance"
        );

        Ok(Response::new(ProcessorHandle {
            processor_id: req.processor_id,
            processor_type,
        }))
    }

    async fn destroy_processor(
        &self,
        request: Request<ProcessorHandle>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();

        let mut processors = self.processors.write().await;
        if processors.remove(&req.processor_id).is_some() {
            info!(processor_id = %req.processor_id, "Destroyed processor instance");
            Ok(Response::new(Empty {}))
        } else {
            Err(Status::not_found(format!(
                "Processor not found: {}",
                req.processor_id
            )))
        }
    }

    async fn on_scheduled(
        &self,
        request: Request<ScheduledRequest>,
    ) -> Result<Response<ScheduledResponse>, Status> {
        let req = request.into_inner();

        let processors = self.processors.read().await;
        let instance_lock = processors.get(&req.processor_id).ok_or_else(|| {
            Status::not_found(format!("Processor not found: {}", req.processor_id))
        })?;

        let mut instance = instance_lock.write().await;

        // Update context with properties
        instance.context.max_concurrent_tasks = req.max_concurrent_tasks as u32;
        instance.context.scheduling_period_millis = req.scheduling_period_millis as u64;
        instance.context.is_scheduled = true;

        for prop in &req.properties {
            if let Some((name, value)) = Self::property_from_proto(prop) {
                instance.context.set_property(name, value);
            }
        }

        // Call on_scheduled
        let ctx = instance.context.clone();
        match instance.processor.on_scheduled(&ctx).await {
            Ok(()) => {
                info!(processor_id = %req.processor_id, "Processor scheduled successfully");
                Ok(Response::new(ScheduledResponse {
                    success: true,
                    error_message: String::new(),
                }))
            }
            Err(e) => {
                error!(processor_id = %req.processor_id, error = %e, "Failed to schedule processor");
                Ok(Response::new(ScheduledResponse {
                    success: false,
                    error_message: e.to_string(),
                }))
            }
        }
    }

    async fn on_stopped(
        &self,
        request: Request<StoppedRequest>,
    ) -> Result<Response<StoppedResponse>, Status> {
        let req = request.into_inner();

        let processors = self.processors.read().await;
        let instance_lock = processors.get(&req.processor_id).ok_or_else(|| {
            Status::not_found(format!("Processor not found: {}", req.processor_id))
        })?;

        let mut instance = instance_lock.write().await;
        instance.context.is_scheduled = false;

        let ctx = instance.context.clone();
        match instance.processor.on_stopped(&ctx).await {
            Ok(()) => {
                info!(processor_id = %req.processor_id, "Processor stopped successfully");
                Ok(Response::new(StoppedResponse {
                    success: true,
                    error_message: String::new(),
                }))
            }
            Err(e) => {
                error!(processor_id = %req.processor_id, error = %e, "Failed to stop processor");
                Ok(Response::new(StoppedResponse {
                    success: false,
                    error_message: e.to_string(),
                }))
            }
        }
    }

    async fn on_trigger(
        &self,
        request: Request<TriggerRequest>,
    ) -> Result<Response<TriggerResponse>, Status> {
        let req = request.into_inner();

        let processors = self.processors.read().await;
        let instance_lock = processors.get(&req.processor_id).ok_or_else(|| {
            Status::not_found(format!("Processor not found: {}", req.processor_id))
        })?;

        let mut instance = instance_lock.write().await;

        // Convert input FlowFiles
        let input_ffs: Vec<FlowFile> = req
            .input_flowfiles
            .into_iter()
            .map(Self::flowfile_from_proto)
            .collect();

        let mut session = ProcessSession::with_input(input_ffs);

        // Call on_trigger
        let ctx = instance.context.clone();
        match instance.processor.on_trigger(&ctx, &mut session).await {
            Ok(()) => {
                // Collect results
                let transferred: Vec<ProtoTransferTarget> = session
                    .take_transferred()
                    .into_iter()
                    .map(|t| ProtoTransferTarget {
                        flowfile: Some(Self::flowfile_to_proto(&t.flowfile)),
                        relationship: t.relationship.name.to_string(),
                    })
                    .collect();

                let removed_ids: Vec<String> = session
                    .take_removed()
                    .into_iter()
                    .map(|ff| ff.id.to_string())
                    .collect();

                Ok(Response::new(TriggerResponse {
                    success: true,
                    error_message: String::new(),
                    transferred,
                    removed_ids,
                }))
            }
            Err(e) => {
                warn!(processor_id = %req.processor_id, error = %e, "Processor trigger failed");
                Ok(Response::new(TriggerResponse {
                    success: false,
                    error_message: e.to_string(),
                    transferred: vec![],
                    removed_ids: vec![],
                }))
            }
        }
    }

    type OnTriggerStreamStream =
        Pin<Box<dyn Stream<Item = Result<TriggerResponse, Status>> + Send>>;

    async fn on_trigger_stream(
        &self,
        request: Request<Streaming<TriggerRequest>>,
    ) -> Result<Response<Self::OnTriggerStreamStream>, Status> {
        let mut stream = request.into_inner();
        let processors = self.processors.clone();

        let output = async_stream::try_stream! {
            while let Some(req) = stream.message().await? {
                let processors = processors.read().await;
                let instance_lock = match processors.get(&req.processor_id) {
                    Some(lock) => lock,
                    None => {
                        yield TriggerResponse {
                            success: false,
                            error_message: format!("Processor not found: {}", req.processor_id),
                            transferred: vec![],
                            removed_ids: vec![],
                        };
                        continue;
                    }
                };

                let mut instance = instance_lock.write().await;

                let input_ffs: Vec<FlowFile> = req
                    .input_flowfiles
                    .into_iter()
                    .map(Self::flowfile_from_proto)
                    .collect();

                let mut session = ProcessSession::with_input(input_ffs);

                let ctx = instance.context.clone();
                match instance.processor.on_trigger(&ctx, &mut session).await {
                    Ok(()) => {
                        let transferred: Vec<ProtoTransferTarget> = session
                            .take_transferred()
                            .into_iter()
                            .map(|t| ProtoTransferTarget {
                                flowfile: Some(Self::flowfile_to_proto(&t.flowfile)),
                                relationship: t.relationship.name.to_string(),
                            })
                            .collect();

                        let removed_ids: Vec<String> = session
                            .take_removed()
                            .into_iter()
                            .map(|ff| ff.id.to_string())
                            .collect();

                        yield TriggerResponse {
                            success: true,
                            error_message: String::new(),
                            transferred,
                            removed_ids,
                        };
                    }
                    Err(e) => {
                        yield TriggerResponse {
                            success: false,
                            error_message: e.to_string(),
                            transferred: vec![],
                            removed_ids: vec![],
                        };
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(output)))
    }

    async fn health_check(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<HealthResponse>, Status> {
        let processor_count = self.processors.read().await.len();

        Ok(Response::new(HealthResponse {
            healthy: true,
            version: env!("CARGO_PKG_VERSION").to_string(),
            processor_count: processor_count as i32,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_check() {
        let server = NifiRustServer::new();
        let request = Request::new(Empty {});
        let response = server.health_check(request).await.unwrap();
        let resp = response.into_inner();

        assert!(resp.healthy);
        assert!(!resp.version.is_empty());
    }

    #[tokio::test]
    async fn test_get_processor_types() {
        let server = NifiRustServer::new();
        let request = Request::new(Empty {});
        let response = server.get_processor_types(request).await.unwrap();
        let resp = response.into_inner();

        // Should return empty list since no processors are registered in test
        assert!(resp.processor_types.is_empty());
    }
}
