//! Processor trait and related types

use crate::context::{ProcessContext, ProcessSession};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

/// Error type for processor operations
#[derive(Debug, thiserror::Error)]
pub enum ProcessorError {
    #[error("Not initialized: {0}")]
    NotInitialized(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Processing failed: {0}")]
    Processing(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("{0}")]
    Other(String),
}

impl From<serde_json::Error> for ProcessorError {
    fn from(e: serde_json::Error) -> Self {
        ProcessorError::Serialization(e.to_string())
    }
}

/// Describes a relationship that a processor may transfer FlowFiles to
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Relationship {
    pub name: &'static str,
    pub description: &'static str,
    pub auto_terminate: bool,
}

impl Relationship {
    /// Create a new relationship
    pub const fn new(name: &'static str, description: &'static str) -> Self {
        Self {
            name,
            description,
            auto_terminate: false,
        }
    }

    /// Standard success relationship
    pub const SUCCESS: Self = Self::new("success", "FlowFiles that were processed successfully");

    /// Standard failure relationship
    pub const FAILURE: Self = Self::new("failure", "FlowFiles that failed processing");

    /// Standard original relationship (for cloning processors)
    pub const ORIGINAL: Self = Self::new("original", "Original FlowFile");
}

/// Value types for processor properties
#[derive(Debug, Clone)]
pub enum PropertyValue {
    String(String),
    Integer(i64),
    Boolean(bool),
    Duration(std::time::Duration),
}

impl PropertyValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            PropertyValue::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            PropertyValue::Integer(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            PropertyValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_duration(&self) -> Option<std::time::Duration> {
        match self {
            PropertyValue::Duration(d) => Some(*d),
            _ => None,
        }
    }
}

impl From<String> for PropertyValue {
    fn from(s: String) -> Self {
        PropertyValue::String(s)
    }
}

impl From<&str> for PropertyValue {
    fn from(s: &str) -> Self {
        PropertyValue::String(s.to_string())
    }
}

impl From<i64> for PropertyValue {
    fn from(i: i64) -> Self {
        PropertyValue::Integer(i)
    }
}

impl From<bool> for PropertyValue {
    fn from(b: bool) -> Self {
        PropertyValue::Boolean(b)
    }
}

/// Describes a configurable property for a processor
#[derive(Debug, Clone)]
pub struct PropertyDescriptor {
    pub name: &'static str,
    pub display_name: &'static str,
    pub description: &'static str,
    pub required: bool,
    pub default_value: Option<PropertyValue>,
    pub sensitive: bool,
    pub expression_language_supported: bool,
    pub allowable_values: Option<Vec<&'static str>>,
}

impl PropertyDescriptor {
    /// Create a new property descriptor
    pub const fn new(name: &'static str) -> PropertyDescriptorBuilder {
        PropertyDescriptorBuilder::new(name)
    }
}

/// Builder for PropertyDescriptor
pub struct PropertyDescriptorBuilder {
    name: &'static str,
    display_name: Option<&'static str>,
    description: &'static str,
    required: bool,
    default_value: Option<PropertyValue>,
    sensitive: bool,
    expression_language_supported: bool,
    allowable_values: Option<Vec<&'static str>>,
}

impl PropertyDescriptorBuilder {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            display_name: None,
            description: "",
            required: false,
            default_value: None,
            sensitive: false,
            expression_language_supported: false,
            allowable_values: None,
        }
    }

    pub fn display_name(mut self, display_name: &'static str) -> Self {
        self.display_name = Some(display_name);
        self
    }

    pub fn description(mut self, description: &'static str) -> Self {
        self.description = description;
        self
    }

    pub fn required(mut self, required: bool) -> Self {
        self.required = required;
        self
    }

    pub fn default_value(mut self, value: PropertyValue) -> Self {
        self.default_value = Some(value);
        self
    }

    pub fn sensitive(mut self, sensitive: bool) -> Self {
        self.sensitive = sensitive;
        self
    }

    pub fn expression_language_supported(mut self, supported: bool) -> Self {
        self.expression_language_supported = supported;
        self
    }

    pub fn allowable_values(mut self, values: Vec<&'static str>) -> Self {
        self.allowable_values = Some(values);
        self
    }

    pub fn build(self) -> PropertyDescriptor {
        PropertyDescriptor {
            name: self.name,
            display_name: self.display_name.unwrap_or(self.name),
            description: self.description,
            required: self.required,
            default_value: self.default_value,
            sensitive: self.sensitive,
            expression_language_supported: self.expression_language_supported,
            allowable_values: self.allowable_values,
        }
    }
}

/// The core Processor trait that all Rust NiFi processors must implement.
///
/// This trait mirrors the Java Processor interface, providing methods for:
/// - Describing the processor (type, description, properties, relationships)
/// - Lifecycle hooks (onScheduled, onStopped)
/// - Processing logic (onTrigger)
#[async_trait]
pub trait Processor: Send + Sync + 'static {
    /// Returns the fully qualified type name of this processor.
    ///
    /// This should follow Java package naming conventions, e.g.,
    /// "org.apache.nifi.frost.ConsumeFrostBus"
    fn get_type(&self) -> &'static str;

    /// Returns a human-readable description of what this processor does.
    fn get_description(&self) -> &'static str;

    /// Returns the property descriptors for this processor's configuration.
    fn get_property_descriptors(&self) -> Vec<PropertyDescriptor>;

    /// Returns the relationships that this processor may transfer FlowFiles to.
    fn get_relationships(&self) -> Vec<Relationship>;

    /// Called when the processor is scheduled to run.
    ///
    /// This is where you should initialize any resources like connections,
    /// thread pools, etc.
    async fn on_scheduled(&mut self, _context: &ProcessContext) -> Result<(), ProcessorError> {
        Ok(())
    }

    /// Called each time the processor is triggered to run.
    ///
    /// This is where the main processing logic lives. Read FlowFiles from
    /// the session, process them, and transfer them to relationships.
    async fn on_trigger(
        &mut self,
        context: &ProcessContext,
        session: &mut ProcessSession,
    ) -> Result<(), ProcessorError>;

    /// Called when the processor is stopped.
    ///
    /// Clean up any resources allocated in on_scheduled.
    async fn on_stopped(&mut self, _context: &ProcessContext) -> Result<(), ProcessorError> {
        Ok(())
    }
}

/// Type alias for a processor constructor function
pub type ProcessorConstructor = fn() -> Box<dyn Processor>;

/// Factory for creating processor instances.
///
/// This struct is registered via the `register_processor!` macro and
/// collected by the `inventory` crate for runtime discovery.
pub struct ProcessorFactory {
    pub type_name: &'static str,
    pub constructor: ProcessorConstructor,
}

impl ProcessorFactory {
    /// Create an instance of this processor
    pub fn create(&self) -> Box<dyn Processor> {
        (self.constructor)()
    }
}

/// Helper function to create a processor - used by register_processor! macro
pub fn make_processor<P: Processor + Default>() -> Box<dyn Processor> {
    Box::new(P::default())
}

// Register ProcessorFactory with inventory
inventory::collect!(ProcessorFactory);

/// Get all registered processor factories
pub fn get_registered_processors() -> inventory::iter<ProcessorFactory> {
    inventory::iter::<ProcessorFactory>
}

/// Registry of processor instances keyed by ID
pub struct ProcessorRegistry {
    processors: HashMap<String, Arc<tokio::sync::RwLock<Box<dyn Processor>>>>,
}

impl ProcessorRegistry {
    pub fn new() -> Self {
        Self {
            processors: HashMap::new(),
        }
    }

    /// Create a processor instance and register it
    pub fn create_processor(&mut self, processor_type: &str) -> Option<String> {
        let factory = get_registered_processors()
            .into_iter()
            .find(|f| f.type_name == processor_type)?;
        let processor = factory.create();
        let id = uuid::Uuid::new_v4().to_string();
        self.processors.insert(
            id.clone(),
            Arc::new(tokio::sync::RwLock::new(processor)),
        );
        Some(id)
    }

    /// Get a processor by ID
    pub fn get(
        &self,
        id: &str,
    ) -> Option<Arc<tokio::sync::RwLock<Box<dyn Processor>>>> {
        self.processors.get(id).cloned()
    }

    /// Remove a processor
    pub fn remove(&mut self, id: &str) -> bool {
        self.processors.remove(id).is_some()
    }

    /// List all available processor types
    pub fn list_types() -> Vec<&'static str> {
        get_registered_processors()
            .into_iter()
            .map(|f| f.type_name)
            .collect()
    }
}

impl Default for ProcessorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct TestProcessor;

    #[async_trait]
    impl Processor for TestProcessor {
        fn get_type(&self) -> &'static str {
            "org.test.TestProcessor"
        }

        fn get_description(&self) -> &'static str {
            "A test processor"
        }

        fn get_property_descriptors(&self) -> Vec<PropertyDescriptor> {
            vec![PropertyDescriptor::new("test.property")
                .description("A test property")
                .required(true)
                .build()]
        }

        fn get_relationships(&self) -> Vec<Relationship> {
            vec![Relationship::SUCCESS]
        }

        async fn on_trigger(
            &mut self,
            _context: &ProcessContext,
            _session: &mut ProcessSession,
        ) -> Result<(), ProcessorError> {
            Ok(())
        }
    }

    #[test]
    fn test_relationship_constants() {
        assert_eq!(Relationship::SUCCESS.name, "success");
        assert_eq!(Relationship::FAILURE.name, "failure");
    }

    #[test]
    fn test_property_descriptor_builder() {
        let prop = PropertyDescriptor::new("my.property")
            .display_name("My Property")
            .description("A custom property")
            .required(true)
            .sensitive(true)
            .build();

        assert_eq!(prop.name, "my.property");
        assert_eq!(prop.display_name, "My Property");
        assert!(prop.required);
        assert!(prop.sensitive);
    }

    #[test]
    fn test_property_value_conversions() {
        let s: PropertyValue = "hello".into();
        assert_eq!(s.as_str(), Some("hello"));

        let i: PropertyValue = 42i64.into();
        assert_eq!(i.as_i64(), Some(42));

        let b: PropertyValue = true.into();
        assert_eq!(b.as_bool(), Some(true));
    }
}
