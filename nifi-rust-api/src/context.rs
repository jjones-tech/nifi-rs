//! ProcessContext and ProcessSession abstractions
//!
//! These types provide the interface between a processor and the NiFi framework.

use crate::flowfile::FlowFile;
use crate::processor::{PropertyValue, Relationship};
use bytes::Bytes;
use std::collections::HashMap;

/// ProcessContext provides access to processor configuration and NiFi services.
///
/// This is passed to lifecycle methods (on_scheduled, on_stopped) and on_trigger.
#[derive(Debug, Clone)]
pub struct ProcessContext {
    /// Processor identifier
    pub processor_id: String,

    /// Processor name (user-defined)
    pub processor_name: String,

    /// Configured properties
    properties: HashMap<String, PropertyValue>,

    /// Maximum concurrent tasks
    pub max_concurrent_tasks: u32,

    /// Scheduling period in milliseconds
    pub scheduling_period_millis: u64,

    /// Whether the processor is scheduled to run
    pub is_scheduled: bool,
}

impl ProcessContext {
    /// Create a new ProcessContext
    pub fn new(processor_id: impl Into<String>, processor_name: impl Into<String>) -> Self {
        Self {
            processor_id: processor_id.into(),
            processor_name: processor_name.into(),
            properties: HashMap::new(),
            max_concurrent_tasks: 1,
            scheduling_period_millis: 0,
            is_scheduled: false,
        }
    }

    /// Get a property value by name
    pub fn get_property(&self, name: &str) -> Option<&PropertyValue> {
        self.properties.get(name)
    }

    /// Get a property value as a string
    pub fn get_property_string(&self, name: &str) -> Option<&str> {
        self.properties.get(name).and_then(|v| v.as_str())
    }

    /// Get a property value as an integer
    pub fn get_property_int(&self, name: &str) -> Option<i64> {
        self.properties.get(name).and_then(|v| v.as_i64())
    }

    /// Get a property value as a boolean
    pub fn get_property_bool(&self, name: &str) -> Option<bool> {
        self.properties.get(name).and_then(|v| v.as_bool())
    }

    /// Set a property value
    pub fn set_property(&mut self, name: impl Into<String>, value: PropertyValue) {
        self.properties.insert(name.into(), value);
    }

    /// Check if a property is set
    pub fn has_property(&self, name: &str) -> bool {
        self.properties.contains_key(name)
    }

    /// Get all property names
    pub fn property_names(&self) -> impl Iterator<Item = &str> {
        self.properties.keys().map(|s| s.as_str())
    }
}

impl Default for ProcessContext {
    fn default() -> Self {
        Self::new("", "")
    }
}

/// Represents a transfer target for a FlowFile
#[derive(Debug, Clone)]
pub struct TransferTarget {
    pub flowfile: FlowFile,
    pub relationship: Relationship,
}

/// ProcessSession manages FlowFiles during a single execution of on_trigger.
///
/// The session provides methods to:
/// - Get FlowFiles from input queues
/// - Create new FlowFiles
/// - Modify FlowFile content and attributes
/// - Transfer FlowFiles to relationships
pub struct ProcessSession {
    /// FlowFiles available for processing (from input queues)
    input_queue: Vec<FlowFile>,

    /// FlowFiles that have been transferred
    transferred: Vec<TransferTarget>,

    /// FlowFiles that have been removed (consumed without transfer)
    removed: Vec<FlowFile>,

    /// Counter for pending commits
    pending_count: usize,
}

impl ProcessSession {
    /// Create a new session
    pub fn new() -> Self {
        Self {
            input_queue: Vec::new(),
            transferred: Vec::new(),
            removed: Vec::new(),
            pending_count: 0,
        }
    }

    /// Create a session with input FlowFiles
    pub fn with_input(flowfiles: Vec<FlowFile>) -> Self {
        let count = flowfiles.len();
        Self {
            input_queue: flowfiles,
            transferred: Vec::new(),
            removed: Vec::new(),
            pending_count: count,
        }
    }

    /// Get a FlowFile from the input queue
    pub fn get(&mut self) -> Option<FlowFile> {
        self.input_queue.pop()
    }

    /// Peek at the next FlowFile without removing it
    pub fn peek(&self) -> Option<&FlowFile> {
        self.input_queue.last()
    }

    /// Check if there are FlowFiles available
    pub fn has_input(&self) -> bool {
        !self.input_queue.is_empty()
    }

    /// Get the number of FlowFiles in the input queue
    pub fn input_count(&self) -> usize {
        self.input_queue.len()
    }

    /// Create a new FlowFile
    pub fn create(&mut self) -> FlowFile {
        self.pending_count += 1;
        FlowFile::new()
    }

    /// Create a new FlowFile with content
    pub fn create_with_content(&mut self, content: Bytes) -> FlowFile {
        self.pending_count += 1;
        FlowFile::with_content(content)
    }

    /// Clone an existing FlowFile (creates a new ID, inherits attributes)
    pub fn clone_flowfile(&mut self, source: &FlowFile) -> FlowFile {
        self.pending_count += 1;
        source.clone_with_new_id()
    }

    /// Write content to a FlowFile
    pub fn write_content(&mut self, flowfile: &mut FlowFile, content: Bytes) {
        flowfile.set_content(content);
    }

    /// Read content from a FlowFile
    pub fn read_content(&self, flowfile: &FlowFile) -> Option<Bytes> {
        flowfile.content().cloned()
    }

    /// Put an attribute on a FlowFile
    pub fn put_attribute(
        &mut self,
        flowfile: &mut FlowFile,
        name: impl Into<String>,
        value: impl Into<String>,
    ) {
        flowfile.put_attribute(name, value);
    }

    /// Remove an attribute from a FlowFile
    pub fn remove_attribute(&mut self, flowfile: &mut FlowFile, name: &str) -> Option<String> {
        flowfile.remove_attribute(name)
    }

    /// Transfer a FlowFile to a relationship
    pub fn transfer(&mut self, flowfile: FlowFile, relationship: Relationship) {
        self.pending_count = self.pending_count.saturating_sub(1);
        self.transferred.push(TransferTarget {
            flowfile,
            relationship,
        });
    }

    /// Remove a FlowFile without transferring (acknowledge/consume)
    pub fn remove(&mut self, flowfile: FlowFile) {
        self.pending_count = self.pending_count.saturating_sub(1);
        self.removed.push(flowfile);
    }

    /// Get all transferred FlowFiles
    pub fn get_transferred(&self) -> &[TransferTarget] {
        &self.transferred
    }

    /// Get all removed FlowFiles
    pub fn get_removed(&self) -> &[FlowFile] {
        &self.removed
    }

    /// Take all transferred FlowFiles (consuming the list)
    pub fn take_transferred(&mut self) -> Vec<TransferTarget> {
        std::mem::take(&mut self.transferred)
    }

    /// Take all removed FlowFiles (consuming the list)
    pub fn take_removed(&mut self) -> Vec<FlowFile> {
        std::mem::take(&mut self.removed)
    }

    /// Check if the session has any pending FlowFiles (created but not transferred/removed)
    pub fn has_pending(&self) -> bool {
        self.pending_count > 0
    }

    /// Commit the session (validates all FlowFiles are transferred/removed)
    pub fn commit(&mut self) -> Result<(), SessionError> {
        if self.pending_count > 0 {
            return Err(SessionError::PendingFlowFiles(self.pending_count));
        }
        Ok(())
    }

    /// Rollback the session (returns input FlowFiles to queue)
    pub fn rollback(&mut self) {
        // Put transferred files back in input queue
        for target in self.transferred.drain(..) {
            self.input_queue.push(target.flowfile);
        }
        // Put removed files back in input queue
        self.input_queue.extend(self.removed.drain(..));
    }

    /// Get statistics about this session
    pub fn stats(&self) -> SessionStats {
        SessionStats {
            input_count: self.input_queue.len(),
            transferred_count: self.transferred.len(),
            removed_count: self.removed.len(),
            pending_count: self.pending_count,
        }
    }
}

impl Default for ProcessSession {
    fn default() -> Self {
        Self::new()
    }
}

/// Session statistics
#[derive(Debug, Clone, Default)]
pub struct SessionStats {
    pub input_count: usize,
    pub transferred_count: usize,
    pub removed_count: usize,
    pub pending_count: usize,
}

/// Session errors
#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("Session has {0} pending FlowFiles that were not transferred or removed")]
    PendingFlowFiles(usize),

    #[error("FlowFile not found: {0}")]
    FlowFileNotFound(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_context() {
        let mut ctx = ProcessContext::new("proc-1", "MyProcessor");
        ctx.set_property("batch.size", PropertyValue::Integer(100));
        ctx.set_property("connection.url", PropertyValue::String("localhost:6379".into()));

        assert_eq!(ctx.get_property_int("batch.size"), Some(100));
        assert_eq!(
            ctx.get_property_string("connection.url"),
            Some("localhost:6379")
        );
        assert!(ctx.has_property("batch.size"));
        assert!(!ctx.has_property("nonexistent"));
    }

    #[test]
    fn test_process_session_create_and_transfer() {
        let mut session = ProcessSession::new();

        let mut ff = session.create();
        ff.put_attribute("test", "value");
        session.write_content(&mut ff, Bytes::from("hello"));

        assert!(session.has_pending());

        session.transfer(ff, Relationship::SUCCESS);

        assert!(!session.has_pending());
        assert_eq!(session.get_transferred().len(), 1);
        assert!(session.commit().is_ok());
    }

    #[test]
    fn test_process_session_input_queue() {
        let input_ffs = vec![FlowFile::new(), FlowFile::new(), FlowFile::new()];
        let mut session = ProcessSession::with_input(input_ffs);

        assert!(session.has_input());
        assert_eq!(session.input_count(), 3);

        let ff1 = session.get().unwrap();
        session.transfer(ff1, Relationship::SUCCESS);

        let ff2 = session.get().unwrap();
        session.remove(ff2);

        let ff3 = session.get().unwrap();
        session.transfer(ff3, Relationship::FAILURE);

        assert!(!session.has_input());
        assert!(!session.has_pending());
        assert!(session.commit().is_ok());

        let stats = session.stats();
        assert_eq!(stats.transferred_count, 2);
        assert_eq!(stats.removed_count, 1);
    }

    #[test]
    fn test_process_session_pending_error() {
        let mut session = ProcessSession::new();
        let _ff = session.create(); // Created but never transferred

        assert!(session.has_pending());
        assert!(session.commit().is_err());
    }

    #[test]
    fn test_process_session_rollback() {
        let input_ffs = vec![FlowFile::new()];
        let mut session = ProcessSession::with_input(input_ffs);

        let ff = session.get().unwrap();
        session.transfer(ff, Relationship::SUCCESS);

        assert!(!session.has_input());
        assert_eq!(session.get_transferred().len(), 1);

        session.rollback();

        assert!(session.has_input());
        assert!(session.get_transferred().is_empty());
    }
}
