//! FlowFile abstraction
//!
//! A FlowFile represents a piece of data flowing through a NiFi processor.
//! It consists of content (the actual data) and attributes (metadata).

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// A FlowFile represents a single piece of data in the NiFi flow.
///
/// FlowFiles are immutable data containers with:
/// - A unique ID
/// - Attributes (key-value metadata)
/// - Content (the actual data bytes)
/// - Size (content length)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowFile {
    /// Unique identifier for this FlowFile
    pub id: Uuid,

    /// Attributes (metadata) associated with this FlowFile
    #[serde(default)]
    pub attributes: HashMap<String, String>,

    /// The content of this FlowFile
    #[serde(skip_serializing, skip_deserializing)]
    content: Option<Bytes>,

    /// Size of the content in bytes
    pub size: u64,

    /// Entry date (when the FlowFile entered the flow)
    pub entry_date: i64,

    /// Lineage start date (when the original FlowFile was created)
    pub lineage_start_date: i64,
}

impl FlowFile {
    /// Create a new empty FlowFile
    pub fn new() -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            id: Uuid::new_v4(),
            attributes: HashMap::new(),
            content: None,
            size: 0,
            entry_date: now,
            lineage_start_date: now,
        }
    }

    /// Create a FlowFile with content
    pub fn with_content(content: Bytes) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        let size = content.len() as u64;
        Self {
            id: Uuid::new_v4(),
            attributes: HashMap::new(),
            content: Some(content),
            size,
            entry_date: now,
            lineage_start_date: now,
        }
    }

    /// Get an attribute value
    pub fn get_attribute(&self, name: &str) -> Option<&str> {
        self.attributes.get(name).map(|s| s.as_str())
    }

    /// Set an attribute value
    pub fn put_attribute(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.attributes.insert(name.into(), value.into());
    }

    /// Remove an attribute
    pub fn remove_attribute(&mut self, name: &str) -> Option<String> {
        self.attributes.remove(name)
    }

    /// Get the content as bytes
    pub fn content(&self) -> Option<&Bytes> {
        self.content.as_ref()
    }

    /// Get the content as a string (if valid UTF-8)
    pub fn content_as_string(&self) -> Option<String> {
        self.content
            .as_ref()
            .and_then(|b| String::from_utf8(b.to_vec()).ok())
    }

    /// Set the content
    pub fn set_content(&mut self, content: Bytes) {
        self.size = content.len() as u64;
        self.content = Some(content);
    }

    /// Take ownership of the content
    pub fn take_content(&mut self) -> Option<Bytes> {
        self.size = 0;
        self.content.take()
    }

    /// Check if this FlowFile has content
    pub fn has_content(&self) -> bool {
        self.content.is_some()
    }

    /// Create a clone with a new ID (for splitting/cloning operations)
    pub fn clone_with_new_id(&self) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            id: Uuid::new_v4(),
            attributes: self.attributes.clone(),
            content: self.content.clone(),
            size: self.size,
            entry_date: now,
            lineage_start_date: self.lineage_start_date,
        }
    }

    /// Get the filename attribute if present
    pub fn filename(&self) -> Option<&str> {
        self.get_attribute("filename")
    }

    /// Set the filename attribute
    pub fn set_filename(&mut self, filename: impl Into<String>) {
        self.put_attribute("filename", filename);
    }

    /// Get the path attribute if present
    pub fn path(&self) -> Option<&str> {
        self.get_attribute("path")
    }

    /// Set the path attribute
    pub fn set_path(&mut self, path: impl Into<String>) {
        self.put_attribute("path", path);
    }

    /// Get the MIME type attribute if present
    pub fn mime_type(&self) -> Option<&str> {
        self.get_attribute("mime.type")
    }

    /// Set the MIME type attribute
    pub fn set_mime_type(&mut self, mime_type: impl Into<String>) {
        self.put_attribute("mime.type", mime_type);
    }
}

impl Default for FlowFile {
    fn default() -> Self {
        Self::new()
    }
}

/// Standard FlowFile attribute names
pub mod attributes {
    /// The filename of the FlowFile
    pub const FILENAME: &str = "filename";

    /// The path of the FlowFile
    pub const PATH: &str = "path";

    /// The absolute path (path + filename)
    pub const ABSOLUTE_PATH: &str = "absolute.path";

    /// MIME type of the content
    pub const MIME_TYPE: &str = "mime.type";

    /// UUID of the FlowFile
    pub const UUID: &str = "uuid";

    /// Priority for processing order
    pub const PRIORITY: &str = "priority";

    /// Entry date timestamp
    pub const ENTRY_DATE: &str = "entryDate";

    /// Lineage start date timestamp
    pub const LINEAGE_START_DATE: &str = "lineageStartDate";

    /// Fragment identifier for split FlowFiles
    pub const FRAGMENT_ID: &str = "fragment.identifier";

    /// Fragment index for split FlowFiles
    pub const FRAGMENT_INDEX: &str = "fragment.index";

    /// Total fragment count for split FlowFiles
    pub const FRAGMENT_COUNT: &str = "fragment.count";

    /// Segment original filename
    pub const SEGMENT_ORIGINAL_FILENAME: &str = "segment.original.filename";
}

/// FlowFile batch for bulk operations
#[derive(Debug, Default)]
pub struct FlowFileBatch {
    pub flowfiles: Vec<FlowFile>,
}

impl FlowFileBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, flowfile: FlowFile) {
        self.flowfiles.push(flowfile);
    }

    pub fn len(&self) -> usize {
        self.flowfiles.len()
    }

    pub fn is_empty(&self) -> bool {
        self.flowfiles.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &FlowFile> {
        self.flowfiles.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut FlowFile> {
        self.flowfiles.iter_mut()
    }
}

impl IntoIterator for FlowFileBatch {
    type Item = FlowFile;
    type IntoIter = std::vec::IntoIter<FlowFile>;

    fn into_iter(self) -> Self::IntoIter {
        self.flowfiles.into_iter()
    }
}

impl FromIterator<FlowFile> for FlowFileBatch {
    fn from_iter<T: IntoIterator<Item = FlowFile>>(iter: T) -> Self {
        Self {
            flowfiles: iter.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flowfile_creation() {
        let ff = FlowFile::new();
        assert!(!ff.has_content());
        assert!(ff.attributes.is_empty());
    }

    #[test]
    fn test_flowfile_with_content() {
        let content = Bytes::from("Hello, NiFi!");
        let ff = FlowFile::with_content(content.clone());

        assert!(ff.has_content());
        assert_eq!(ff.size, 12);
        assert_eq!(ff.content_as_string(), Some("Hello, NiFi!".to_string()));
    }

    #[test]
    fn test_flowfile_attributes() {
        let mut ff = FlowFile::new();

        ff.put_attribute("key1", "value1");
        ff.put_attribute("key2", "value2");

        assert_eq!(ff.get_attribute("key1"), Some("value1"));
        assert_eq!(ff.get_attribute("key2"), Some("value2"));
        assert_eq!(ff.get_attribute("key3"), None);

        ff.remove_attribute("key1");
        assert_eq!(ff.get_attribute("key1"), None);
    }

    #[test]
    fn test_flowfile_filename() {
        let mut ff = FlowFile::new();
        ff.set_filename("test.txt");
        ff.set_path("/data/input/");
        ff.set_mime_type("text/plain");

        assert_eq!(ff.filename(), Some("test.txt"));
        assert_eq!(ff.path(), Some("/data/input/"));
        assert_eq!(ff.mime_type(), Some("text/plain"));
    }

    #[test]
    fn test_flowfile_batch() {
        let mut batch = FlowFileBatch::new();
        batch.add(FlowFile::new());
        batch.add(FlowFile::new());

        assert_eq!(batch.len(), 2);
        assert!(!batch.is_empty());
    }

    #[test]
    fn test_clone_with_new_id() {
        let mut ff = FlowFile::new();
        ff.put_attribute("test", "value");
        ff.set_content(Bytes::from("content"));

        let cloned = ff.clone_with_new_id();

        assert_ne!(ff.id, cloned.id);
        assert_eq!(ff.get_attribute("test"), cloned.get_attribute("test"));
        assert_eq!(ff.size, cloned.size);
    }
}
