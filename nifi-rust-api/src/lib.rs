//! NiFi Rust API
//!
//! Core traits and types for building Rust-based NiFi processors.
//! This crate provides the Processor trait, FlowFile abstraction,
//! and ProcessContext/ProcessSession types.
//!
//! # Example
//!
//! ```rust,ignore
//! use nifi_rust_api::prelude::*;
//!
//! pub struct MyProcessor;
//!
//! #[async_trait]
//! impl Processor for MyProcessor {
//!     fn get_type(&self) -> &'static str { "org.example.MyProcessor" }
//!     fn get_description(&self) -> &'static str { "My custom processor" }
//!     fn get_property_descriptors(&self) -> Vec<PropertyDescriptor> { vec![] }
//!     fn get_relationships(&self) -> Vec<Relationship> { vec![Relationship::SUCCESS] }
//!
//!     async fn on_trigger(&mut self, ctx: &ProcessContext, session: &mut ProcessSession)
//!         -> Result<(), ProcessorError>
//!     {
//!         // Process flow files...
//!         Ok(())
//!     }
//! }
//!
//! register_processor!(MyProcessor);
//! ```

mod context;
mod flowfile;
mod processor;

pub use context::{ProcessContext, ProcessSession};
pub use flowfile::FlowFile;
pub use processor::{
    get_registered_processors, make_processor, Processor, ProcessorError, ProcessorFactory,
    PropertyDescriptor, PropertyValue, Relationship,
};

// Re-export async_trait for convenience
pub use async_trait::async_trait;

/// Prelude for common imports
pub mod prelude {
    pub use crate::context::{ProcessContext, ProcessSession};
    pub use crate::flowfile::FlowFile;
    pub use crate::processor::{
        Processor, ProcessorError, ProcessorFactory, PropertyDescriptor, PropertyValue,
        Relationship,
    };
    pub use crate::{async_trait, register_processor};
}

/// Register a processor type for discovery at runtime.
///
/// This macro uses the `inventory` crate to register processor factories
/// at compile time, enabling runtime discovery without reflection.
///
/// # Example
///
/// ```rust,ignore
/// use nifi_rust_api::prelude::*;
///
/// pub struct MyProcessor;
///
/// impl Processor for MyProcessor {
///     // ... implement trait methods
/// }
///
/// register_processor!(MyProcessor);
/// ```
#[macro_export]
macro_rules! register_processor {
    ($processor:ty) => {
        ::inventory::submit! {
            $crate::ProcessorFactory {
                type_name: ::std::stringify!($processor),
                constructor: $crate::make_processor::<$processor>,
            }
        }
    };
}
