//! NiFi Rust Runtime
//!
//! gRPC server that hosts Rust-based NiFi processors.
//! This runtime is started by the Java NiFi framework and communicates
//! via gRPC to execute processor lifecycle methods.

mod server;

pub mod proto {
    tonic::include_proto!("nifi.rust");
}

use proto::rust_processor_service_server::RustProcessorServiceServer;
use server::NifiRustServer;
use std::net::SocketAddr;
use tonic::transport::Server;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

/// Default gRPC port
const DEFAULT_PORT: u16 = 50051;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(true)
        .with_thread_ids(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Get port from environment or use default
    let port: u16 = std::env::var("NIFI_RUST_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_PORT);

    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;

    info!("Starting NiFi Rust Runtime on {}", addr);

    // List available processors
    let processors: Vec<_> = nifi_rust_api::get_registered_processors()
        .into_iter()
        .collect();
    if processors.is_empty() {
        info!("No processors registered. Make sure processor crates are linked.");
    } else {
        info!("Registered processors:");
        for factory in &processors {
            let p = factory.create();
            info!("  - {}: {}", p.get_type(), p.get_description());
        }
    }

    // Create and start the gRPC server
    let server = NifiRustServer::new();

    Server::builder()
        .add_service(RustProcessorServiceServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}
