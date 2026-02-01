//! Lynx - High-performance async TCP chat server.
//!
//! This crate provides the server implementation for the Lynx chat system.
//! It handles concurrent client connections, room-based messaging, and
//! exports Prometheus metrics.
//!
//! # Quick Start
//!
//! ```no_run
//! use lynx_server::{Config, Server};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = Config::default();
//!     let (server, handle) = Server::bind("127.0.0.1:6006", config).await?;
//!
//!     // run server in background
//!     let server_task = tokio::spawn(server.run());
//!
//!     // ... do other things ...
//!
//!     // trigger graceful shutdown
//!     handle.shutdown();
//!     server_task.await??;
//!     Ok(())
//! }
//! ```
//!
//! # Architecture
//!
//! - One Tokio task per client connection (split into read/write tasks)
//! - [`DashMap`](dashmap::DashMap) for lock-free client registry
//! - Bounded channels with backpressure for slow clients
//! - Token bucket rate limiting per user

pub mod config;
pub mod metrics;
pub mod rate_limiter;
pub mod server;

pub use config::Config;
pub use metrics::HealthState;
pub use server::{Server, ServerHandle};
