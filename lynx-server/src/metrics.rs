//! Prometheus metrics initialization for Lynx server.

use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
use std::net::SocketAddr;

// Custom histogram buckets for sub-millisecond chat latency (in seconds)
// 100us, 200us, 500us, 1ms, 2ms, 5ms, 10ms, 25ms, 50ms, 100ms
const LATENCY_BUCKETS: &[f64] = &[
    0.0001, 0.0002, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1,
];

/// Initialize the Prometheus metrics exporter.
/// Starts HTTP server on the given address for /metrics endpoint.
pub fn init(addr: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let socket_addr: SocketAddr = addr.parse()?;

    PrometheusBuilder::new()
        .with_http_listener(socket_addr)
        .set_buckets_for_metric(
            Matcher::Full("lynx_message_processing_duration_seconds".to_string()),
            LATENCY_BUCKETS,
        )?
        .install()?;

    // Describe metrics (adds HELP text in /metrics output)
    describe_gauge!(
        "lynx_connections_active",
        "Current number of active TCP connections"
    );
    describe_counter!(
        "lynx_connections_total",
        "Total TCP connections accepted since server start"
    );
    describe_counter!(
        "lynx_messages_processed_total",
        "Total messages processed by type"
    );
    describe_counter!("lynx_errors_total", "Total errors by type");
    describe_histogram!(
        "lynx_message_processing_duration_seconds",
        "Message processing duration in seconds"
    );

    // Initialize metrics with zero values so they appear immediately
    gauge!("lynx_connections_active").set(0.0);
    counter!("lynx_connections_total").absolute(0);

    // Initialize message counters for each message type
    for msg_type in [
        "connect",
        "send_room_message",
        "send_private_message",
        "join_room",
        "list_users",
        "disconnect",
    ] {
        counter!("lynx_messages_processed_total", "message_type" => msg_type).absolute(0);
    }

    // Initialize error counters for each error type
    for err_type in [
        "username_taken",
        "not_registered",
        "recipient_not_found",
        "decode_error",
        "already_authenticated",
    ] {
        counter!("lynx_errors_total", "error_type" => err_type).absolute(0);
    }

    Ok(())
}
