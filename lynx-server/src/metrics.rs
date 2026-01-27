//! prometheus metrics and health endpoints for lynx server.

use axum::{Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

// Custom histogram buckets for sub-millisecond chat latency (in seconds)
// 100us, 200us, 500us, 1ms, 2ms, 5ms, 10ms, 25ms, 50ms, 100ms
const LATENCY_BUCKETS: &[f64] = &[
    0.0001, 0.0002, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1,
];

/// shared state for health endpoints.
pub struct HealthState {
    pub active_connections: Arc<AtomicUsize>,
    pub max_connections: usize,
    pub accepting: Arc<AtomicBool>,
}

#[derive(Clone)]
struct AppState {
    prometheus_handle: PrometheusHandle,
    health_state: Arc<HealthState>,
}

/// initialize prometheus metrics and start HTTP server with health endpoints.
/// exposes: GET /metrics, GET /health, GET /ready
pub async fn init_with_health(
    addr: &str,
    health_state: Arc<HealthState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let socket_addr: SocketAddr = addr.parse()?;

    // Build prometheus recorder without built-in HTTP listener
    let prometheus_handle = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full("lynx_message_processing_duration_seconds".to_string()),
            LATENCY_BUCKETS,
        )?
        .install_recorder()?;

    // Describe and initialize metrics
    describe_metrics();
    initialize_metrics();

    let app_state = AppState {
        prometheus_handle,
        health_state,
    };

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .route("/ready", get(ready_handler))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind(socket_addr).await?;
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });

    Ok(())
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    state.prometheus_handle.render()
}

async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

async fn ready_handler(State(state): State<AppState>) -> impl IntoResponse {
    let accepting = state.health_state.accepting.load(Ordering::Relaxed);
    if !accepting {
        return (StatusCode::SERVICE_UNAVAILABLE, "Server shutting down");
    }

    let current = state
        .health_state
        .active_connections
        .load(Ordering::Relaxed);
    if current >= state.health_state.max_connections {
        return (StatusCode::SERVICE_UNAVAILABLE, "At connection limit");
    }

    (StatusCode::OK, "Ready")
}

fn describe_metrics() {
    // Connection metrics
    describe_gauge!(
        "lynx_connections_active",
        "Current number of active TCP connections"
    );
    describe_counter!(
        "lynx_connections_total",
        "Total TCP connections accepted since server start"
    );

    // Message metrics
    describe_counter!(
        "lynx_messages_processed_total",
        "Total messages processed by type"
    );
    describe_histogram!(
        "lynx_message_processing_duration_seconds",
        "Message processing duration in seconds"
    );

    // Error metrics
    describe_counter!("lynx_errors_total", "Total errors by type");

    // Resource management metrics
    describe_counter!(
        "lynx_connections_rejected_total",
        "Connections rejected due to capacity limit"
    );
    describe_counter!(
        "lynx_messages_dropped_total",
        "Messages dropped due to slow consumers"
    );
    describe_counter!(
        "lynx_clients_slow_disconnected_total",
        "Clients disconnected for being slow consumers"
    );
    describe_counter!(
        "lynx_rate_limited_total",
        "Messages rejected due to rate limiting"
    );
}

fn initialize_metrics() {
    // Initialize with zero values so they appear immediately in /metrics
    gauge!("lynx_connections_active").set(0.0);
    counter!("lynx_connections_total").absolute(0);

    // Message counters for each type
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

    // Error counters for each type
    for err_type in [
        "username_taken",
        "not_registered",
        "recipient_not_found",
        "decode_error",
        "already_authenticated",
    ] {
        counter!("lynx_errors_total", "error_type" => err_type).absolute(0);
    }

    // Resource management counters
    counter!("lynx_connections_rejected_total").absolute(0);
    counter!("lynx_messages_dropped_total").absolute(0);
    counter!("lynx_clients_slow_disconnected_total").absolute(0);
    counter!("lynx_rate_limited_total").absolute(0);
}
