pub fn server_addr() -> String {
    std::env::var("LYNX_SERVER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:6006".to_string())
}
