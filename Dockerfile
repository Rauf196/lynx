# build stage
FROM rust:latest AS builder
WORKDIR /app
COPY . .
RUN cargo build --release --bin lynx-server

# runtime stage
FROM debian:trixie-slim
COPY --from=builder /app/target/release/lynx-server /usr/local/bin/
EXPOSE 6006 9090
CMD ["lynx-server"]
