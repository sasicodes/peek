FROM rust:1.86 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release --bin peek-relay

FROM debian:bookworm-slim
LABEL org.opencontainers.image.source="https://github.com/sasicodes/peek"
LABEL org.opencontainers.image.description="A localhost-to-public proxy for local development"
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/peek-relay /usr/local/bin/
ENV RELAY_DOMAIN=localhost
CMD ["peek-relay"]
