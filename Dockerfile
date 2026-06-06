FROM rust:1.95-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --locked --release -p peek-relay \
    && strip target/release/peek-relay

FROM debian:bookworm-slim
LABEL org.opencontainers.image.source="https://github.com/sasicodes/peek"
LABEL org.opencontainers.image.description="A localhost-to-public proxy for local development"
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/peek-relay /usr/local/bin/peek-relay
ENV PEEK_DOMAIN=localhost
EXPOSE 8080
USER 10001:10001
CMD ["peek-relay"]
