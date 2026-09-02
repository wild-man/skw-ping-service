FROM rust:bookworm AS builder

RUN apt-get update \
 && apt-get install -y --no-install-recommends cmake \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /build/target/release/ping-stream-service /app/ping-stream-service
COPY --from=builder /build/services/ping/dev.json /app/dev.json

ENV ENV=dev
ENV RUST_LOG=info

CMD ["/app/ping-stream-service"]
