# syntax=docker/dockerfile:1
# The caller image: `ceilidh serve` with the web client baked in. Nothing in
# this image runs a model; band runners live on the operator's own machines.

FROM node:22-bookworm-slim AS web
WORKDIR /src/web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release -p ceilidh-cli

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/ceilidh /usr/local/bin/ceilidh
COPY --from=web /src/web/dist /app/web
ENV CEILIDH_DB=/data/ceilidh.db
EXPOSE 10000
CMD ["sh", "-c", "exec ceilidh serve --bind 0.0.0.0:${PORT:-10000} --db ${CEILIDH_DB:-/data/ceilidh.db} --web-dir /app/web"]
