FROM node:24-bookworm-slim AS web
WORKDIR /app/web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM rust:1.95-bookworm AS rust
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
RUN cargo build --release -p edgerunner

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=rust /app/target/release/edgerunner /usr/local/bin/edgerunner
COPY --from=web /app/web/dist web/dist
EXPOSE 8080
ENV EDGERUNNER_CONTROL_TOKEN=replace-me
ENTRYPOINT ["edgerunner"]
CMD ["serve", "--bind", "0.0.0.0:8080"]

