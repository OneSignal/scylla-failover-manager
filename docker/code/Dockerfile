FROM europe-west4-docker.pkg.dev/core-infra-onesignal/development/rust-ubuntu:1.83.0 AS chef
WORKDIR /app

FROM chef AS planner
# Cache invalidation is OK in this step because the recipe.json file
# generated in the next step is deterministic.
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG CIRCLE_SHA1
ENV CIRCLE_SHA1=${CIRCLE_SHA1}

COPY --from=planner /app/recipe.json recipe.json

RUN cargo chef cook --release --recipe-path recipe.json

COPY Cargo.lock .
COPY Cargo.toml .
COPY .cargo/ .cargo/
COPY src/ src/

RUN cargo build --release --target-dir target

FROM europe-west4-docker.pkg.dev/core-infra-onesignal/development/rust-runtime:bookworm-20231030-slim-r1
COPY --from=builder /app/target/release/scylla-failover-manager /usr/bin/
COPY --from=builder /app/target/release/force-host-error /usr/bin/
