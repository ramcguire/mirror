# ---------------------------------------------------------------------------
# Stage: chef — the shared base every later stage builds on. Installing
# cargo-chef here, once, means it's cached identically across the planner
# and builder stages below rather than reinstalled in each.
# ---------------------------------------------------------------------------
FROM rust:1-slim-bookworm AS chef
WORKDIR /build
RUN cargo install cargo-chef --locked
# ca-certificates: needed at *build* time too — cargo/crates.io access and,
# transitively, anything the build script layer might fetch over HTTPS.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Stage: planner — computes the dependency-only recipe. Takes the full
# source tree as input, but its only output that later stages consume is
# recipe.json, so source-only edits change this stage's result without
# invalidating anything downstream of the recipe file itself.
# ---------------------------------------------------------------------------
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ---------------------------------------------------------------------------
# Stage: builder — the expensive one. `cargo chef cook` builds every
# dependency named in recipe.json and nothing else (no mirrorvol source is
# present yet), so this RUN layer's cache key is purely a function of
# Cargo.toml/Cargo.lock across the workspace. Only after that layer is copied
# in do we add real source and build the two binaries this Dockerfile ships.
# ---------------------------------------------------------------------------
FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
# mirrorvol-csi/build.rs needs protoc — protoc-bin-vendored supplies its own
# vendored binary (no apt install needed here), see that crate's build.rs.
RUN cargo build --release \
    --bin mirrorvol-controller --bin mirrorvol-agent --bin mirrorvol-csi \
    && mkdir -p /out \
    && cp target/release/mirrorvol-controller target/release/mirrorvol-agent target/release/mirrorvol-csi /out/

# ---------------------------------------------------------------------------
# Shared runtime base: minimal, non-root, just the CA bundle needed for
# outbound HTTPS (the in-cluster kube client normally trusts the API
# server's own mounted CA instead, but this covers any other HTTPS caller).
# Debian-slim (not distroless/musl/scratch) specifically to stay glibc-
# compatible with the `rust:1-slim-bookworm` build stage above without extra
# musl-target plumbing — a reasonable simplicity/size trade-off for a
# low-traffic control-plane image; revisit if image size ever matters more
# than build simplicity.
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime-base
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 65532 mirrorvol \
    && useradd --uid 65532 --gid mirrorvol --no-create-home --shell /usr/sbin/nologin mirrorvol
USER mirrorvol:mirrorvol

# ---------------------------------------------------------------------------
# Stage: controller — the cluster-wide MirroredVolume controller
# (mirrorvol-controller/src/main.rs). One replica, per DESIGN.md.
# ---------------------------------------------------------------------------
FROM runtime-base AS controller
COPY --from=builder /out/mirrorvol-controller /usr/local/bin/mirrorvol-controller
ENTRYPOINT ["/usr/local/bin/mirrorvol-controller"]

# ---------------------------------------------------------------------------
# Stage: agent — the per-node backend agent (mirrorvol-agent/src/main.rs),
# run as the DaemonSet sidecar per ARCHITECTURE-PHASE0.md's deployment
# topology. Requires NODE_NAME (downward API) at runtime — see main.rs.
# ---------------------------------------------------------------------------
FROM runtime-base AS agent
COPY --from=builder /out/mirrorvol-agent /usr/local/bin/mirrorvol-agent
ENTRYPOINT ["/usr/local/bin/mirrorvol-agent"]

# ---------------------------------------------------------------------------
# Stage: csi — the CSI overlay driver (mirrorvol-csi/src/main.rs), deployed
# as an extra container in the same DaemonSet pod as the agent/syncthing —
# see deploy/syncthing/daemonset.yaml and ARCHITECTURE-CSI.md. Deliberately
# NOT based on runtime-base/its non-root `mirrorvol` user: this binary binds
# a Unix socket under a hostPath directory and dials another one, both
# created by root-owned processes — the daemonset.yaml container overrides
# this image's default user back to root anyway (same reasoning as the
# agent container's own securityContext there), so there's no actual
# non-root benefit to inheriting runtime-base's USER here, just an extra
# thing that has to agree with the deploy manifest instead of being
# self-evidently consistent with it.
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS csi
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/mirrorvol-csi /usr/local/bin/mirrorvol-csi
ENTRYPOINT ["/usr/local/bin/mirrorvol-csi"]
