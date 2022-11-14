FROM clux/muslrust:nightly-2022-11-13 AS builder
COPY Cargo.* ./
COPY src/ src/
RUN --mount=type=cache,target=/volume/target \
    --mount=type=cache,target=/root/.cargo/registry \
    cargo build --release && \
    mv /volume/target/x86_64-unknown-linux-musl/release/githanos .

FROM gcr.io/distroless/static:nonroot
COPY --from=builder --chown=nonroot:nonroot /volume/githanos /app/
ENTRYPOINT ["/app/githanos"]
