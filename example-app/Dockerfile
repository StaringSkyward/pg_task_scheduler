FROM rust:1.96-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock README.md ./
COPY src src
COPY migrations migrations
COPY admin admin
COPY example-app example-app
RUN cargo build --release --locked --manifest-path example-app/Cargo.toml

FROM debian:bookworm-slim
COPY --from=build /src/example-app/target/release/example-app /usr/local/bin/example-app
ENTRYPOINT ["example-app"]
