default:
    just --list build --unsorted

# example-app is outside the workspace, so each recipe runs it separately; --locked as in CI

# build the workspace in debug mode, all features
build:
    cargo build --workspace --all-features
    cargo build --locked --manifest-path example-app/Cargo.toml

# build the workspace in release mode, default features
build-release:
    cargo build --workspace --release
    cargo build --locked --manifest-path example-app/Cargo.toml --release

# cargo fmt code
fmt:
    cargo +nightly fmt --all
    cargo +nightly fmt --manifest-path example-app/Cargo.toml

# fail if any code is not formatted
fmt-check:
    cargo +nightly fmt --all --check
    cargo +nightly fmt --manifest-path example-app/Cargo.toml --check

# cargo clippy lint, warnings are errors
lint:
    cargo clippy --workspace --all-targets --all-features -- -D warnings
    cargo clippy --locked --manifest-path example-app/Cargo.toml --all-targets -- -D warnings

# run the tests; nextest does not run doctests, so cargo test --doc does
test:
    cargo nextest run --workspace --all-features
    cargo test --workspace --all-features --doc
    cargo test --locked --manifest-path example-app/Cargo.toml

# clippy, fmt check, build, test
check: lint fmt-check build test
