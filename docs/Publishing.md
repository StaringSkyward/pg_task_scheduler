# Publishing: Decisions and Release Steps

This records the decisions made when preparing `pg_task_scheduler` for
crates.io, and what each one should look like from outside. The admin crate,
`pg_task_scheduler_admin`, is held back for now (§2). The record ends with the
release steps.

Each entry has a source: **(user)** means the user decided it; **(call)** means
it was a judgement call made during the work, open to change.

## Decisions

### 1. The example app stays off crates.io

- **Source:** user.
- **Decision:** `example-app/` is a separate package with `publish = false`. It
  is excluded from the workspace, and neither published crate contains it. It
  stays in the repository and is kept in step with the core crate. The README
  links to it on GitHub. A future GitHub Pages site for the project will say
  more about it.
- **Why:** It is a demo, not a library.
- **Expected behaviour:** `cargo package --list` for either crate shows no
  `example-app` files, and `cargo publish` refuses `example-app`.

### 2. The admin crate is not published yet

- **Source:** user.
- **Decision:** `pg_task_scheduler_admin` has `publish = false`. It stays in the
  workspace and is maintained with the core crate, and is available from
  GitHub. Its crates.io metadata, `include` list and docs.rs settings stay, so
  publishing it later needs only that line removed.
- **Why:** Whether it is worth publishing waits on feedback from users about
  its value and features. A published version cannot be deleted, only yanked,
  and it would make the crate's API a semver commitment.
- **Expected behaviour:**
  - Both READMEs say the crate is on GitHub but not published, pending user
    feedback, and invite that feedback in GitHub issues.
  - Embedding it means taking both crates from the same git tag. That resolves
    to one copy of the core crate; this was checked with `cargo tree` against a
    local git URL. Taking the core crate from crates.io and the admin crate from
    git builds two copies of the core crate.
  - The standalone server installs with
    `cargo install --locked --git https://github.com/StaringSkyward/pg_task_scheduler --tag v0.1.0 pg_task_scheduler_admin`.
  - `cargo publish -p pg_task_scheduler_admin` refuses to run.

### 3. Packages contain only what a user builds or reads

- **Source:** call.
- **Decision:** Each crate has an `include` list:
  - core: `src`, `migrations`, `README.md` and `LICENSE`;
  - admin, ready for when it is published: `src`, `assets`, `templates`,
    `README.md` and `LICENSE`. The admin crate's `LICENSE` is a symlink to the
    root one.

  Tests, `docs/`, CI config, compose files, the justfiles and
  `rust-toolchain.toml` stay in the repository.
- **Why:** The integration tests need a database, and the rest is tooling for
  this repository.
- **Expected behaviour:** `cargo package --list` shows only those paths, plus
  the files Cargo generates.

### 4. README links are absolute GitHub URLs

- **Source:** call.
- **Decision:** Links from either README to other files in the repository use
  `https://github.com/StaringSkyward/pg_task_scheduler/blob/main/...`. The
  migrations link points at the release tag (`tree/v0.1.0/migrations`).
- **Why:** Each README is also its crate's front page on docs.rs, through
  `#![doc = include_str!("../README.md")]`. docs.rs cannot resolve relative
  links to files outside the crate.
- **Expected behaviour:** The links work on GitHub, crates.io and docs.rs, but
  only once the repository is public. Links into `docs/` show the latest `main`,
  not the published version. The migrations link 404s until the tag is pushed.

### 5. docs.rs builds every feature, with feature badges

- **Source:** call.
- **Decision:** Both crates set `[package.metadata.docs.rs] all-features = true`
  (the admin crate's takes effect once it is published). The core crate has
  `#![cfg_attr(docsrs, feature(doc_cfg))]`.
- **Why:** docs.rs builds only default features otherwise, and everything behind
  `cron` (`jobs`, `CronExpression`, `CreateJob`, ...) would be missing.
- **Expected behaviour:** docs.rs shows the `cron` items, each marked as needing
  that feature.

### 6. Every public item is documented

- **Source:** user.
- **Decision:** Both crates have `#![warn(missing_docs)]`. CI runs clippy with
  `-D warnings`, so an undocumented public item fails a PR.
- **Expected behaviour:** `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
  --all-features` passes for both crates.

### 7. The admin crate's dependencies are not exact-pinned

- **Source:** call.
- **Decision:** The `=x.y.z` requirements (argon2, askama, base64,
  serde_urlencoded, toml, tracing-subscriber) became ordinary caret requirements
  at the same versions.
- **Why:** An application that embeds the admin router resolves one version of
  each crate for its whole dependency tree. This is true whether the admin crate
  comes from git or from crates.io. An exact pin in a library forces that
  version on the application, so resolution fails if any other dependency
  needs a newer patch release.
- **Expected behaviour:** `Cargo.lock` is unchanged, so this repository builds
  the same. `cargo install --locked --git ...` uses the repository's
  lockfile.

### 8. `CancellationToken` is re-exported

- **Source:** call.
- **Decision:** `pg_task_scheduler::CancellationToken` re-exports
  `tokio_util::sync::CancellationToken`.
- **Why:** `Scheduler::run_until_shutdown` takes one, and
  `TaskContext::cancellation` is one, so every user needs the type.
- **Expected behaviour:** A worker needs no direct `tokio-util` dependency.

### 9. `runtime::Registry` and `runtime::Handler` are private

- **Source:** call.
- **Decision:** They are no longer exported.
- **Why:** No public API accepts or returns them. Handlers are registered through
  `SchedulerBuilder::register` and `register_task`.
- **Expected behaviour:** No change for any user of the builder.

### 10. Minimum Rust version is 1.88

- **Source:** call.
- **Decision:** `rust-version = "1.88"`, set once in `[workspace.package]`.
- **Why:** The crates use let chains, which became stable in 1.88. The
  dependencies need at most 1.86 (Diesel). The workspace was checked with 1.88,
  and failed to build with 1.87.
- **Expected behaviour:** Cargo reports a clear error on an older toolchain.
  CI builds only on nightly, so a newer language feature could raise the real
  minimum without anyone noticing.

### 11. First release is plain `0.1.0`

- **Source:** call.
- **Decision:** Plain `0.1.0`, with no `-alpha` suffix. The README says the
  crate is in early alpha.
- **Why:** Under Cargo's semver rules, any 0.x release may break the API, and
  `cargo add` skips pre-release versions by default.

### 12. The README describes the first release, not its history

- **Source:** call.
- **Decision:** These notes came out of the READMEs, because no published
  version has ever lacked `0003`:
  - "Upgrading to `0003`";
  - behaviour of workers from before `0003`;
  - `queue_task_replays`;
  - the core crate's former `axum` feature.
- **Expected behaviour:** The upgrade notes stay in git history and in
  [Admin UI decisions](AdminUI.md).

## Release steps

1. Check that the core crate packages and builds from its packaged sources:
   `cargo publish --dry-run -p pg_task_scheduler`.
2. Publish it: `cargo publish -p pg_task_scheduler`.
3. Tag the release and push the tag: `git tag v0.1.0 && git push origin v0.1.0`.
   The README's migrations link and both READMEs' git instructions for the
   admin crate point at it.
4. For the next release, bump:
   - both crates' `version`;
   - the admin crate's `pg_task_scheduler` dependency version;
   - every `v0.1.0` in `README.md` and `admin/README.md`.

To publish the admin crate later:
1. Remove `publish = false` from `admin/Cargo.toml`.
2. Switch both READMEs' git instructions to crates.io ones.
3. Publish it after the core crate.
