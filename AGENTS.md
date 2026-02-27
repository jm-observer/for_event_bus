# Repository Guidelines

## Project Structure & Module Organization
This repository is a Rust workspace with two crates:
- `for-event-bus/`: main runtime crate (`src/bus`, `src/worker`, `src/lib.rs`).
- `for-event-bus-derive/`: proc-macro crate (`src/lib.rs`) providing `#[derive(Event|Merge|Worker)]`.

Examples live in each crate's `examples/` directory (for manual behavior checks). Build artifacts are under `target/` and should not be committed.

## Build, Test, and Development Commands
Run commands from the workspace root:
- `cargo check --workspace`: fast compile check for both crates.
- `cargo build --workspace`: full workspace build.
- `cargo test --workspace --lib`: run library unit tests/doctests only.
- `cargo test -p for_event_bus --examples`: compile and run event-bus examples.
- `cargo run -p for_event_bus --example normal`: run one async demo.
- `cargo fmt --all`: format code with rustfmt.
- `cargo clippy --workspace --all-targets -D warnings`: lint strictly before PR.

Note: `cargo test --workspace` currently attempts derive examples that may fail due to `custom-utils` API drift; prefer the scoped commands above unless fixing those examples.

## Coding Style & Naming Conventions
Use Rust 2021 idioms and default rustfmt output (4-space indentation, trailing commas where rustfmt adds them). Follow existing naming:
- Modules/files: `snake_case` (`sub_bus.rs`, `identity/mod.rs`).
- Types/traits/enums: `UpperCamelCase` (`IdentityOfSimple`, `BusError`).
- Functions/variables: `snake_case`.

Keep proc-macro error messages actionable; avoid adding `unsafe` unless justified and documented.

## Testing Guidelines
Prefer unit tests near implementation using `#[cfg(test)] mod tests` and async tests with `#[tokio::test]` where needed. Name tests by behavior (e.g., `dispatches_merge_event_to_subscriber`). Use examples for integration-like flows and keep them runnable.

## Commit & Pull Request Guidelines
Recent history favors short, imperative commit subjects (often Chinese), e.g., `修复...`, `优化...`. Keep subject lines concise and specific.

PRs should include:
- what changed and why,
- affected crate(s) (`for-event-bus` / `for-event-bus-derive`),
- commands run locally (check/test/fmt/clippy),
- linked issue(s) and example output/log snippets when behavior changes.
