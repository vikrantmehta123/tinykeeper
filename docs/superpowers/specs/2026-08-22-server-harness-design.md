# Server Harness Design

Date: 2026-08-22
Branch: feature/server

## Purpose

`tinyKeeper` currently has all of its logic inline inside `main.rs`'s TCP
accept loop: binding the socket, reading frames, parsing the protocol,
mutating storage, and appending to the WAL are all interleaved in one
function. This spec introduces a thin harness around that logic —
splitting responsibilities into named components — without implementing
the eventual Raft/consensus behavior. It mirrors (loosely) the shape of
ClickHouse Keeper's own `main()`, adapted and simplified for tinyKeeper's
current milestone.

This is scaffolding: several structs are given the right shape and public
API for where they'll grow into (request queues, session routing), but
their bodies may be stubs (`todo!()` or minimal pass-through) until later
tasks fill them in.

## Components

- **`config.rs` — `Config`**
  Plain struct: `listen_host: String`, `tcp_port: u16`, `storage_path:
  PathBuf`. `Config::load(path: &str) -> Config` reads a TOML file if
  present; falls back to hardcoded defaults if missing. No hot-reload.

- **`server_uuid.rs` — `ServerUUID`**
  Wraps a `Uuid`. `ServerUUID::load_or_create(storage_path: &Path) ->
  ServerUUID` reads a `uuid` file under the storage directory if it
  exists; otherwise generates a new UUID and writes it there.

- **`context.rs` — `KeeperContext`**
  ```rust
  pub struct KeeperContext {
      pub config: Config,
      pub uuid: ServerUUID,
      pub dispatcher: OnceLock<Arc<KeeperDispatcher>>,
  }
  ```
  The one struct threaded through the app for anything shared. The
  dispatcher field is filled in after `KeeperDispatcher` is constructed,
  since the dispatcher itself may need a reference to the context later.

- **Worker thread pool**
  An existing crate (e.g. `threadpool`), not a hand-rolled one. `main()`
  builds `Arc<ThreadPool>` sized from `config` and passes it into
  `KeeperDispatcher::new(..., worker_pool)`.

- **`dispatcher.rs` — `KeeperDispatcher`**
  Owns a `KeeperServer` and the `Arc<ThreadPool>`. Represents the future
  request-queueing and session-routing layer that sits between the
  network and the state machine (and eventually Raft). For this pass:
  - Holds `KeeperServer` and the thread pool.
  - Exposes `dispatch(&self, raw_request: ...) -> raw_response` (or the
    typed equivalent), which submits the work to the thread pool; the
    pooled closure calls into `KeeperServer` to actually apply the
    request and produce a response.
  - Session routing is structurally present (e.g. a field/type for it)
    but not implemented — no real session tracking yet.
  - `shutdown(&self)` — stub, called from `main` on ctrl-c; should stop
    the thread pool.

- **`keeper_server.rs` — `KeeperServer`**
  Owns `KeeperStorage` and `WalManager`. This is where today's
  `main.rs` per-opcode logic (create/get/set/delete/ping, WAL replay on
  startup, WAL append before mutating state) moves to, largely
  unchanged — just relocated out of the accept loop and behind a
  cleaner API (e.g. one method per opcode, or a single `apply(request)`
  matching on opcode internally).

- **`main.rs`**
  Becomes the wiring + accept loop only: no protocol/storage/WAL logic
  lives here anymore.

## `main()` Wiring Order

1. Load `Config`.
2. Create the storage directory (`std::fs::create_dir_all`).
3. `ServerUUID::load_or_create(&config.storage_path)`.
4. Build `Arc<KeeperContext>` with config, uuid, and an empty
   `OnceLock` for the dispatcher.
5. Build the worker `Arc<ThreadPool>` from `config`.
6. Build `KeeperServer` (opens/replays the WAL, initializes
   `KeeperStorage`).
7. Build `Arc<KeeperDispatcher>` wrapping `KeeperServer` and the thread
   pool; call `context.dispatcher.set(...)`.
8. Bind the single `TcpListener` on `config.listen_host` /
   `config.tcp_port`. (Only one port for this pass — no TLS,
   Prometheus, or HTTP control ports.)
9. Loop: accept a connection, spawn a task that reads length-prefixed
   frames and calls `dispatcher.dispatch(...)`, writing the response
   back.
10. `tokio::signal::ctrl_c().await`. On trigger: stop accepting new
    connections and call `dispatcher.shutdown()` (which stops the
    thread pool).

No async metrics collector in this pass — deferred to a later version.

Config reloading and a cgroups observer (present in the ClickHouse
reference `main()`) are dropped entirely for tinyKeeper, not stubbed.
Leave a short comment in `main.rs` noting they were considered and
intentionally omitted, in case a future version wants file-watching
config reload.

## Error Handling

`main` keeps returning `Result<(), Box<dyn std::error::Error>>`.
Startup failures (bad config, can't bind the port, WAL open/replay
failure) propagate up and exit the process, same as today. Per-connection
errors stay contained to that connection's spawned task and don't affect
other connections or the process — same pattern as the current code.

## Testing

Since `KeeperDispatcher` and `KeeperServer`'s deeper logic (session
routing, real request queueing beyond the thread pool) are stubs in this
pass, there isn't much to unit test yet beyond:
- `Config::load` falling back to defaults when the file is absent.
- `ServerUUID::load_or_create` round-tripping (creates once, reloads
  the same UUID on a second call).
- `KeeperDispatcher::dispatch` actually reaching `KeeperServer` and
  getting a correct response back for at least one opcode, to confirm
  the thread-pool hand-off works end to end.

## Explicit Non-Goals (this pass)

- Raft/consensus.
- Session routing/tracking (structurally present, not implemented).
- Async metrics.
- Multiple listener types (TLS, Prometheus, HTTP control).
- Config hot-reload, cgroups observation.
- Migrating protocol.rs's actual per-opcode logic into `KeeperServer`
  is in scope for this pass (the move), but any *behavioral* changes to
  that logic are not.
