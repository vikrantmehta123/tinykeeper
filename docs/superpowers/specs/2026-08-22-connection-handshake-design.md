# Connection Handshake Design

Date: 2026-08-22
Branch: feature/server (or a follow-up branch)

## Purpose

`ConnectionHandler` currently jumps straight into the request loop —
every connection is treated as already-authenticated with no concept
of a ZooKeeper session. Real ZooKeeper clients (and ClickHouse Keeper)
require a handshake before any request: either a raw four-letter
command (`ruok`, `stat`, etc. — no framing at all) or a `ConnectRequest`
that establishes a session and gets a `ConnectResponse` back.

This spec adds that handshake phase. It intentionally does **not**
build:
- A real `SessionTracker` with expiry (`tasks/client-sessions-and-expiry.md`).
- Any four-letter command's actual behavior (out of scope for v1).
- Session *resumption* using a client-supplied session id/password —
  every connect gets a fresh session id.
- The push-based response architecture (`mpsc` channel +
  `tokio::select!`) needed for unprompted server-to-client messages
  (watch events) — nothing needs to push yet, so building that channel
  now would be speculative. The existing simple request-in/response-out
  loop in `ConnectionHandler` stays as-is after the handshake completes.

## Components

- **`protocol.rs`** — new structs, matching real ZooKeeper's wire
  format (the only messages with no `xid`/`opcode` header):
  ```rust
  pub struct ConnectRequest {
      pub protocol_version: i32,
      pub last_zxid_seen: i64,
      pub timeout_ms: i32,
      pub session_id: i64,
      pub password: Vec<u8>,
  }
  impl ConnectRequest {
      pub fn from_bytes(buf: &mut &[u8]) -> Option<Self>;
  }

  pub struct ConnectResponse {
      pub protocol_version: i32,
      pub timeout_ms: i32,
      pub session_id: i64,
      pub password: Vec<u8>,
  }
  impl ConnectResponse {
      pub fn to_bytes(&self) -> Vec<u8>;
  }
  ```

- **`dispatcher.rs`** — `KeeperDispatcher` gains:
  ```rust
  next_session_id: std::sync::atomic::AtomicI64,
  ```
  and:
  ```rust
  pub fn handshake(&self, request: ConnectRequest) -> ConnectResponse {
      // TODO: real session resumption (matching request.session_id /
      // request.password against a SessionTracker) is out of scope —
      // every connect gets a fresh session id for now.
      let session_id = self.next_session_id.fetch_add(1, Ordering::SeqCst);
      ConnectResponse {
          protocol_version: request.protocol_version,
          timeout_ms: request.timeout_ms,
          session_id,
          password: Vec::new(), // TODO: no real auth yet
      }
  }
  ```
  This is consistent with the earlier decision that `KeeperDispatcher`
  owns state shared across all connections (it already will, once
  session tracking exists) — this method is the exact seam a real
  `SessionTracker` replaces later, without `ConnectionHandler` needing
  to change.

- **`connection_handler.rs`** — `ConnectionHandler::run()` gains a phase
  before the existing loop:
  1. Read the first 4 bytes (wrapped in the existing idle timeout,
     same as every other read in this file).
  2. If those 4 bytes are all printable ASCII letters, treat it as a
     four-letter command: log it, then
     `todo!("four-letter commands are out of scope for v1")`. This
     panics only the current connection's spawned task — tokio
     isolates task panics, so the server keeps running (same
     panic-containment property the rest of the codebase already
     relies on).
  3. Otherwise, treat the 4 bytes as the length prefix of a
     `ConnectRequest`: read that many bytes (same size sanity check as
     the existing `0..=1_048_575` bound), parse via
     `ConnectRequest::from_bytes`, call
     `self.dispatcher.handshake(request)`, store the returned
     `session_id` on `self` (new field), encode the `ConnectResponse`
     via `to_bytes`, length-prefix it, write it back.
  4. Fall through into the existing request loop, unchanged.

  `ConnectionHandler` gains one new field: `session_id: Option<i64>`
  (set after a successful handshake; not yet used by anything else in
  this pass — it's there for the next task, session tracking, to build
  on).

## Data Flow

```
client                     ConnectionHandler                KeeperDispatcher
  |--- 4 bytes ------------------->|
  |                                | ASCII letters? -> todo!() (stub)
  |                                | else: read ConnectRequest body
  |                                |------- handshake(request) ------->|
  |                                |<------ ConnectResponse -----------|
  |<-- ConnectResponse ------------|
  |                                |
  |  (existing request loop, unchanged, now runs after handshake)
```

## Error Handling

- Read failures or idle timeouts during the handshake end the
  connection exactly like they do in the existing loop — no special
  casing.
- The four-letter-command branch's `todo!()` is a deliberate, contained
  panic (see above) — not a bug, flagged with a comment.
- Malformed `ConnectRequest` bytes are not specially validated in this
  pass, matching the rest of the protocol's current posture (tracked
  separately as a known gap in `tasks/bugs/20-08-2026.md`, item 4).

## Testing

- Unit test in `protocol.rs`: `ConnectRequest`/`ConnectResponse` byte
  round-trip (encode → decode → same fields).
- Unit test in `dispatcher.rs`: `handshake()` returns unique,
  increasing session ids across repeated calls.
- Manual verification: a script sending a real `ConnectRequest`,
  checking a sane `ConnectResponse` comes back, then sending a normal
  ping request on the *same* connection afterward to confirm the
  handshake phase doesn't break the existing request loop.

## Explicit Non-Goals (this pass)

- Real `SessionTracker` / session expiry.
- Four-letter command behavior (any of them).
- Session resumption (client-supplied session id/password validated
  against existing state).
- Push-based response architecture (`mpsc` + `tokio::select!`) for
  unprompted server-to-client messages.
- Fixing pre-existing malformed-packet panic risk (separate, already
  tracked).
