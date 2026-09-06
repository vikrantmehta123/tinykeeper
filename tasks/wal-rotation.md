# WAL Rotation Failure Handling

Status: Open — design exploration.

Explore how tinykeeper should behave when WAL rotation fails, particularly when creating the next segment file fails.

Currently, `WalStore::flush()` writes and syncs the buffered records before attempting rotation. A rotation error can therefore be returned after a transaction is already durable. Treating this as a failed transaction and discarding staged storage could make live state disagree with replay after restart.

Questions to explore:

- How should the WAL API distinguish persistence failures from rotation failures?
- If the transaction is durable but rotation fails, should the server publish staged storage and return success while reporting the rotation problem separately?
- Should subsequent transactions continue using the current segment and retry rotation after flushing? File creation currently happens before replacing the active writer, so a creation failure leaves the old writer in place.
- What reporting, retry policy, or file growth limits are needed if rotation repeatedly fails?
- How should recovery handle failures at other points during rotation?

Validate the chosen behavior with injected rotation failures, including repeated failures and restart/replay. Keep write and sync failures distinct: their persistence outcome may be uncertain.
