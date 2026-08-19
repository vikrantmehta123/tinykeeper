# Task

If you stop tinyKeeper right now, all data is lost. We must implement a Write-Ahead Log (WAL) that flushes every transaction to disk before executing it, along with periodic background snapshots of the KeeperStorage tree.
