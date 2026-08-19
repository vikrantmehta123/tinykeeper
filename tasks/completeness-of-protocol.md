# Task

Our ZNode is currently very simple. Real ZNodes hold a massive Stat struct containing ctime, mtime, version, cversion, zxid, etc., as well as Access Control Lists (ACLs) to handle security permissions.

Furthermore, there are several other OpCodes that we need to support as well.
