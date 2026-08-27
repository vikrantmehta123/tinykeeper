"""
Markers used across the suite. This defines a custom tags we attach to our tests.

`todo(FEATURE)` marks a test that describes correct ZooKeeper behaviour
tinykeeper does not implement yet. It is a *strict* xfail: the test must fail
today, and the moment it starts passing the suite goes red, telling you to
delete the marker.

To see everything still outstanding:

    pytest tests/ -rx

To work on one feature at a time:

    pytest tests/ -k watches
"""

import pytest

# ── The v1 feature checklist ──
#
# Every `todo` points at one of these. When a name disappears from the suite,
# that feature is done.

SESSIONS = "sessions: ids, passwords, expiry, resumption"
WATCHES = "watches"
EPHEMERAL = "ephemeral nodes"
SEQUENTIAL = "sequential nodes"
VERSIONS = "conditional updates (version checks)"
ZXIDS = "zxid assignment (czxid/mzxid/pzxid)"
MULTI = "multi / transactions"
ACL = "ACLs (getACL, setACL)"
AUTH = "auth requests"
SYNC = "sync"
CREATE2 = "create2"
GETCHILDREN = "getChildren (opcode 8)"
PATHS = "path validation"
CLOSE = "graceful session close"
NOT_EMPTY = "refusing to delete a node with children"
PERSISTENCE = "persistence (WAL durability and replay)"


def todo(feature: str):
    """Mark a test as covering a feature that is not implemented yet."""
    return pytest.mark.xfail(reason=f"v1 TODO — {feature}", strict=True)


# ── Environment markers ──

needs_restart = pytest.mark.needs_restart # For durability tests
real_root = pytest.mark.real_root
slow = pytest.mark.slow # For timeout related tests
signoff = pytest.mark.signoff
