"""
Durability: what survives the process dying.

Every write goes to the write-ahead log before the server answers the client.
The promise being made is precise: *if the client got an OK, the change is on
disk.* On startup the server replays the log and rebuilds the tree.

Two shapes of restart are tested here, and the difference between them is the
whole point:

  * `keeper.restart()` sends SIGTERM. The server gets a chance to tidy up.
  * `keeper.crash_and_restart()` sends SIGKILL. It gets nothing — no flush,
    no close, no final fsync.

A server that only passes the first one has not implemented durability; it has
implemented saving on exit, which loses everything on the failure that
actually matters. Every test here uses the SIGKILL path unless it is
specifically about clean shutdown.

These tests need to control the process, so they are skipped under --zk-host.
"""

import pytest

from helpers import wait_until
from markers import EPHEMERAL, PERSISTENCE, SEQUENTIAL, ZXIDS, needs_restart, todo

pytestmark = needs_restart


class TestAcknowledgedWritesSurvive:
    def test_created_nodes_survive_a_crash(self, keeper):
        client = keeper.client()
        client.create("/persist_me", b"important")
        client.create("/also_this", b"also_important")

        keeper.crash_and_restart()

        client = keeper.client()
        assert client.get("/persist_me")[0] == b"important"
        assert client.get("/also_this")[0] == b"also_important"

    def test_updates_survive_a_crash(self, keeper):
        client = keeper.client()
        client.create("/mutable", b"v1")
        client.set("/mutable", b"v2")
        client.set("/mutable", b"v3")

        keeper.crash_and_restart()

        client = keeper.client()
        assert client.get("/mutable")[0] == b"v3"

    def test_deletes_survive_a_crash(self, keeper):
        """A delete is a write like any other. Replaying a log that
        resurrects deleted nodes is worse than losing them."""
        client = keeper.client()
        client.create("/temp", b"will_be_deleted")
        client.delete("/temp")

        keeper.crash_and_restart()

        client = keeper.client()
        assert client.exists("/temp") is None

    def test_a_nested_tree_survives_a_crash(self, keeper):
        client = keeper.client()
        client.create("/level1", b"")
        client.create("/level1/level2", b"")
        client.create("/level1/level2/level3", b"deep")

        keeper.crash_and_restart()

        client = keeper.client()
        assert client.get("/level1/level2/level3")[0] == b"deep"
        assert client.get_children("/level1", include_data=True)[0] == ["level2"]
        assert client.exists("/level1").numChildren == 1

    def test_empty_and_binary_data_survive_a_crash(self, keeper):
        payload = bytes(range(256))
        client = keeper.client()
        client.create("/empty_node", b"")
        client.create("/binary_node", payload)

        keeper.crash_and_restart()

        client = keeper.client()
        assert client.get("/empty_node")[0] == b""
        assert client.get("/binary_node")[0] == payload

    def test_a_large_write_survives_a_crash(self, keeper):
        payload = b"y" * 100_000
        client = keeper.client()
        client.create("/large_persist", payload)

        keeper.crash_and_restart()

        client = keeper.client()
        assert client.get("/large_persist")[0] == payload

    def test_a_long_history_replays_correctly(self, keeper):
        """Two hundred writes, then a crash. Replay has to end at exactly
        the same tree — not an approximation of it."""
        client = keeper.client()
        client.create("/history", b"")
        for index in range(100):
            client.create(f"/history/n{index:03d}", str(index).encode())
        for index in range(0, 100, 2):
            client.set(f"/history/n{index:03d}", b"updated")
        for index in range(1, 100, 4):
            client.delete(f"/history/n{index:03d}")

        keeper.crash_and_restart()

        client = keeper.client()
        survivors = set(client.get_children("/history", include_data=True)[0])
        expected = {
            f"n{index:03d}" for index in range(100) if index % 4 != 1
        }
        assert survivors == expected
        assert client.get("/history/n000")[0] == b"updated"
        assert client.get("/history/n002")[0] == b"updated"


class TestMetadataSurvives:
    """Replay has to rebuild the stat, not just the data. A client that
    reconnects and finds every version back at zero will happily overwrite
    changes it should have been told about."""

    def test_versions_survive_a_crash(self, keeper):
        client = keeper.client()
        client.create("/meta", b"v0")
        client.set("/meta", b"v1")
        client.set("/meta", b"v2")
        client.create("/meta_parent", b"")
        client.create("/meta_parent/a", b"")
        client.create("/meta_parent/b", b"")
        client.delete("/meta_parent/a")

        before_data = client.exists("/meta")
        before_parent = client.exists("/meta_parent")

        keeper.crash_and_restart()

        client = keeper.client()
        after_data = client.exists("/meta")
        after_parent = client.exists("/meta_parent")

        assert after_data.version == before_data.version == 2
        assert after_parent.cversion == before_parent.cversion == 3
        assert after_parent.numChildren == 1

    def test_timestamps_survive_a_crash(self, keeper):
        client = keeper.client()
        client.create("/timed_persist", b"v0")
        client.set("/timed_persist", b"v1")
        before = client.exists("/timed_persist")

        keeper.crash_and_restart()

        client = keeper.client()
        after = client.exists("/timed_persist")
        assert after.ctime == before.ctime
        assert after.mtime == before.mtime

    @todo(ZXIDS)
    def test_zxids_survive_a_crash_and_keep_climbing(self, keeper):
        """The transaction counter is part of the durable state. If it
        restarted at zero, a node created after the restart would look older
        than one created before it."""
        client = keeper.client()
        client.create("/zxid_persist", b"")
        before = client.exists("/zxid_persist")

        keeper.crash_and_restart()

        client = keeper.client()
        after = client.exists("/zxid_persist")
        assert after.czxid == before.czxid

        client.create("/zxid_after", b"")
        assert client.exists("/zxid_after").czxid > before.czxid

    @todo(SEQUENTIAL)
    def test_the_sequential_counter_survives_a_crash(self, keeper):
        client = keeper.client()
        client.create("/seq_crash", b"")
        before = int(client.create("/seq_crash/n-", b"", sequence=True).rsplit("-", 1)[1])

        keeper.crash_and_restart()

        client = keeper.client()
        after = int(client.create("/seq_crash/n-", b"", sequence=True).rsplit("-", 1)[1])
        assert after > before


class TestWhatMustNotSurvive:
    @todo(EPHEMERAL)
    def test_ephemeral_nodes_do_not_come_back(self, keeper):
        """Sessions do not survive the process. Neither should the nodes
        that belonged to them — an ephemeral node that reappears after a
        restart is a lock with no owner, held forever."""
        client = keeper.client()
        client.create("/durable", b"stays")
        client.create("/fleeting", b"goes", ephemeral=True)

        keeper.crash_and_restart()

        client = keeper.client()
        assert client.exists("/durable") is not None
        wait_until(
            lambda: client.exists("/fleeting") is None,
            timeout=30,
            interval=0.5,
            message="an ephemeral node survived the restart",
        )


class TestRepeatedRestarts:
    def test_a_graceful_restart_keeps_everything(self, keeper):
        client = keeper.client()
        client.create("/graceful", b"data")

        keeper.restart()

        client = keeper.client()
        assert client.get("/graceful")[0] == b"data"

    def test_state_accumulates_across_several_restarts(self, keeper):
        """Each restart replays a log that already contains a replay's worth
        of history. Getting this wrong tends to show up as duplicated or
        doubled state on the third pass, not the first."""
        for round_number in range(3):
            client = keeper.client()
            client.create(f"/round_{round_number}", str(round_number).encode())
            keeper.crash_and_restart()

        client = keeper.client()
        children = set(client.get_children("/", include_data=True)[0])
        for round_number in range(3):
            assert f"round_{round_number}" in children
            assert client.get(f"/round_{round_number}")[0] == str(round_number).encode()

    def test_a_restart_with_nothing_written_is_fine(self, keeper):
        """An empty log must replay to an empty tree, not to an error."""
        keeper.crash_and_restart()

        client = keeper.client()
        assert client.exists("/") is not None
        client.create("/after_empty_restart", b"ok")
        assert client.get("/after_empty_restart")[0] == b"ok"
