"""
Conditional writes.

`set` and `delete` both take a version. The server applies the change only if
the node's current version matches; otherwise it returns BadVersion (-103) and
changes nothing. Passing -1 means "whatever the version is".

This is ZooKeeper's entire concurrency-control story. There are no locks in
the protocol: a client reads a node, computes a new value, and writes it back
with the version it read. If someone else got there first the write is
rejected and the client retries. Without this, two clients doing
read-modify-write silently lose one of the updates.
"""

import pytest
from kazoo.exceptions import BadVersionError, NoNodeError




class TestConditionalSet:

    def test_set_with_the_matching_version(self, zk):
        zk.create("/cond_set", b"v0")
        zk.set("/cond_set", b"v1", version=0)
        data, stat = zk.get("/cond_set")
        assert data == b"v1"
        assert stat.version == 1


    def test_set_with_a_stale_version_is_rejected(self, zk):
        zk.create("/cond_stale", b"v0")
        zk.set("/cond_stale", b"v1")  # version is now 1

        with pytest.raises(BadVersionError):
            zk.set("/cond_stale", b"v2", version=0)

        data, stat = zk.get("/cond_stale")
        assert data == b"v1", "a rejected set must not change the data"
        assert stat.version == 1, "a rejected set must not bump the version"


    def test_set_with_version_minus_one_always_applies(self, zk):
        zk.create("/cond_any", b"v0")
        zk.set("/cond_any", b"v1")
        zk.set("/cond_any", b"v2", version=-1)
        data, _stat = zk.get("/cond_any")
        assert data == b"v2"


    def test_the_version_a_set_returns_works_for_the_next_set(self, zk):
        """A client that chains writes should never need to re-read."""
        zk.create("/chained", b"v0")
        stat = zk.set("/chained", b"v1")
        stat = zk.set("/chained", b"v2", version=stat.version)
        zk.set("/chained", b"v3", version=stat.version)

        data, stat = zk.get("/chained")
        assert data == b"v3"
        assert stat.version == 3


    def test_a_missing_node_reports_nonode_not_badversion(self, zk):
        """Existence is checked before the version. A client retrying on
        BadVersion would otherwise spin forever on a deleted node."""
        with pytest.raises(NoNodeError):
            zk.set("/never_existed", b"data", version=0)


    def test_the_loser_of_a_race_is_rejected(self, zk, zk2):
        """Both clients read version 0 and try to write. Exactly one wins."""
        zk.create("/contested", b"v0")

        _data, stat_a = zk.get("/contested")
        _data, stat_b = zk2.get("/contested")
        assert stat_a.version == stat_b.version

        zk.set("/contested", b"written_by_a", version=stat_a.version)
        with pytest.raises(BadVersionError):
            zk2.set("/contested", b"written_by_b", version=stat_b.version)

        data, _stat = zk.get("/contested")
        assert data == b"written_by_a"


class TestConditionalDelete:

    def test_delete_with_the_matching_version(self, zk):
        zk.create("/cond_del", b"data")
        zk.delete("/cond_del", version=0)
        assert zk.exists("/cond_del") is None


    def test_delete_with_a_stale_version_is_rejected(self, zk):
        zk.create("/cond_del_stale", b"data")
        zk.set("/cond_del_stale", b"changed")

        with pytest.raises(BadVersionError):
            zk.delete("/cond_del_stale", version=0)

        assert zk.exists("/cond_del_stale") is not None


    def test_delete_with_version_minus_one_always_applies(self, zk):
        zk.create("/cond_del_any", b"data")
        zk.set("/cond_del_any", b"changed")
        zk.delete("/cond_del_any", version=-1)
        assert zk.exists("/cond_del_any") is None
