"""
The Stat struct.

Every read returns the node's metadata alongside its data, and clients lean on
it hard. `version` drives optimistic concurrency. `ephemeralOwner` says who
owns the node. The three zxids say *when*, on a timeline the whole server
shares, each part of the node last changed.

A zxid is the id of the transaction that made a change. It comes from a single
counter that every write increments, so comparing two zxids tells you which
change happened first — something wall-clock timestamps cannot do reliably.
Each node carries three:

    czxid   the transaction that created this node
    mzxid   the transaction that last changed this node's data
    pzxid   the transaction that last added or removed one of its children
"""

import time

import pytest




class TestVersions:
    """`version` counts data changes and nothing else."""

    def test_a_new_node_is_version_zero(self, zk):
        zk.create("/v", b"data")
        _data, stat = zk.get("/v")
        assert stat.version == 0

    def test_each_set_increments_the_version(self, zk):
        zk.create("/ver", b"v0")
        for expected in (1, 2, 3):
            zk.set("/ver", b"x")
            _data, stat = zk.get("/ver")
            assert stat.version == expected

    def test_creating_a_child_does_not_touch_the_parents_version(self, zk):
        """Child changes move `cversion`, never `version`. A client watching
        `version` for data changes must not be woken by child churn."""
        zk.create("/untouched", b"data")
        zk.create("/untouched/child", b"")

        _data, stat = zk.get("/untouched")
        assert stat.version == 0

    def test_cversion_counts_child_creates_and_deletes(self, zk):
        zk.create("/cv", b"")
        assert zk.exists("/cv").cversion == 0

        zk.create("/cv/one", b"")
        assert zk.exists("/cv").cversion == 1

        zk.create("/cv/two", b"")
        assert zk.exists("/cv").cversion == 2

        zk.delete("/cv/one")
        assert zk.exists("/cv").cversion == 3

    def test_a_new_node_has_acl_version_zero(self, zk):
        zk.create("/av", b"")
        assert zk.exists("/av").aversion == 0


class TestSizes:
    def test_data_length(self, zk):
        zk.create("/sized", b"12345")
        assert zk.exists("/sized").dataLength == 5

    def test_data_length_follows_a_set(self, zk):
        zk.create("/resized", b"12345")
        zk.set("/resized", b"ab")
        assert zk.exists("/resized").dataLength == 2

    def test_num_children(self, zk):
        zk.create("/parent_stat", b"")
        assert zk.exists("/parent_stat").numChildren == 0

        zk.create("/parent_stat/a", b"")
        assert zk.exists("/parent_stat").numChildren == 1

        zk.create("/parent_stat/b", b"")
        assert zk.exists("/parent_stat").numChildren == 2

        zk.delete("/parent_stat/a")
        assert zk.exists("/parent_stat").numChildren == 1

    def test_num_children_counts_only_direct_children(self, zk):
        zk.create("/depth", b"")
        zk.create("/depth/child", b"")
        zk.create("/depth/child/grandchild", b"")
        assert zk.exists("/depth").numChildren == 1


class TestTimestamps:
    """ctime and mtime are server wall-clock milliseconds since the epoch."""

    def test_ctime_is_the_moment_of_creation(self, zk):
        before = time.time() * 1000
        zk.create("/timed", b"")
        after = time.time() * 1000

        stat = zk.exists("/timed")
        # A one-second margin either way: the server's clock is not this
        # process's clock, and the value is truncated to milliseconds.
        assert before - 1000 <= stat.ctime <= after + 1000

    def test_mtime_starts_equal_to_ctime(self, zk):
        zk.create("/fresh", b"")
        stat = zk.exists("/fresh")
        assert stat.mtime == stat.ctime

    def test_mtime_moves_forward_on_set(self, zk):
        zk.create("/mtimed", b"v0")
        before = zk.exists("/mtimed").mtime

        time.sleep(0.05)
        zk.set("/mtimed", b"v1")

        assert zk.exists("/mtimed").mtime >= before

    def test_ctime_never_changes(self, zk):
        zk.create("/stable", b"v0")
        original = zk.exists("/stable").ctime

        time.sleep(0.05)
        zk.set("/stable", b"v1")
        zk.create("/stable/child", b"")

        assert zk.exists("/stable").ctime == original


class TestZxids:
    """Every write gets a transaction id, and the ids only go up."""


    def test_a_created_node_has_a_real_czxid(self, zk):
        zk.create("/zx", b"")
        stat = zk.exists("/zx")
        assert stat.czxid > 0


    def test_a_new_node_has_all_three_zxids_equal(self, zk):
        """At creation, the node was created, its data was written, and its
        (empty) child list was established — all by one transaction."""
        zk.create("/zx_equal", b"data")
        stat = zk.exists("/zx_equal")
        assert stat.czxid == stat.mzxid == stat.pzxid


    def test_later_nodes_get_higher_czxids(self, zk):
        zk.create("/zx_first", b"")
        first = zk.exists("/zx_first").czxid

        zk.create("/zx_second", b"")
        second = zk.exists("/zx_second").czxid

        assert second > first


    def test_set_advances_mzxid_but_not_czxid(self, zk):
        zk.create("/zx_set", b"v0")
        before = zk.exists("/zx_set")

        zk.set("/zx_set", b"v1")
        after = zk.exists("/zx_set")

        assert after.czxid == before.czxid
        assert after.mzxid > before.mzxid


    def test_reads_do_not_advance_zxids(self, zk):
        """Only writes get transaction ids. A busy reader must not move the
        counter, or every read would look like a change to anyone watching."""
        zk.create("/zx_read", b"v0")
        before = zk.exists("/zx_read").mzxid

        for _ in range(5):
            zk.get("/zx_read")
            zk.exists("/zx_read")

        assert zk.exists("/zx_read").mzxid == before


    def test_creating_a_child_advances_the_parents_pzxid(self, zk):
        zk.create("/zx_parent", b"")
        before = zk.exists("/zx_parent")

        zk.create("/zx_parent/child", b"")
        after = zk.exists("/zx_parent")

        assert after.pzxid > before.pzxid
        assert after.mzxid == before.mzxid, "a child is not a data change"


    def test_deleting_a_child_advances_the_parents_pzxid(self, zk):
        zk.create("/zx_del_parent", b"")
        zk.create("/zx_del_parent/child", b"")
        before = zk.exists("/zx_del_parent").pzxid

        zk.delete("/zx_del_parent/child")

        assert zk.exists("/zx_del_parent").pzxid > before


    def test_changing_a_childs_data_does_not_advance_the_parents_pzxid(self, zk):
        """pzxid tracks the *membership* of the child list, not what is
        inside the children."""
        zk.create("/zx_stable_parent", b"")
        zk.create("/zx_stable_parent/child", b"v0")
        before = zk.exists("/zx_stable_parent").pzxid

        zk.set("/zx_stable_parent/child", b"v1")

        assert zk.exists("/zx_stable_parent").pzxid == before


    def test_a_recreated_node_gets_a_new_czxid(self, zk):
        zk.create("/zx_phoenix", b"")
        original = zk.exists("/zx_phoenix").czxid

        zk.delete("/zx_phoenix")
        zk.create("/zx_phoenix", b"")

        assert zk.exists("/zx_phoenix").czxid > original
