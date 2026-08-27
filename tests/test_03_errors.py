"""
Error codes.

A client distinguishes "the node is not there" from "somebody beat me to it"
purely by the error code in the reply header. Locks, leader election and
retry loops are all built on getting these exactly right — returning the
wrong code, or no reply at all, breaks callers in ways that look like
distributed-systems bugs rather than protocol bugs.
"""

import pytest
from kazoo.exceptions import (
    BadArgumentsError,
    NoNodeError,
    NodeExistsError,
    NotEmptyError,
)

from markers import GETCHILDREN, NOT_EMPTY, PATHS, real_root, todo


class TestMissingNodes:
    """Every operation on a path that does not exist returns NoNode (-101)."""

    def test_get_missing(self, zk):
        with pytest.raises(NoNodeError):
            zk.get("/nonexistent")

    def test_set_missing(self, zk):
        with pytest.raises(NoNodeError):
            zk.set("/nonexistent", b"data")

    def test_delete_missing(self, zk):
        with pytest.raises(NoNodeError):
            zk.delete("/nonexistent")

    @todo(GETCHILDREN)
    def test_get_children_missing(self, zk):
        with pytest.raises(NoNodeError):
            zk.get_children("/nonexistent")

    def test_get_children_with_stat_missing(self, zk):
        with pytest.raises(NoNodeError):
            zk.get_children("/nonexistent", include_data=True)

    def test_create_under_a_missing_parent(self, zk):
        """ZooKeeper never creates intermediate nodes. /a/b/c fails unless
        /a/b is already there — the client is expected to walk down and
        create each level itself."""
        with pytest.raises(NoNodeError):
            zk.create("/a/b/c", b"deep")

    def test_a_failed_create_leaves_nothing_behind(self, zk):
        with pytest.raises(NoNodeError):
            zk.create("/missing_parent/child", b"data")
        assert zk.exists("/missing_parent") is None


class TestExistingNodes:
    def test_create_duplicate(self, zk):
        """NodeExists (-110) is what makes create() a usable lock primitive:
        exactly one racing client gets the node, everyone else gets this."""
        zk.create("/dup", b"first")
        with pytest.raises(NodeExistsError):
            zk.create("/dup", b"second")

    def test_a_failed_create_does_not_overwrite(self, zk):
        zk.create("/nooverwrite", b"original")
        with pytest.raises(NodeExistsError):
            zk.create("/nooverwrite", b"clobbered")
        data, stat = zk.get("/nooverwrite")
        assert data == b"original"
        assert stat.version == 0

    def test_create_root(self, zk):
        with pytest.raises(NodeExistsError):
            zk.create("/", b"")


class TestDeletingParents:
    @todo(NOT_EMPTY)
    def test_delete_a_node_that_still_has_children(self, zk):
        """NotEmpty (-111). ZooKeeper has no recursive delete on the wire:
        a client that wants one deletes the leaves itself, bottom up.
        A server that quietly drops the whole subtree instead loses data
        the client never asked it to touch."""
        zk.create("/notempty", b"")
        zk.create("/notempty/child", b"")

        with pytest.raises(NotEmptyError):
            zk.delete("/notempty")

        assert zk.exists("/notempty/child") is not None

    @todo(NOT_EMPTY)
    def test_delete_becomes_possible_once_the_children_are_gone(self, zk):
        zk.create("/emptying", b"")
        zk.create("/emptying/child", b"")

        zk.delete("/emptying/child")
        zk.delete("/emptying")

        assert zk.exists("/emptying") is None


class TestPaths:
    """Path rules the server has to enforce itself.

    Kazoo normalises away the easy mistakes (`//a`, `/a/`, relative paths)
    before anything reaches the wire, so what is left here is what a
    misbehaving or hand-rolled client can actually send.
    """

    @todo(PATHS)
    def test_path_containing_a_null_byte_is_rejected(self, zk):
        with pytest.raises(BadArgumentsError):
            zk.create("/bad" + chr(0) + "path", b"data")

    @real_root
    @todo(PATHS)
    def test_root_cannot_be_deleted(self, zk):
        with pytest.raises(BadArgumentsError):
            zk.delete("/")
        assert zk.exists("/") is not None
