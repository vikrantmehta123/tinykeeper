"""
The five operations everything else is built on: create, get, set, delete,
exists, and listing children.

Nothing here is exotic. If any of it is wrong, no client works at all.
"""

import pytest
from kazoo.exceptions import BadArgumentsError

from markers import CREATE2, GETCHILDREN, real_root, todo


class TestCreate:
    def test_create_then_get(self, zk):
        zk.create("/hello", b"world")
        data, _stat = zk.get("/hello")
        assert data == b"world"

    def test_create_returns_the_path(self, zk):
        assert zk.create("/mynode", b"data") == "/mynode"

    def test_create_with_empty_data(self, zk):
        zk.create("/empty", b"")
        data, _stat = zk.get("/empty")
        assert data == b""

    def test_create_with_arbitrary_bytes(self, zk):
        """Node data is opaque bytes, not text. Every byte value must
        survive the round trip untouched."""
        payload = bytes(range(256))
        zk.create("/binary", payload)
        data, _stat = zk.get("/binary")
        assert data == payload

    def test_create_with_a_large_payload(self, zk):
        """ZooKeeper's default ceiling is 1MB per node. Well under it
        must work, and must not be silently truncated."""
        payload = b"x" * 100_000
        zk.create("/large", payload)
        data, stat = zk.get("/large")
        assert data == payload
        assert stat.dataLength == len(payload)

    def test_create_nested_levels(self, zk):
        zk.create("/a", b"")
        zk.create("/a/b", b"")
        zk.create("/a/b/c", b"leaf")
        data, _stat = zk.get("/a/b/c")
        assert data == b"leaf"


class TestGet:
    def test_get_returns_data_and_stat(self, zk):
        zk.create("/node", b"payload")
        data, stat = zk.get("/node")
        assert data == b"payload"
        assert stat is not None

    def test_get_root(self, zk):
        """The root always exists; it is created by the server, not a client."""
        _data, stat = zk.get("/")
        assert stat is not None


class TestSet:
    def test_set_replaces_the_data(self, zk):
        zk.create("/mutable", b"v1")
        zk.set("/mutable", b"v2")
        data, _stat = zk.get("/mutable")
        assert data == b"v2"

    def test_set_returns_the_new_stat(self, zk):
        zk.create("/returns_stat", b"v1")
        stat = zk.set("/returns_stat", b"v2")
        assert stat.version == 1
        assert stat.dataLength == 2

    def test_set_to_empty(self, zk):
        zk.create("/clearable", b"something")
        zk.set("/clearable", b"")
        data, stat = zk.get("/clearable")
        assert data == b""
        assert stat.dataLength == 0

    def test_repeated_sets(self, zk):
        zk.create("/multi", b"a")
        for value in (b"b", b"c", b"d"):
            zk.set("/multi", value)
        data, stat = zk.get("/multi")
        assert data == b"d"
        assert stat.version == 3


class TestDelete:
    def test_delete_removes_the_node(self, zk):
        zk.create("/doomed", b"bye")
        zk.delete("/doomed")
        assert zk.exists("/doomed") is None

    def test_delete_removes_it_from_the_parents_children(self, zk):
        zk.create("/keep", b"")
        zk.create("/keep/remove_me", b"")
        zk.delete("/keep/remove_me")
        assert zk.get_children("/keep", include_data=True)[0] == []
        assert zk.exists("/keep") is not None

    def test_recreating_a_deleted_node_starts_fresh(self, zk):
        """A path is not an identity. Delete and recreate, and you get a
        brand-new node: version back to 0, new czxid, new ctime."""
        zk.create("/phoenix", b"v1")
        zk.set("/phoenix", b"v2")
        zk.delete("/phoenix")
        zk.create("/phoenix", b"reborn")

        data, stat = zk.get("/phoenix")
        assert data == b"reborn"
        assert stat.version == 0


class TestExists:
    def test_exists_returns_a_stat(self, zk):
        zk.create("/present", b"here")
        assert zk.exists("/present") is not None

    def test_exists_returns_none_when_missing(self, zk):
        """exists is the one read that is not an error on a missing node."""
        assert zk.exists("/ghost") is None

    def test_exists_on_root(self, zk):
        assert zk.exists("/") is not None

    def test_exists_agrees_with_get(self, zk):
        zk.create("/agree", b"data")
        _data, from_get = zk.get("/agree")
        from_exists = zk.exists("/agree")
        assert from_exists.czxid == from_get.czxid
        assert from_exists.version == from_get.version
        assert from_exists.dataLength == from_get.dataLength


class TestGetChildren:
    """Two opcodes do this job.

    GetChildren (8) returns only the names. GetChildren2 (12) returns the
    names *and* the parent's stat. Kazoo picks between them based on
    `include_data`, and zkCli uses both (`ls` and `ls -s`), so a v1 server
    needs to answer both.
    """

    @todo(GETCHILDREN)
    def test_get_children_returns_names(self, zk):
        zk.create("/parent", b"")
        zk.create("/parent/kid", b"")
        assert zk.get_children("/parent") == ["kid"]

    @todo(GETCHILDREN)
    def test_get_children_returns_bare_names_not_paths(self, zk):
        zk.create("/names", b"")
        zk.create("/names/alpha", b"")
        zk.create("/names/beta", b"")
        assert set(zk.get_children("/names")) == {"alpha", "beta"}

    @todo(GETCHILDREN)
    def test_get_children_of_a_leaf_is_empty(self, zk):
        zk.create("/leaf", b"data")
        assert zk.get_children("/leaf") == []

    @todo(GETCHILDREN)
    def test_get_children_of_root(self, zk):
        zk.create("/top_a", b"")
        zk.create("/top_b", b"")
        assert {"top_a", "top_b"}.issubset(set(zk.get_children("/")))

    def test_get_children_with_stat(self, zk):
        """GetChildren2: the same list, plus the parent's stat."""
        zk.create("/with_stat", b"")
        zk.create("/with_stat/one", b"")
        zk.create("/with_stat/two", b"")

        children, stat = zk.get_children("/with_stat", include_data=True)
        assert set(children) == {"one", "two"}
        assert stat.numChildren == 2

    def test_children_at_every_level(self, zk):
        zk.create("/x", b"")
        zk.create("/x/y", b"")
        zk.create("/x/y/z", b"")

        assert "x" in zk.get_children("/", include_data=True)[0]
        assert zk.get_children("/x", include_data=True)[0] == ["y"]
        assert zk.get_children("/x/y", include_data=True)[0] == ["z"]
        assert zk.get_children("/x/y/z", include_data=True)[0] == []

    def test_many_children(self, zk):
        zk.create("/wide", b"")
        for index in range(100):
            zk.create(f"/wide/child_{index:03d}", b"")

        children, stat = zk.get_children("/wide", include_data=True)
        assert len(children) == 100
        assert stat.numChildren == 100
        assert set(children) == {f"child_{index:03d}" for index in range(100)}


class TestCreate2:
    """Create2 (opcode 15) is Create plus the new node's stat in the reply.

    Clients use it to avoid a follow-up round trip after creating a node.
    """

    @todo(CREATE2)
    def test_create_returning_stat(self, zk):
        path, stat = zk.create("/create2_node", b"hello", include_data=True)
        assert path == "/create2_node"
        assert stat.version == 0
        assert stat.dataLength == 5

    @todo(CREATE2)
    def test_create2_stat_matches_a_subsequent_get(self, zk):
        _path, from_create = zk.create("/create2_match", b"abc", include_data=True)
        _data, from_get = zk.get("/create2_match")
        assert from_create.czxid == from_get.czxid
        assert from_create.ctime == from_get.ctime


class TestReservedNamespace:
    """ZooKeeper reserves /zookeeper for itself.

    zkCli shows it in `ls /`, and clients must not be able to scribble in it.
    """

    @real_root
    @todo("the /zookeeper reserved namespace")
    def test_zookeeper_namespace_exists(self, zk):
        assert zk.exists("/zookeeper") is not None

    @real_root
    @todo("the /zookeeper reserved namespace")
    def test_zookeeper_namespace_cannot_be_deleted(self, zk):
        with pytest.raises(BadArgumentsError):
            zk.delete("/zookeeper")
        assert zk.exists("/zookeeper") is not None
