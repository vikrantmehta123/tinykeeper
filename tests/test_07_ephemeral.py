"""
Ephemeral nodes.

An ephemeral node belongs to the session that created it. When that session
ends — politely or otherwise — the server deletes the node.

"Or otherwise" is the important half. A client that closes cleanly is easy;
a client whose process is killed says nothing at all, and the server only
finds out when the session timeout elapses with no ping. Ephemeral nodes are
how a system notices that a member died, so a server that only reaps them on a
clean close is a server that never notices a real failure.
"""

import pytest
from kazoo.exceptions import NoChildrenForEphemeralsError

from helpers import wait_until
from markers import EPHEMERAL, SESSIONS, needs_restart, slow, todo


class TestOwnership:
    def test_ephemeral_owner_is_the_creating_session(self, zk):
        """`ephemeralOwner` is how a client tells "this is mine" from
        "this is someone else's" — the basis of every lock recipe."""
        zk.create("/mine", b"data", ephemeral=True)
        _data, stat = zk.get("/mine")
        assert stat.ephemeralOwner == zk.client_id[0]

    def test_a_persistent_node_has_no_owner(self, zk):
        zk.create("/not_mine", b"data")
        _data, stat = zk.get("/not_mine")
        assert stat.ephemeralOwner == 0

    def test_an_ephemeral_node_is_visible_to_other_sessions(self, zk, zk2):
        zk.create("/visible", b"alive", ephemeral=True)
        data, stat = zk2.get("/visible")
        assert data == b"alive"
        assert stat.ephemeralOwner == zk.client_id[0]

    def test_an_ephemeral_node_behaves_normally_while_it_lives(self, zk):
        zk.create("/normal", b"v0", ephemeral=True)
        zk.set("/normal", b"v1")
        data, stat = zk.get("/normal")
        assert data == b"v1"
        assert stat.version == 1

    @todo(EPHEMERAL)
    def test_an_ephemeral_node_cannot_have_children(self, zk):
        """A leaf, always. Otherwise the session ending would have to delete
        a whole subtree that other sessions may own parts of."""
        zk.create("/eph_parent", b"", ephemeral=True)
        with pytest.raises(NoChildrenForEphemeralsError):
            zk.create("/eph_parent/child", b"nope")


class TestReaping:
    def test_reaped_when_the_session_closes(self, keeper):
        owner = keeper.client()
        owner.create("/eph_close", b"temp", ephemeral=True)

        observer = keeper.client()
        assert observer.exists("/eph_close") is not None

        owner.stop()
        owner.close()

        wait_until(
            lambda: observer.exists("/eph_close") is None,
            message="ephemeral node outlived its session",
        )

    def test_persistent_nodes_are_not_reaped(self, keeper):
        owner = keeper.client()
        owner.create("/kept", b"data")
        owner.create("/tossed", b"data", ephemeral=True)
        owner.stop()
        owner.close()

        observer = keeper.client()
        wait_until(lambda: observer.exists("/tossed") is None)
        assert observer.exists("/kept") is not None

    @slow
    def test_reaped_when_the_session_times_out(self, keeper):
        """The client is killed with SIGKILL: no Close request, no TCP
        shutdown handshake it can rely on. The server has to notice that the
        pings stopped and expire the session on its own.

        This is what happens when a machine loses power, and it is the case
        a v1 server has to get right for ephemeral nodes to mean anything.
        """
        victim = keeper.spawn_client_process(
            'zk.create("/eph_expire", b"i-will-be-killed", ephemeral=True)'
        )

        observer = keeper.client()
        assert observer.exists("/eph_expire") is not None

        victim.kill()
        victim.wait(timeout=10)

        # Give the server the negotiated session timeout, plus room to act.
        wait_until(
            lambda: observer.exists("/eph_expire") is None,
            timeout=30,
            interval=0.5,
            message="session never expired, so the ephemeral node was never reaped",
        )

    def test_another_session_may_delete_someone_elses_ephemeral_node(self, zk, zk2):
        """Ownership decides who it dies with, not who may delete it."""
        zk.create("/deletable", b"", ephemeral=True)
        zk2.delete("/deletable")
        assert zk.exists("/deletable") is None

    def test_only_the_dead_sessions_nodes_are_reaped(self, keeper):
        doomed = keeper.client()
        survivor = keeper.client()

        doomed.create("/doomed_node", b"", ephemeral=True)
        survivor.create("/surviving_node", b"", ephemeral=True)

        doomed.stop()
        doomed.close()

        wait_until(lambda: survivor.exists("/doomed_node") is None)
        assert survivor.exists("/surviving_node") is not None
