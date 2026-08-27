"""
Watches.

A read can carry a watch flag. If it does, the server remembers the path, the
kind of read, and which session asked — and the next time something changes
there it pushes an unsolicited message to that session. The client did not
poll; the server told it.

Three rules shape everything below:

  * A watch fires **once**. After it fires it is gone, and the client that
    still cares has to set it again on its next read. This is what stops a
    slow client from drowning in notifications for a busy node.
  * The notification says *that* something changed, not *what* it changed to.
    The client re-reads to find out — and by then the value may have moved
    again. Watches are an invalidation signal, not a data feed.
  * A watch belongs to a session, not a connection.

Which read you used decides what you hear about: `get` and `exists` watch the
node's data, `getChildren` watches its child list.
"""

import socket
import time

import pytest

from helpers import WatchRecorder, wait_until
from markers import SESSIONS, WATCHES, todo


class TestDataWatches:
    """Set with `get`. Fires on a change to this node's data, or its death."""

    @todo(WATCHES)
    def test_fires_on_set(self, zk, zk2):
        zk.create("/watched", b"v0")

        watch = WatchRecorder()
        zk.get("/watched", watch=watch)

        zk2.set("/watched", b"v1")

        assert watch.wait(), "no notification arrived"
        assert watch.last.type == "CHANGED"
        assert watch.last.path == "/watched"

    @todo(WATCHES)
    def test_fires_on_delete(self, zk, zk2):
        zk.create("/watch_del", b"data")

        watch = WatchRecorder()
        zk.get("/watch_del", watch=watch)

        zk2.delete("/watch_del")

        assert watch.wait()
        assert watch.last.type == "DELETED"
        assert watch.last.path == "/watch_del"

    @todo(WATCHES)
    def test_does_not_fire_when_a_child_appears(self, zk, zk2):
        """A data watch is not a child watch. Waking a client for changes it
        did not ask about is as wrong as not waking it at all."""
        zk.create("/data_only", b"v0")

        watch = WatchRecorder()
        zk.get("/data_only", watch=watch)

        zk2.create("/data_only/child", b"")

        watch.assert_silent()

    @todo(WATCHES)
    def test_the_notification_carries_no_data(self, zk, zk2):
        """The client is told the path changed and must re-read it. This is
        why a watch can never deliver a stale value."""
        zk.create("/no_payload", b"v0")

        watch = WatchRecorder()
        zk.get("/no_payload", watch=watch)
        zk2.set("/no_payload", b"v1")
        assert watch.wait()

        assert not hasattr(watch.last, "data")
        data, _stat = zk.get("/no_payload")
        assert data == b"v1"


class TestExistsWatches:
    """Set with `exists`. The only watch you can set on a node that is not
    there yet — which is how a client waits for something to appear."""

    @todo(WATCHES)
    def test_fires_on_create(self, zk, zk2):
        watch = WatchRecorder()
        assert zk.exists("/will_appear", watch=watch) is None

        zk2.create("/will_appear", b"surprise")

        assert watch.wait()
        assert watch.last.type == "CREATED"
        assert watch.last.path == "/will_appear"

    @todo(WATCHES)
    def test_fires_on_set(self, zk, zk2):
        zk.create("/exists_set", b"v0")

        watch = WatchRecorder()
        zk.exists("/exists_set", watch=watch)

        zk2.set("/exists_set", b"v1")

        assert watch.wait()
        assert watch.last.type == "CHANGED"

    @todo(WATCHES)
    def test_fires_on_delete(self, zk, zk2):
        zk.create("/exists_del", b"v0")

        watch = WatchRecorder()
        zk.exists("/exists_del", watch=watch)

        zk2.delete("/exists_del")

        assert watch.wait()
        assert watch.last.type == "DELETED"


class TestChildWatches:
    """Set with `getChildren`. Fires when the child list changes — not when
    the children's contents do."""

    @todo(WATCHES)
    def test_fires_when_a_child_is_created(self, zk, zk2):
        zk.create("/watch_parent", b"")

        watch = WatchRecorder()
        zk.get_children("/watch_parent", watch=watch, include_data=True)

        zk2.create("/watch_parent/new_child", b"hi")

        assert watch.wait()
        assert watch.last.type == "CHILD"
        assert watch.last.path == "/watch_parent"

    @todo(WATCHES)
    def test_fires_when_a_child_is_deleted(self, zk, zk2):
        zk.create("/watch_shrink", b"")
        zk.create("/watch_shrink/child", b"")

        watch = WatchRecorder()
        zk.get_children("/watch_shrink", watch=watch, include_data=True)

        zk2.delete("/watch_shrink/child")

        assert watch.wait()
        assert watch.last.type == "CHILD"

    @todo(WATCHES)
    def test_does_not_fire_when_a_childs_data_changes(self, zk, zk2):
        zk.create("/membership", b"")
        zk.create("/membership/child", b"v0")

        watch = WatchRecorder()
        zk.get_children("/membership", watch=watch, include_data=True)

        zk2.set("/membership/child", b"v1")

        watch.assert_silent()

    @todo(WATCHES)
    def test_fires_deleted_when_the_parent_itself_goes(self, zk, zk2):
        """The child list did not change — it ceased to exist. A client
        waiting on it must be told, or it waits forever."""
        zk.create("/vanishing", b"")

        watch = WatchRecorder()
        zk.get_children("/vanishing", watch=watch, include_data=True)

        zk2.delete("/vanishing")

        assert watch.wait()
        assert watch.last.type == "DELETED"


class TestOneShot:
    @todo(WATCHES)
    def test_a_watch_fires_only_once(self, zk, zk2):
        zk.create("/once", b"v0")

        watch = WatchRecorder()
        zk.get("/once", watch=watch)

        zk2.set("/once", b"v1")
        assert watch.wait()

        zk2.set("/once", b"v2")
        zk2.set("/once", b"v3")
        time.sleep(1.0)

        assert watch.count == 1, f"watch fired {watch.count} times"

    @todo(WATCHES)
    def test_a_client_can_re_arm_the_watch(self, zk, zk2):
        """The normal loop: get with a watch, get told, re-read with a new
        watch. Nothing is missed as long as the client re-reads."""
        zk.create("/rearm", b"v0")

        first = WatchRecorder()
        zk.get("/rearm", watch=first)
        zk2.set("/rearm", b"v1")
        assert first.wait()

        second = WatchRecorder()
        data, _stat = zk.get("/rearm", watch=second)
        assert data == b"v1"

        zk2.set("/rearm", b"v2")
        assert second.wait()
        assert first.count == 1, "the spent watch fired again"


class TestDelivery:
    @todo(WATCHES)
    def test_a_client_is_notified_of_its_own_write(self, zk):
        """No special case for the writer. A client that both watches and
        writes hears about its own change."""
        zk.create("/self_watch", b"v0")

        watch = WatchRecorder()
        zk.get("/self_watch", watch=watch)
        zk.set("/self_watch", b"v1")

        assert watch.wait()
        assert watch.last.type == "CHANGED"

    @todo(WATCHES)
    def test_every_watcher_of_a_node_is_notified(self, keeper):
        watchers = [keeper.client() for _ in range(3)]
        writer = keeper.client()

        writer.create("/crowd", b"v0")

        recorders = []
        for client in watchers:
            recorder = WatchRecorder()
            client.get("/crowd", watch=recorder)
            recorders.append(recorder)

        writer.set("/crowd", b"v1")

        for index, recorder in enumerate(recorders):
            assert recorder.wait(), f"watcher {index} was not notified"

    @todo(WATCHES)
    def test_a_watch_is_delivered_only_to_the_session_that_set_it(self, zk, zk2):
        zk.create("/private", b"v0")

        mine = WatchRecorder()
        theirs = WatchRecorder()

        zk.get("/private", watch=mine)
        zk2.get("/private")  # no watch

        zk2.set("/private", b"v1")

        assert mine.wait()
        assert theirs.count == 0

    @todo(WATCHES)
    def test_watches_on_different_paths_do_not_cross(self, zk, zk2):
        zk.create("/path_a", b"v0")
        zk.create("/path_b", b"v0")

        watch_a = WatchRecorder()
        watch_b = WatchRecorder()
        zk.get("/path_a", watch=watch_a)
        zk.get("/path_b", watch=watch_b)

        zk2.set("/path_a", b"v1")

        assert watch_a.wait()
        assert watch_a.last.path == "/path_a"
        assert watch_b.count == 0


class TestWatchesAcrossReconnects:
    @todo(SESSIONS)
    def test_a_watch_survives_a_broken_connection(self, keeper, zk2):
        """Watches live on the server, keyed by session — but the server
        forgets them when the connection drops. On reconnect the client
        replays them with SetWatches (opcode 101), and the server has to
        honour that replay, including firing immediately for anything that
        changed while the client was away."""
        watcher = keeper.client(reconnecting=True)
        watcher.create("/watch_reconnect", b"v0")

        watch = WatchRecorder()
        watcher.get("/watch_reconnect", watch=watch)

        # Break the connection underneath the client; Kazoo reconnects and
        # re-registers its outstanding watches.
        watcher._connection._socket.shutdown(socket.SHUT_RDWR)
        wait_until(
            lambda: watcher.connected, timeout=15, message="client never reconnected"
        )

        zk2.set("/watch_reconnect", b"v1")

        assert watch.wait(), "the watch was lost across the reconnect"
