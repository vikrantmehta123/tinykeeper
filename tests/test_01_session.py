"""
Sessions: the connection, the handshake, and everything that hangs off it.

A ZooKeeper session is not the same thing as a TCP connection. The client
opens a TCP connection, sends a ConnectRequest, and the server replies with a
session id, a 16-byte session password, and the timeout it is willing to
honour. If the TCP connection drops, the client can open a new one and present
that id and password to *resume the same session* — with its ephemeral nodes
and its pending watches intact.

That distinction is the reason sessions matter for v1. Ephemeral nodes are
owned by a session, not a connection, and a client that briefly loses its
network must not lose its lock.
"""

import socket
import time

import pytest
from kazoo.protocol.states import KazooState

from helpers import wait_until
from markers import CLOSE, SESSIONS, SYNC, slow, todo


class TestHandshake:
    """What the server must return when a client connects."""

    def test_client_connects(self, zk):
        assert zk.connected

    def test_server_assigns_a_session_id(self, zk):
        """Every session gets a non-zero id. Clients use it to reconnect,
        and it is what an ephemeral node records as its owner."""
        session_id, _password = zk.client_id
        assert session_id != 0

    @todo(SESSIONS)
    def test_server_returns_a_16_byte_session_password(self, zk):
        """The password is the server's proof that a reconnecting client
        really owns the session it claims. The protocol fixes it at 16
        bytes; a client that gets fewer cannot resume."""
        _session_id, password = zk.client_id
        assert len(password) == 16

    def test_sessions_are_distinct(self, zk, zk2):
        """Two clients must never be handed the same session id, or one
        client's close would delete the other's ephemeral nodes."""
        assert zk.client_id[0] != zk2.client_id[0]

    def test_second_client_sees_first_clients_writes(self, zk, zk2):
        zk.create("/shared", b"from_first")
        data, _stat = zk2.get("/shared")
        assert data == b"from_first"


class TestSessionLifetime:
    """Keeping a session alive, and ending it."""

    @slow
    def test_ping_keeps_an_idle_session_alive(self, zk):
        """With no requests to send, the client sends a Ping (opcode 11)
        every two-thirds of the session timeout. If the server does not
        answer, the client declares the session lost."""
        zk.create("/idle", b"before")

        lost = []
        zk.add_listener(lambda state: lost.append(state) if state != KazooState.CONNECTED else None)

        # Sit idle for longer than the session timeout. Only pings keep it up.
        time.sleep(6)

        assert not lost, f"session did not survive being idle: {lost}"
        assert zk.connected
        data, _stat = zk.get("/idle")
        assert data == b"before"

    def test_graceful_close_is_acknowledged(self, keeper):
        """`stop()` sends Close (opcode -11). The server should answer it
        and tear the session down, rather than waiting for the socket to
        break. Kazoo waits for that reply before returning."""
        client = keeper.client()
        client.create("/before_close", b"data")

        start = time.time()
        client.stop()
        elapsed = time.time() - start

        assert elapsed < 2.0, "server never answered the Close request"

        # The server is still healthy and still has the data.
        survivor = keeper.client()
        data, _stat = survivor.get("/before_close")
        assert data == b"data"

    def test_a_new_client_can_connect_after_one_disconnects(self, keeper):
        first = keeper.client()
        first.create("/handoff", b"v1")
        first.stop()
        first.close()

        second = keeper.client()
        data, _stat = second.get("/handoff")
        assert data == b"v1"

    @todo(SESSIONS)
    def test_session_survives_a_broken_connection(self, keeper):
        """Kill the TCP connection out from under the client. Kazoo opens a
        new one and presents the old session id and password; the server
        must recognise it and let the session continue.

        This is the mechanism that keeps a lock held across a blip."""
        client = keeper.client(reconnecting=True)
        original_id = client.client_id[0]
        client.create("/before_blip", b"v1")

        states = []
        client.add_listener(states.append)

        # There is no public API for "pretend the network died", so reach in
        # and break the socket. Kazoo treats it exactly like a real failure.
        client._connection._socket.shutdown(socket.SHUT_RDWR)

        wait_until(
            lambda: client.connected,
            timeout=15,
            message=f"client never reconnected; states seen: {states}",
        )

        assert KazooState.LOST not in states, "session was expired, not resumed"
        assert client.client_id[0] == original_id, "server issued a new session id"

        # `retry` re-issues the read if it lands in the tail of the outage,
        # which is what any real client does around a reconnect.
        data, _stat = client.retry(client.get, "/before_blip")
        assert data == b"v1"


class TestConcurrency:
    """The server has to hold up with more than one client on it."""

    def test_ten_clients_write_and_all_see_everything(self, keeper):
        clients = [keeper.client() for _ in range(10)]

        for index, client in enumerate(clients):
            client.create(f"/client_{index}", str(index).encode())

        expected = {f"client_{index}" for index in range(10)}
        for client in clients:
            assert expected.issubset(set(client.get_children("/")))

    def test_writes_from_one_client_are_immediately_visible_to_another(self, zk, zk2):
        """A single-node server has no replication lag: once a write is
        acknowledged, every other session must see it."""
        for round_number in range(20):
            path = f"/round_{round_number}"
            zk.create(path, str(round_number).encode())
            data, _stat = zk2.get(path)
            assert data == str(round_number).encode()

    def test_requests_are_answered_in_order(self, zk):
        """Every request carries an xid, and the server must answer in the
        order it received them. Kazoo raises if a reply arrives with an
        unexpected xid, so a violation shows up as a hard failure here."""
        zk.create("/pipeline", b"")

        pending = [zk.create_async(f"/pipeline/n{i}", str(i).encode()) for i in range(50)]
        for index, result in enumerate(pending):
            assert result.get(timeout=10) == f"/pipeline/n{index}"

        assert len(zk.get_children("/pipeline")) == 50


class TestSync:
    """Sync (opcode 9) flushes the leader's pending writes to this client.

    On a single node there is nothing to flush, but the request still has to
    be answered — clients that call it will hang forever otherwise.
    """

    @todo(SYNC)
    def test_sync_returns_the_path(self, zk):
        zk.create("/synced", b"data")
        assert zk.sync("/synced") == "/synced"

    @todo(SYNC)
    def test_sync_on_root(self, zk):
        assert zk.sync("/") == "/"
