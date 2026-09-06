"""Authentication wire checks without Kazoo heartbeats or reconnects.

These tests observe replies and socket closure directly. They cannot inspect
connection-local identity storage; that needs Rust unit tests or ACL coverage.
"""

import socket
import struct
import time
from contextlib import contextmanager

import pytest

from helpers import wait_until


def _buffer(value):
    return struct.pack(">i", len(value)) + value


def _auth_body(scheme=b"digest", credentials=b"user:password"):
    return struct.pack(">i", 0) + _buffer(scheme) + _buffer(credentials)


class WireClient:
    """A synchronous connection: no automatic pings, retries, or auth."""

    def __init__(self, sock):
        self.sock = sock

    def send(self, payload):
        self.sock.sendall(_buffer(payload))

    def _read_exact(self, size):
        data = bytearray()
        while len(data) < size:
            chunk = self.sock.recv(size - len(data))
            assert chunk, "connection closed before a complete reply arrived"
            data.extend(chunk)
        return bytes(data)

    def receive(self):
        size, = struct.unpack(">i", self._read_exact(4))
        assert 0 <= size <= 1_048_576, f"invalid reply length: {size}"
        return self._read_exact(size)

    def auth(self, credentials=b"user:password", scheme=b"digest"):
        self.send(struct.pack(">ii", -4, 100) + _auth_body(scheme, credentials))
        reply = self.receive()
        # Auth has no response body and does not allocate a zxid.
        assert len(reply) == 16
        return struct.unpack(">iqi", reply)

    def assert_closed(self):
        # A timeout is a failure: the peer must actually close the socket.
        try:
            assert self.sock.recv(1) == b"", "unexpected bytes after final reply"
        except ConnectionResetError:
            pass


@pytest.fixture
def wire_client(keeper):
    @contextmanager
    def connect():
        host, port = keeper.address.rsplit(":", 1)
        with socket.create_connection((host, int(port)), timeout=5) as sock:
            client = WireClient(sock)
            # protocolVersion, lastZxidSeen, timeout, sessionId, password.
            client.send(struct.pack(">iqiq", 0, 0, 4000, 0) + _buffer(bytes(16)))
            response = client.receive()
            assert len(response) >= 20
            version, timeout_ms, session_id = struct.unpack_from(">iiq", response)
            assert version == 0 and timeout_ms > 0 and session_id != 0
            client.timeout_seconds = timeout_ms / 1000
            client.session_id = session_id
            yield client
    return connect


class TestAuthProtocol:
    def test_failure_reply_is_followed_by_server_close(self, wire_client):
        with wire_client() as client:
            # Failure must close even an already-authenticated connection.
            assert client.auth() == (-4, 0, 0)
            assert client.auth(scheme=b"tinykeeper-unsupported-scheme") == (-4, 0, -115)
            client.assert_closed()

        with wire_client() as fresh:
            assert fresh.auth() == (-4, 0, 0)

    def test_repeated_and_distinct_credentials_keep_connection_usable(self, wire_client):
        with wire_client() as client:
            for credentials in (b"alice:one", b"alice:one", b"bob:two", b"alice:one"):
                assert client.auth(credentials) == (-4, 0, 0)

            client.send(struct.pack(">ii", -2, 11))  # ping
            reply = client.receive()
            assert len(reply) == 16
            xid, _, error = struct.unpack(">iqi", reply)
            assert (xid, error) == (-2, 0)

    @pytest.mark.parametrize("body", [
        b"",                              # missing auth_type
        b"\x00\x00\x00",                 # truncated auth_type
        struct.pack(">i", 0),              # missing scheme length
        struct.pack(">ii", 0, -2),         # negative scheme length
        struct.pack(">ii", 0, 2**31 - 1),   # scheme extends past frame
        struct.pack(">ii", 0, 6) + b"dig",  # truncated scheme
        struct.pack(">i", 0) + _buffer(b"digest"),  # missing auth length
        struct.pack(">i", 0) + _buffer(b"digest") + struct.pack(">i", -2),
        struct.pack(">i", 0) + _buffer(b"digest") + struct.pack(">i", 2**31 - 1),
        _auth_body()[:-1],                 # truncated credentials
        _auth_body(scheme=b"\xff"),        # invalid UTF-8 scheme
        _auth_body() + b"unexpected",      # trailing bytes
    ], ids=[
        "empty", "short-type", "missing-scheme-length", "negative-scheme-length",
        "huge-scheme-length", "short-scheme", "missing-auth-length",
        "negative-auth-length", "huge-auth-length", "short-auth",
        "invalid-scheme-utf8", "trailing-bytes",
    ])
    def test_malformed_auth_closes_without_panicking(self, keeper, wire_client, body):
        if keeper.external:
            pytest.skip("tinykeeper's strict malformed-body policy")
        with wire_client() as client:
            client.send(struct.pack(">ii", -4, 100) + body)
            client.assert_closed()

        with wire_client() as fresh:
            assert fresh.auth() == (-4, 0, 0)
        assert "panicked at" not in keeper.read_log()

    def test_invalid_credential_utf8_is_rejected(self, keeper, wire_client):
        if keeper.external:
            pytest.skip("tinykeeper rejects invalid UTF-8; Java replaces invalid bytes")
        with wire_client() as client:
            assert client.auth(b"user:\xff") == (-4, 0, -115)
            client.assert_closed()
        with wire_client() as fresh:
            assert fresh.auth() == (-4, 0, 0)
        assert "panicked at" not in keeper.read_log()

    @pytest.mark.slow
    def test_auth_only_traffic_refreshes_session(self, keeper, wire_client, zk):
        with wire_client() as client:
            path = "/auth_only_ephemeral"
            wire_path = (keeper.chroot + path).encode()
            # Create an ephemeral with world:anyone ALL ACL using this session.
            acl = struct.pack(">ii", 1, 31) + _buffer(b"world") + _buffer(b"anyone")
            body = _buffer(wire_path) + _buffer(b"alive") + acl + struct.pack(">i", 1)
            client.send(struct.pack(">ii", 1, 1) + body)
            reply = client.receive()
            assert struct.unpack_from(">iqi", reply)[::2] == (1, 0)
            stat = zk.exists(path)
            assert stat is not None and stat.ephemeralOwner == client.session_id

            # Only Auth travels on the owning connection from this point.
            deadline = time.monotonic() + 2 * client.timeout_seconds + 2
            while time.monotonic() < deadline:
                assert client.auth() == (-4, 0, 0)
                assert zk.exists(path) is not None, "session expired during Auth traffic"
                time.sleep(min(0.25, client.timeout_seconds / 4))

            # Keep TCP open, but stop Auth: prove the same session can expire.
            wait_until(
                lambda: zk.exists(path) is None,
                timeout=2 * client.timeout_seconds + 5,
                message="session did not expire after Auth traffic stopped",
            )
