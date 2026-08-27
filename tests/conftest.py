"""
Test harness for the tinykeeper v1 acceptance suite.

The suite talks to the server the way a real client does: over TCP, using
Kazoo (the Python ZooKeeper client). Nothing here reaches into tinykeeper's
internals, so the same tests can be pointed at a real ZooKeeper to prove the
tests themselves are correct:

    pytest tests/                              # against tinykeeper
    pytest tests/ --zk-host 127.0.0.1:2181     # against real ZooKeeper

How a test gets a server
------------------------
Each test gets its own tinykeeper process, in its own working directory, on
its own free port. tinykeeper reads `keeper_config.toml` from its working
directory, so the harness writes one there and launches the binary with that
cwd. No test can see another test's data, and the suite never touches the
repository's own `tinykeeper-data/`.

The `keeper` fixture hands back a `Keeper` handle that owns the process. Tests
that need to prove durability call `keeper.restart()` or
`keeper.crash_and_restart()`; because the handle owns the process, teardown
always kills whatever is actually running, and no orphan survives to poison
the next test.
"""

from __future__ import annotations

import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import textwrap
import time
import uuid
from pathlib import Path

import pytest
from kazoo.client import KazooClient
from kazoo.retry import KazooRetry

PROJECT_ROOT = Path(
    os.environ.get(
        "TINYKEEPER_PROJECT_ROOT",
        Path(__file__).resolve().parent.parent,
    )
)

HOST = "127.0.0.1"

# Session timeout requested by test clients, in seconds. Real ZooKeeper clamps
# this to [2 * tickTime, 20 * tickTime]; with the usual tickTime of 2000ms the
# floor is 4 seconds, so anything lower is silently raised.
SESSION_TIMEOUT = 4.0

# How long to wait for a client to connect before giving up.
CONNECT_TIMEOUT = 10.0


# ───────────────────────────────────────────────────────────────────────
# Command line
# ───────────────────────────────────────────────────────────────────────


def pytest_addoption(parser):
    parser.addoption(
        "--zk-host",
        action="store",
        default=None,
        metavar="HOST:PORT",
        help=(
            "Run against an already-running ZooKeeper instead of tinykeeper. "
            "Used to validate that the tests themselves are correct."
        ),
    )
    parser.addoption(
        "--keep-logs",
        action="store_true",
        default=False,
        help="Do not delete each test's working directory (useful for debugging).",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "needs_restart: test restarts the server process; skipped in --zk-host mode",
    )
    config.addinivalue_line(
        "markers",
        "real_root: test needs the server's true '/' ; skipped in --zk-host mode",
    )
    config.addinivalue_line("markers", "slow: test waits on a real timeout")
    config.addinivalue_line(
        "markers", "signoff: end-to-end check against a real client binary"
    )


def pytest_collection_modifyitems(config, items):
    """In --zk-host mode, drop the `todo` xfail markers.

    Real ZooKeeper implements everything in this suite, so a test marked
    "not implemented in tinykeeper yet" must run as a normal pass/fail there.
    Skipping of restart-dependent tests is handled by the `keeper` fixture,
    not here, so this hook only ever removes markers.
    """
    if not config.getoption("--zk-host"):
        return
    for item in items:
        item.own_markers[:] = [m for m in item.own_markers if m.name != "xfail"]


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Record each phase's result so fixtures can see whether the test failed."""
    outcome = yield
    report = outcome.get_result()
    setattr(item, f"rep_{report.when}", report)


# ───────────────────────────────────────────────────────────────────────
# The server under test
# ───────────────────────────────────────────────────────────────────────


class Keeper:
    """Owns the server under test and every client connected to it.

    A test never constructs this; it arrives via the `keeper` fixture.
    """

    def __init__(self, binary: Path, workdir: Path):
        self.binary = binary
        self.workdir = workdir
        self.port = _free_port()
        self.log_path = workdir / "server.log"
        self.proc: subprocess.Popen | None = None
        self.chroot = ""
        self._clients: list[KazooClient] = []
        self._expect_dead = False

    # ── addressing ──

    @property
    def address(self) -> str:
        """host:port, as a client would spell it."""
        return f"{HOST}:{self.port}"

    @property
    def external(self) -> bool:
        return self.binary is None

    # ── process lifecycle ──

    def start(self) -> None:
        """Launch the server and wait until it accepts connections."""
        self._write_config()

        deadline = time.time() + 20
        last_log = ""
        while time.time() < deadline:
            log = open(self.log_path, "ab")
            self.proc = subprocess.Popen(
                [str(self.binary)],
                cwd=str(self.workdir),
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            log.close()

            if _wait_for_port(HOST, self.port, timeout=10):
                self._expect_dead = False
                return

            # The port never opened. If the process died on a stale listening
            # socket from the run we just killed, back off and try again.
            self.proc.poll()
            last_log = self.read_log()
            _terminate(self.proc)
            if "Address already in use" not in last_log:
                break
            time.sleep(0.3)

        raise RuntimeError(
            f"tinykeeper did not start on {self.address}\n"
            f"--- server log ---\n{last_log or self.read_log()}"
        )

    def stop(self, *, graceful: bool = True) -> None:
        """Stop the server. `graceful=False` sends SIGKILL, giving it no
        chance to flush anything on the way out."""
        self.close_clients()
        if self.proc is None:
            return
        if graceful:
            _terminate(self.proc)
        else:
            self.proc.kill()
            self.proc.wait(timeout=10)
        self.proc = None
        self._expect_dead = True

    def restart(self) -> None:
        """Stop the server politely and bring it back up on the same data."""
        self.stop(graceful=True)
        self.start()

    def crash_and_restart(self) -> None:
        """SIGKILL the server and bring it back up on the same data.

        This is the honest durability test: nothing can be flushed during a
        SIGKILL, so whatever survives was already on disk when the write was
        acknowledged.
        """
        self.stop(graceful=False)
        self.start()

    @property
    def alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    # ── clients ──

    def client(
        self, *, timeout: float = SESSION_TIMEOUT, reconnecting: bool = False, **kwargs
    ) -> KazooClient:
        """A started Kazoo client, closed automatically at end of test.

        By default the client does not retry a lost connection, so a test
        that breaks the server sees it immediately. `reconnecting=True` gives
        it a retry policy instead — needed by the tests that deliberately
        drop the connection to watch the session come back.
        """
        if reconnecting:
            kwargs.setdefault(
                "connection_retry",
                KazooRetry(max_tries=-1, delay=0.1, backoff=1, max_delay=0.5),
            )
        client = KazooClient(
            hosts=self.address + self.chroot, timeout=timeout, **kwargs
        )
        client.start(timeout=CONNECT_TIMEOUT)
        self._clients.append(client)
        return client

    def spawn_client_process(self, body: str) -> subprocess.Popen:
        """Run a Kazoo client in a separate process, so the test can kill it.

        `body` is Python source that may use `zk` (a started client) and must
        print "READY" once its setup is done. The returned process is left
        blocked; the caller decides how it dies.
        """
        script = textwrap.dedent(
            f"""
            import sys, time
            from kazoo.client import KazooClient
            zk = KazooClient(hosts={self.address + self.chroot!r},
                             timeout={SESSION_TIMEOUT})
            zk.start(timeout={CONNECT_TIMEOUT})
            """
        ) + textwrap.dedent(body) + textwrap.dedent(
            """
            print("READY", flush=True)
            time.sleep(600)
            """
        )
        proc = subprocess.Popen(
            [sys.executable, "-c", script],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        line = proc.stdout.readline()
        if "READY" not in line:
            proc.kill()
            raise RuntimeError(f"helper client failed to start: {line}{proc.stdout.read()}")
        return proc

    def close_clients(self) -> None:
        for client in self._clients:
            try:
                client.stop()
                client.close()
            except Exception:
                pass
        self._clients.clear()

    # ── diagnostics ──

    def read_log(self, tail_lines: int = 60) -> str:
        try:
            lines = self.log_path.read_text(errors="replace").splitlines()
        except OSError:
            return ""
        return "\n".join(lines[-tail_lines:])

    def _write_config(self) -> None:
        (self.workdir / "keeper_config.toml").write_text(
            "\n".join(
                [
                    f'listen_host = "{HOST}"',
                    f"tcp_port = {self.port}",
                    'storage_path = "./data"',
                    # Deliberately long: the suite tests *session* expiry, and
                    # a short connection idle timeout would muddy that.
                    "idle_timeout_secs = 120",
                    "",
                ]
            )
        )


class ExternalKeeper(Keeper):
    """A `Keeper` that points at a ZooKeeper someone else is running.

    Used by `--zk-host` to check the suite against the reference
    implementation. Every test gets its own chroot so the suite cannot damage
    data that already lives on that server.
    """

    def __init__(self, host: str, port: int):
        self.binary = None
        self.workdir = None
        self.host = host
        self.port = port
        self.log_path = Path(os.devnull)
        self.proc = None
        self.chroot = ""
        self._clients = []
        self._expect_dead = False

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

    def start(self) -> None:
        bootstrap = KazooClient(hosts=self.address, timeout=SESSION_TIMEOUT)
        bootstrap.start(timeout=CONNECT_TIMEOUT)
        self.chroot = f"/tk_it_{uuid.uuid4().hex[:12]}"
        bootstrap.create(self.chroot)
        bootstrap.stop()
        bootstrap.close()

    def stop(self, *, graceful: bool = True) -> None:
        self.close_clients()
        cleanup = KazooClient(hosts=self.address, timeout=SESSION_TIMEOUT)
        cleanup.start(timeout=CONNECT_TIMEOUT)
        try:
            cleanup.delete(self.chroot, recursive=True)
        except Exception:
            pass
        cleanup.stop()
        cleanup.close()

    def restart(self):
        pytest.skip("cannot restart an externally managed server")

    crash_and_restart = restart

    @property
    def alive(self) -> bool:
        return True

    def read_log(self, tail_lines: int = 60) -> str:
        return "(external server: no log captured)"


# ───────────────────────────────────────────────────────────────────────
# Fixtures
# ───────────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def tinykeeper_binary(request) -> Path | None:
    """Build tinykeeper once per session. `None` in --zk-host mode."""
    if request.config.getoption("--zk-host"):
        return None

    result = subprocess.run(
        ["cargo", "build"],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        timeout=600,
    )
    if result.returncode != 0:
        pytest.fail(f"cargo build failed:\n{result.stdout}\n{result.stderr}")

    binary = PROJECT_ROOT / "target" / "debug" / "tinykeeper"
    if not binary.exists():
        pytest.fail(f"binary not found at {binary}")
    return binary


@pytest.fixture
def keeper(request, tinykeeper_binary):
    """A running server, private to this test."""
    external = request.config.getoption("--zk-host")

    if external:
        host, _, port = external.partition(":")
        for marker in ("needs_restart", "real_root"):
            if request.node.get_closest_marker(marker):
                pytest.skip(f"--zk-host mode: '{marker}' tests are not applicable")
        server = ExternalKeeper(host, int(port or 2181))
        server.start()
        yield server
        server.stop()
        return

    workdir = Path(tempfile.mkdtemp(prefix="tinykeeper-test-"))
    server = Keeper(tinykeeper_binary, workdir)
    server.start()

    yield server

    failed = any(
        getattr(request.node, f"rep_{phase}", None) is not None
        and getattr(request.node, f"rep_{phase}").failed
        for phase in ("setup", "call")
    )
    crashed = not server.alive and not server._expect_dead

    if failed or crashed:
        print(f"\n--- tinykeeper log ({server.log_path}) ---\n{server.read_log()}")

    server.stop()

    if crashed and not failed:
        pytest.fail("the tinykeeper process died during the test (log above)")

    if not request.config.getoption("--keep-logs"):
        shutil.rmtree(workdir, ignore_errors=True)


@pytest.fixture
def zk(keeper) -> KazooClient:
    """A connected client. The common case: one client, one server."""
    return keeper.client()


@pytest.fixture
def zk2(keeper) -> KazooClient:
    """A second, independent client — a different session on the same server."""
    return keeper.client()


# ───────────────────────────────────────────────────────────────────────
# Small helpers
# ───────────────────────────────────────────────────────────────────────


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind((HOST, 0))
        return sock.getsockname()[1]


def _wait_for_port(host: str, port: int, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def _terminate(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)
