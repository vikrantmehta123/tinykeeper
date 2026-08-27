"""
The v1 sign-off test: a real ZooKeeper client binary, driving tinykeeper.

Kazoo is an excellent client, but it is one client. `zkCli.sh` is the shell
that ships with Apache ZooKeeper — it is what an operator reaches for, and
"a real client like zkCli.sh can connect to our server" is the stated bar for
v1. It exercises the wire format through a completely different
implementation, so it catches the things a single client's habits hide.

Point the tests at an installation with:

    export ZKCLI=/opt/zookeeper/bin/zkCli.sh

They skip themselves if zkCli.sh or a JVM is missing.
"""

import os
import re
import shutil
import subprocess

import pytest

from markers import GETCHILDREN, signoff, slow, todo

pytestmark = [signoff, slow]


def _find_zkcli():
    explicit = os.environ.get("ZKCLI")
    if explicit and os.path.exists(explicit):
        return explicit
    for candidate in (
        "/opt/zookeeper/bin/zkCli.sh",
        "/usr/share/zookeeper/bin/zkCli.sh",
        "/usr/local/zookeeper/bin/zkCli.sh",
    ):
        if os.path.exists(candidate):
            return candidate
    return shutil.which("zkCli.sh")


@pytest.fixture(scope="session")
def zkcli():
    path = _find_zkcli()
    if not path:
        pytest.skip("zkCli.sh not found; set ZKCLI to its path")
    if not shutil.which("java"):
        pytest.skip("java not found; zkCli.sh needs a JVM")
    return path


#: zkCli logs its whole environment on startup. Strip the logging so a
#: failure message shows the session, not three screens of JVM banner.
_LOG_LINE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} \[myid")


def run_zkcli(zkcli, keeper, *commands, timeout=90):
    """Run a series of zkCli commands in one session and return its output.

    zkCli reads commands from stdin, so one JVM start covers the whole
    script — which matters, because starting it is slow.
    """
    script = "\n".join(commands) + "\nquit\n"
    result = subprocess.run(
        [zkcli, "-server", keeper.address + keeper.chroot],
        input=script,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    output = result.stdout + result.stderr
    return "\n".join(
        line for line in output.splitlines() if not _LOG_LINE.match(line)
    )


class TestZkCli:
    def test_zkcli_connects(self, zkcli, keeper):
        """The handshake, from a client that is not Kazoo. zkCli prints its
        prompt only once the session is established."""
        output = run_zkcli(zkcli, keeper, "ls /")
        assert "(CONNECTED)" in output, output
        assert "Exception" not in output, output

    @todo(GETCHILDREN)
    def test_zkcli_round_trip(self, zkcli, keeper):
        """Create, read, list, update, delete — the operator's whole
        vocabulary, through somebody else's client."""
        output = run_zkcli(
            zkcli,
            keeper,
            "create /zkcli_node hello",
            "get /zkcli_node",
            "set /zkcli_node goodbye",
            "get /zkcli_node",
            "ls /",
            "delete /zkcli_node",
            "ls /",
        )

        assert "Created /zkcli_node" in output, output
        assert "hello" in output, output
        assert "goodbye" in output, output
        assert "zkcli_node" in output, output

        # And the final state is visible to a normal client too.
        client = keeper.client()
        assert client.exists("/zkcli_node") is None

    @todo(GETCHILDREN)
    def test_zkcli_stat_output(self, zkcli, keeper):
        """`ls -s` asks for children *and* the parent's stat (GetChildren2),
        and `stat` asks for it alone. Both print the fields, so a wrong or
        missing field is visible in the output."""
        client = keeper.client()
        client.create("/statme", b"12345")

        output = run_zkcli(zkcli, keeper, "stat /statme", "ls -s /")

        assert "dataLength = 5" in output, output
        assert "numChildren = 0" in output, output
        assert "cversion" in output, output

    def test_zkcli_reports_a_missing_node(self, zkcli, keeper):
        """Error codes have to survive the trip too: zkCli turns NoNode into
        a specific message, not a hang."""
        output = run_zkcli(zkcli, keeper, "get /definitely_not_here")
        assert "Node does not exist" in output, output

    def test_a_node_created_by_kazoo_is_readable_by_zkcli(self, zkcli, keeper):
        """Two independent client implementations, one server, same bytes."""
        client = keeper.client()
        client.create("/cross_client", b"written_by_kazoo")

        output = run_zkcli(zkcli, keeper, "get /cross_client")
        assert "written_by_kazoo" in output, output
