"""
Sequential nodes.

Ask for `/queue/item-` with the sequential flag and you get back
`/queue/item-0000000003`: the server appends a ten-digit, zero-padded counter
to the name you asked for. The zero padding matters — it makes lexical
ordering the same as numeric ordering, so a client can sort the child list as
plain strings and know who is first in line.

The counter is per parent, and it comes from the parent's `cversion`. That has
a consequence worth stating out loud: it does *not* reset when children are
deleted. If it did, a queue would hand out a name it had already used, and two
clients would think they held the same position.
"""

import pytest

from helpers import wait_until
from markers import EPHEMERAL, SEQUENTIAL, needs_restart, todo


class TestSequentialNaming:
    def test_the_suffix_is_ten_padded_digits(self, zk):
        zk.create("/seq", b"")
        path = zk.create("/seq/item-", b"first", sequence=True)

        assert path.startswith("/seq/item-")
        suffix = path[len("/seq/item-") :]
        assert len(suffix) == 10
        assert suffix.isdigit()

    def test_the_first_child_is_zero(self, zk):
        zk.create("/seq_zero", b"")
        path = zk.create("/seq_zero/n-", b"", sequence=True)
        assert path == "/seq_zero/n-0000000000"

    def test_the_returned_path_is_the_real_one(self, zk):
        """The client only learns the generated name from the reply, so the
        path in that reply has to be the path that was actually created."""
        zk.create("/seq_real", b"")
        path = zk.create("/seq_real/n-", b"payload", sequence=True)

        data, _stat = zk.get(path)
        assert data == b"payload"
        assert path.rsplit("/", 1)[1] in zk.get_children("/seq_real", include_data=True)[0]

    def test_the_counter_increases(self, zk):
        zk.create("/seq_inc", b"")
        numbers = [
            int(zk.create("/seq_inc/n-", b"", sequence=True).rsplit("-", 1)[1])
            for _ in range(5)
        ]
        assert numbers == sorted(numbers)
        assert len(set(numbers)) == 5

    def test_lexical_order_matches_creation_order(self, zk):
        """This is the whole point of the padding: `sorted(children)` is the
        queue, with no parsing."""
        zk.create("/seq_order", b"")
        created = [zk.create("/seq_order/n-", b"", sequence=True) for _ in range(12)]

        children, _stat = zk.get_children("/seq_order", include_data=True)
        assert sorted(children) == [path.rsplit("/", 1)[1] for path in created]

    def test_each_parent_has_its_own_counter(self, zk):
        zk.create("/seq_a", b"")
        zk.create("/seq_b", b"")

        zk.create("/seq_a/n-", b"", sequence=True)
        zk.create("/seq_a/n-", b"", sequence=True)
        first_under_b = zk.create("/seq_b/n-", b"", sequence=True)

        assert first_under_b == "/seq_b/n-0000000000"

    @todo(SEQUENTIAL)
    def test_a_sequential_node_can_have_an_empty_prefix(self, zk):
        zk.create("/seq_bare", b"")
        path = zk.create("/seq_bare/", b"", sequence=True)
        assert path.rsplit("/", 1)[1].isdigit()


class TestCounterDurability:
    """The counter must never hand out a name twice."""

    def test_deleting_children_does_not_reset_the_counter(self, zk):
        zk.create("/seq_reuse", b"")
        first_batch = [zk.create("/seq_reuse/n-", b"", sequence=True) for _ in range(3)]
        for path in first_batch:
            zk.delete(path)

        assert zk.get_children("/seq_reuse", include_data=True)[0] == []

        after = zk.create("/seq_reuse/n-", b"", sequence=True)
        highest = max(int(path.rsplit("-", 1)[1]) for path in first_batch)
        assert int(after.rsplit("-", 1)[1]) > highest

    @needs_restart
    def test_the_counter_survives_a_restart(self, keeper):
        client = keeper.client()
        client.create("/seq_persist", b"")
        before = int(
            client.create("/seq_persist/s-", b"", sequence=True).rsplit("-", 1)[1]
        )

        keeper.restart()

        client = keeper.client()
        after = int(
            client.create("/seq_persist/s-", b"", sequence=True).rsplit("-", 1)[1]
        )
        assert after > before


class TestSequentialAndEphemeral:
    """Both flags at once is the standard recipe for a distributed lock:
    the node disappears if the holder dies, and the number says whose turn
    it is."""

    def test_a_node_can_be_both(self, zk):
        zk.create("/lock", b"")
        path = zk.create("/lock/held-", b"", sequence=True, ephemeral=True)

        assert path.startswith("/lock/held-")
        _data, stat = zk.get(path)
        assert stat.ephemeralOwner == zk.client_id[0]

    def test_the_lock_is_released_when_the_holder_disconnects(self, keeper):
        holder = keeper.client()
        holder.create("/lock_release", b"")
        path = holder.create("/lock_release/held-", b"", sequence=True, ephemeral=True)

        observer = keeper.client()
        assert observer.exists(path) is not None

        holder.stop()
        holder.close()

        wait_until(
            lambda: observer.exists(path) is None,
            message="the lock was never released",
        )
