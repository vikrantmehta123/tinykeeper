"""
Multi: several operations, one atomic request.

The client bundles creates, sets, deletes and version checks into a single
Multi request (opcode 14). The server applies all of them or none of them.

The version check is what makes this more than a batching convenience. "Create
/leader only if /epoch is still at version 7" is a decision that cannot be
expressed as two requests, because something can happen in between. ClickHouse
leans on this for leader election and for keeping its replication log
consistent, which is why it belongs in v1 rather than in a later milestone.
"""

import pytest
from kazoo.exceptions import BadVersionError, NodeExistsError, RolledBackError

from markers import MULTI, todo


class TestSuccessfulTransactions:
    @todo(MULTI)
    def test_create_and_set_together(self, zk):
        zk.create("/txn_existing", b"old")

        txn = zk.transaction()
        txn.create("/txn_new", b"hello")
        txn.set_data("/txn_existing", b"updated")
        results = txn.commit()

        assert len(results) == 2
        assert not any(isinstance(result, Exception) for result in results)

        assert zk.get("/txn_new")[0] == b"hello"
        assert zk.get("/txn_existing")[0] == b"updated"

    @todo(MULTI)
    def test_the_results_come_back_in_order(self, zk):
        txn = zk.transaction()
        txn.create("/first", b"")
        txn.create("/second", b"")
        results = txn.commit()

        assert results[0] == "/first"
        assert results[1] == "/second"

    @todo(MULTI)
    def test_delete_inside_a_transaction(self, zk):
        zk.create("/txn_del", b"bye")

        txn = zk.transaction()
        txn.delete("/txn_del")
        txn.commit()

        assert zk.exists("/txn_del") is None

    @todo(MULTI)
    def test_operations_see_each_other(self, zk):
        """Within one transaction the operations apply in order, so a later
        one can build on an earlier one."""
        txn = zk.transaction()
        txn.create("/parent_txn", b"")
        txn.create("/parent_txn/child", b"data")
        results = txn.commit()

        assert not any(isinstance(result, Exception) for result in results)
        assert zk.get("/parent_txn/child")[0] == b"data"

    @todo(MULTI)
    def test_an_empty_transaction_is_harmless(self, zk):
        assert zk.transaction().commit() == []


class TestRollback:
    @todo(MULTI)
    def test_one_failure_rolls_back_the_rest(self, zk):
        """The set is perfectly legal on its own. It must still not apply,
        because the create next to it failed."""
        zk.create("/txn_blocker", b"exists")
        zk.create("/txn_target", b"original")

        txn = zk.transaction()
        txn.set_data("/txn_target", b"should_not_apply")
        txn.create("/txn_blocker", b"duplicate")
        results = txn.commit()

        assert isinstance(results[0], RolledBackError)
        assert isinstance(results[1], NodeExistsError)

        data, stat = zk.get("/txn_target")
        assert data == b"original"
        assert stat.version == 0

    @todo(MULTI)
    def test_nothing_is_created_when_a_later_create_fails(self, zk):
        zk.create("/collision", b"")

        txn = zk.transaction()
        txn.create("/all_or_nothing_a", b"")
        txn.create("/all_or_nothing_b", b"")
        txn.create("/collision", b"")
        results = txn.commit()

        assert any(isinstance(result, Exception) for result in results)
        assert zk.exists("/all_or_nothing_a") is None
        assert zk.exists("/all_or_nothing_b") is None


class TestVersionChecks:
    """`check` asserts a node's version without changing it."""

    @todo(MULTI)
    def test_a_matching_check_lets_the_transaction_through(self, zk):
        zk.create("/check_ver", b"v0")

        txn = zk.transaction()
        txn.check("/check_ver", version=0)
        txn.set_data("/check_ver", b"v1")
        results = txn.commit()

        assert not any(isinstance(result, Exception) for result in results)

        data, stat = zk.get("/check_ver")
        assert data == b"v1"
        assert stat.version == 1

    @todo(MULTI)
    def test_a_failing_check_stops_everything(self, zk):
        zk.create("/check_bad", b"v0")

        txn = zk.transaction()
        txn.check("/check_bad", version=99)
        txn.set_data("/check_bad", b"should_not_apply")
        results = txn.commit()

        assert isinstance(results[0], BadVersionError)

        data, stat = zk.get("/check_bad")
        assert data == b"v0"
        assert stat.version == 0

    @todo(MULTI)
    def test_a_check_on_an_unrelated_node_still_guards_the_write(self, zk):
        """The real pattern: guard a write to one node on the state of
        another. This is how a client says "only if I am still the leader"."""
        zk.create("/epoch", b"7")
        zk.create("/guarded", b"original")
        zk.set("/epoch", b"8")  # someone else moved on

        txn = zk.transaction()
        txn.check("/epoch", version=0)
        txn.set_data("/guarded", b"stale_write")
        results = txn.commit()

        assert any(isinstance(result, Exception) for result in results)
        assert zk.get("/guarded")[0] == b"original"
