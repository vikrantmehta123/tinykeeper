"""
ACLs and auth.

v1 does not need real access control. It does need the requests to be
answered, because clients send them as a matter of course: every `create`
carries an ACL list on the wire, `zkCli`'s `getAcl` asks for one back, and
some clients send an Auth request during connection setup. A server that
ignores any of these leaves the client waiting for a reply that never comes.

The v1 bar, then: store the ACL the client sends, hand it back unchanged, and
accept auth without falling over. Enforcement can wait.
"""

import pytest
from kazoo.exceptions import BadVersionError
from kazoo.security import OPEN_ACL_UNSAFE, READ_ACL_UNSAFE, Permissions

from markers import ACL, AUTH, todo


class TestGetAcl:
    @todo(ACL)
    def test_a_new_node_has_the_open_acl(self, zk):
        """Created without an explicit ACL, a node gets world:anyone with
        every permission — which is what makes an unsecured cluster work."""
        zk.create("/acl_node", b"data")
        acls, stat = zk.get_acls("/acl_node")

        assert len(acls) == 1
        assert acls[0].id.scheme == "world"
        assert acls[0].id.id == "anyone"
        assert acls[0].perms == Permissions.ALL

    @todo(ACL)
    def test_get_acl_returns_the_nodes_stat(self, zk):
        zk.create("/acl_stat", b"data")
        _acls, stat = zk.get_acls("/acl_stat")
        assert stat.dataLength == 4
        assert stat.aversion == 0


class TestSetAcl:
    @todo(ACL)
    def test_an_acl_can_be_replaced(self, zk):
        zk.create("/acl_set", b"data")
        zk.set_acls("/acl_set", READ_ACL_UNSAFE)

        acls, _stat = zk.get_acls("/acl_set")
        assert acls[0].perms == Permissions.READ

    @todo(ACL)
    def test_setting_an_acl_bumps_aversion(self, zk):
        """`aversion` is to ACLs what `version` is to data: it exists so a
        client can make a conditional ACL change."""
        zk.create("/acl_version", b"data")
        assert zk.exists("/acl_version").aversion == 0

        zk.set_acls("/acl_version", OPEN_ACL_UNSAFE)
        assert zk.exists("/acl_version").aversion == 1

    @todo(ACL)
    def test_setting_an_acl_with_a_stale_version_is_rejected(self, zk):
        """Same conditional-write rule as `set`, keyed on `aversion`.
        (Both ACLs here are the open one: swapping in a restrictive ACL
        would make the second call fail on permissions, not on version.)"""
        zk.create("/acl_cond", b"data")
        zk.set_acls("/acl_cond", OPEN_ACL_UNSAFE)  # aversion is now 1

        with pytest.raises(BadVersionError):
            zk.set_acls("/acl_cond", OPEN_ACL_UNSAFE, version=0)

    @todo(ACL)
    def test_an_acl_given_at_create_time_is_kept(self, zk):
        zk.create("/acl_at_create", b"data", acl=READ_ACL_UNSAFE)
        acls, _stat = zk.get_acls("/acl_at_create")
        assert acls[0].perms == Permissions.READ

    @todo(ACL)
    def test_changing_an_acl_does_not_touch_the_data_version(self, zk):
        zk.create("/acl_isolated", b"data")
        zk.set_acls("/acl_isolated", READ_ACL_UNSAFE)

        _data, stat = zk.get("/acl_isolated")
        assert stat.version == 0


class TestAuth:
    @todo(AUTH)
    def test_an_auth_request_is_answered(self, zk):
        """The session must survive it, and stay usable afterwards."""
        zk.add_auth("digest", "user:password")

        assert zk.connected
        zk.create("/after_auth", b"works")
        assert zk.get("/after_auth")[0] == b"works"

    @todo(AUTH)
    def test_auth_can_be_sent_more_than_once(self, zk):
        zk.add_auth("digest", "user_one:password")
        zk.add_auth("digest", "user_two:password")
        assert zk.connected
