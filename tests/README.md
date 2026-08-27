# tinykeeper v1: Acceptance Suite

As part of the first release, we want: **a correct, complete single-node
ZooKeeper that a real client can connect to.**

It treats tinykeeper as a black-box. The tests don't care how tinykeeper
is implemented internally. Each test connects to the server over TCP
through Kazoo (a real ZooKeeper client) or, through the official shell `zkCli.sh`.

We deliberately define the tests that treat tinykeeper as a black-box, because
by doing so we can run the same tests against Apache ZooKeeper so that we can
verify against actual running Keeper server. 

## Quickstart

```bash
pip install -r tests/requirements.txt
pytest                     # Build tinykeeper and run tests against it. 
```

Useful variations:

```bash
pytest -k watches          # one feature at a time. The feature name is in file name
pytest -m "not slow"       # skip the tests that wait on real timeouts
pytest --keep-logs         # keep each test's working dir and server log
pytest tests/test_11_persistence.py -x
```

## Validation of the Test Suite

Because tinykeeper is being built from scratch, we have to make sure that the tests 
accurately reflect how ZooKeeper actually behaves, not just how we think it behaves. 
We can run the tests against an actual Apache ZooKeeper instance instead of tinykeeper.

```bash
pytest --zk-host 127.0.0.1:2181
```

A test that fails there is a wrong test and it shouldn't be in the suite until it is fixed.

* Every test runs in an isolated, randomly generated sub-directory (like `/tk_it_123)`. 
When the test ends, it deletes the directory. This ensures the tests don't overwrite or
read the live server's actual data.

* Some tests (like persistence testing) require killing the server process 
(SIGTERM or SIGKILL). The suite automatically detects if it's running against 
an external ZK host and skips these destructive tests to avoid bringing down 
your real server. Tests marked `needs_restart` (they kill the server process) 
and `real_root` (they need the true `/`) skip themselves.

* A test marked as `@todo` implies that tinykeeper hasn't implemented it yet and 
those tests are expected to fail. But since Apache ZooKeeper implements everything, 
those tests should pass in when running against an actual server.

## How the Harness Works

Before a test runs, the harness creates a brand new temporary directory, writes
a custom `keeper_config.toml` that binds to a specific port and spins up a dedicated
tinykeeper process. 

That gives every test its own server, its own port and its own storage. Tests
cannot see each other's data, nothing collides with a ZooKeeper already
running on 2181, and the repository's own `tinykeeper-data/` is never touched.

We use the following API to write the tests. In pytest, "fixtures" are the arguments
you pass into a test function. This harness provides three main ones: `keeper`, `zk`, 
and `zk2`.

The `keeper` fixture is like the server administrator. We use it when a test
needs to manipulate the server such as following:

| | |
|---|---|
| `keeper.client()` | a connected Kazoo client, closed for you afterwards |
| `keeper.client(reconnecting=True)` | ditto, but it retries a dropped connection |
| `keeper.restart()` | SIGTERM, then start again on the same data |
| `keeper.crash_and_restart()` | SIGKILL, then start again on the same data |
| `keeper.address` | `host:port` |

Most tests just want a client, so they take the `zk` fixture (and `zk2` for a
second, independent session) for a client.

Since we are spawning processes in the tests, we want to ensure that we 
are not leaving behind zombie processes.

- A test that restarts the server does not leave an orphan behind holding 
  the port — teardown always kills whatever is actually running.
- If we stop a process using SIGTERM, then the OS gives some leeway to do cleanup, 
  flush memory to disk, and exit cleanly. But for real production cases, we can't 
  rely on this. Thus, we use SIGKILL as the default for such tests. These are durability tests. 

When a test fails, the server's log for that test is printed with the failure.

## Feature Roadmap

Any feature that is not yet implemented in tinykeeper is marked as `@todo`.

```python
@todo(WATCHES)
def test_fires_on_set(self, zk, zk2):
    ...
```

That is a **strict** xfail: the test has to fail today, and the moment it
starts passing the suite goes red and tells you to delete the marker. So the
`todo` markers are the v1 tracker.

To see what is left:

```bash
pytest -rx        # (already on by default)
```

The feature categories (like WATCHES or SESSIONS) are defined as constants in a file called `markers.py`.

## What is covered

| File | Features Validated |
|---|---|
| `test_01_session.py` | Handshake, session id and password, ping, close, resumption after a broken connection, concurrent clients, request ordering, sync |
| `test_02_crud.py` | create / get / set / delete / exists, both getChildren opcodes, create2, the `/zookeeper` namespace |
| `test_03_errors.py` | NoNode, NodeExists, NotEmpty, BadArguments, and that failed writes change nothing |
| `test_04_stat.py` | Every Stat field, including czxid / mzxid / pzxid and what does and does not move them |
| `test_05_conditional.py` | Version-checked set and delete — ZooKeeper's whole concurrency-control story |
| `test_06_sequential.py` | Sequential naming, ordering, and a counter that never reuses a number |
| `test_07_ephemeral.py` | Ownership, reaping on close **and on session timeout** |
| `test_08_watches.py` | All three watch kinds, event types, one-shot semantics, delivery rules |
| `test_09_multi.py` | Transactions: atomicity, rollback, version checks |
| `test_10_acl_auth.py` | getACL / setACL / auth answered sanely (no enforcement needed for v1) |
| `test_11_persistence.py` | WAL durability across SIGKILL, and correct replay of data *and* metadata |
| `test_12_zkcli.py` | The sign-off: `zkCli.sh` driving the server end to end |

## V1 Release

v1 is done when, on a clean checkout:

1. `pytest` is green with **no xfails left** — every `todo` marker deleted
   because the feature exists.
2. `pytest --zk-host <a real ZooKeeper>` is green, proving the suite still
   describes ZooKeeper and not just tinykeeper.
3. `pytest -m signoff` passes, i.e. `zkCli.sh` connects and works.

The four-letter commands (`ruok`, `mntr`, etc) are considered out of scope for v1.

