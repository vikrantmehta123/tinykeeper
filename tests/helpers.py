"""Small utilities shared by the tests."""

import threading
import time


class WatchRecorder:
    """Collects the watch notifications the server pushes to a client.

    Pass an instance where Kazoo wants a watch callback:

        watch = WatchRecorder()
        zk.get("/path", watch=watch)
        ...
        assert watch.wait()
        assert watch.last.type == "CHANGED"

    Notifications arrive on Kazoo's event thread, so everything here is
    guarded and every wait is bounded.
    
    Kazoo (the Python ZK client) receives watch notifications on a 
    background thread. If you try to assert against them directly in a 
    test, you'll hit race conditions. WatchRecorder is a thread-safe 
    collector.
    """

    def __init__(self):
        self.events = []
        self._lock = threading.Lock()
        self._fired = threading.Event()

    def __call__(self, event):
        with self._lock:
            self.events.append(event)
        self._fired.set()

    def wait(self, timeout=5.0) -> bool:
        """Block until at least one notification arrives."""
        return self._fired.wait(timeout)

    def assert_silent(self, timeout=1.5):
        """Assert that nothing arrives in the next `timeout` seconds."""
        before = self.count
        time.sleep(timeout)
        after = self.events[before:]
        assert not after, f"expected no notification, got {after}"

    @property
    def last(self):
        with self._lock:
            assert self.events, "no notification was delivered"
            return self.events[-1]

    @property
    def count(self) -> int:
        """How many notifications have arrived so far.

        Deliberately not `__len__`: Kazoo checks the truthiness of a watch
        callback before registering it, and an object whose length is zero
        is falsy — so a recorder with no events yet would be silently
        dropped and never fire.
        """
        with self._lock:
            return len(self.events)


def wait_until(predicate, timeout=10.0, interval=0.1, message="condition never became true"):
    """Poll `predicate` until it is true, or fail the test.

    Used for the handful of things a client learns about only when the
    server gets round to them — a session expiring, an ephemeral node being
    reaped. Everything else in this suite is checked synchronously.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError(f"{message} (waited {timeout}s)")
