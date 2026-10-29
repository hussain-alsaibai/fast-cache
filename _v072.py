"""fast_cache._v072 - SingleFlightCache sidecar.

Stampede protection. When N concurrent callers ask for the same key,
the factory runs once and all N receive the same value. Per-key threads
serialise; different keys run in parallel.

Inspired by Go's singleflight package.

Usage:

```python
    from fast_cache import Cache
    from fast_cache._v072 import SingleFlightCache

    cache = SingleFlightCache(Cache())
    value = cache.get_or_set("hot_key", lambda: expensive_compute(), ttl=60)
```

Stats:
    stats.loads - total factory invocations
    stats.shared - total callers that piggy-backed on a running call
    stats.waiters - currently-waiting callers
    stats.peak_waiters - max simultaneous waiters observed
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Callable

__all__ = ["SingleFlightCache", "SingleFlightStats"]


@dataclass
class SingleFlightStats:
    loads: int = 0
    shared: int = 0
    waiters: int = 0
    peak_waiters: int = 0
    errors: int = 0

    def snapshot(self) -> dict:
        return {
            "loads": self.loads,
            "shared": self.shared,
            "waiters": self.waiters,
            "peak_waiters": self.peak_waiters,
            "errors": self.errors,
        }


class _Flight:
    __slots__ = ("event", "result", "error", "waiters_count")

    def __init__(self) -> None:
        self.event = threading.Event()
        self.result = None
        self.error = None
        self.waiters_count = 0


class SingleFlightCache:
    """Wrap any Cache-like object and add stampede protection.

    Compatible contract: anything with .get(key, default=None),
    .set(key, value, ttl=None), and .has(key).
    """

    def __init__(self, inner, *, name="singleflight"):
        self._inner = inner
        self._name = name
        self._inflight = {}
        self._lock = threading.Lock()
        self.stats = SingleFlightStats()

    def get_or_set(self, key, factory, ttl=None):
        """Get value at key, computing via factory if absent."""
        if self._inner.has(key):
            return self._inner.get(key)

        flight = self._register(key)
        if flight is None:
            # waiter branch
            with self._lock:
                inflight = self._inflight[key]
                inflight.waiters_count += 1
                if inflight.waiters_count > self.stats.peak_waiters:
                    self.stats.peak_waiters = inflight.waiters_count
            event = self._inflight[key].event
            event.wait()
            with self._lock:
                inflight = self._inflight[key]
                if inflight.waiters_count > 0:
                    inflight.waiters_count -= 1
            if inflight.error is not None:
                raise inflight.error
            return inflight.result

        # leader branch
        self.stats.loads += 1
        try:
            value = factory()
        except BaseException as exc:
            self.stats.errors += 1
            flight.error = exc
            flight.event.set()
            with self._lock:
                if self._inflight.get(key) is flight:
                    self._inflight.pop(key, None)
            raise

        flight.result = value
        flight.event.set()
        try:
            self._inner.set(key, value, ttl=ttl)
        except TypeError:
            self._inner.set(key, value)
        with self._lock:
            if self._inflight.get(key) is flight:
                self._inflight.pop(key, None)
        return value

    # -- pass-through ------------------------------------------------------
    # Without these, SingleFlightCache cannot be stacked under another
    # wrapper (e.g. the v0.8.0 SWR layer), because it advertises only
    # get_or_set. Delegation keeps it a drop-in Cache.

    def get(self, key, default=None):
        return self._inner.get(key, default)

    def set(self, key, value, ttl=None):
        try:
            return self._inner.set(key, value, ttl=ttl)
        except TypeError:
            return self._inner.set(key, value)

    def has(self, key):
        return self._inner.has(key)

    def delete(self, key):
        return self._inner.delete(key)

    def keys(self):
        return self._inner.keys()

    def __contains__(self, key):
        return self._inner.has(key)

    def __len__(self):
        return len(self._inner)

    def forget(self, key):
        """Drop any in-flight state for key. Existing waiters wake with a RuntimeError."""
        with self._lock:
            flight = self._inflight.pop(key, None)
        if flight is None:
            return False
        flight.error = RuntimeError(f"in-flight resolution for {key!r} was forgotten")
        flight.event.set()
        return True

    def _register(self, key):
        """Try to claim leadership for key."""
        with self._lock:
            existing = self._inflight.get(key)
            if existing is not None:
                self.stats.shared += 1
                return None
            new_flight = _Flight()
            self._inflight[key] = new_flight
            return new_flight
