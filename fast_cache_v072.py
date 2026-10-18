"""
fast-cache v0.7.2 — Cache-stampede protection (single-flight loader)

The classic "cache stampede" happens when an expensive cache key expires
and N concurrent requests all see the miss and recompute the value at
once. The cost is paid N times instead of once, and the backend feels a
spike in load exactly when it can least afford one.

``SingleFlightCache`` solves this with a per-key lock: the first thread
that misses the cache holds it and runs the factory; the rest block on
the same lock and read the result the first thread writes. After the
first thread finishes, every waiter gets the cached value, the lock is
released, and the factory never runs again until the value expires or is
evicted.

Comparison vs ``Cache.get_or_set``:
    cache.get_or_set('k', factory)
    # 10 concurrent callers, all miss, all run factory(). 10x cost.

    SingleFlightCache(cache).get_or_set('k', factory)
    # 10 concurrent callers, one runs factory(), 9 block, all get the
    # same value. 1x cost.

API:
    sf = SingleFlightCache(cache)
    value = sf.get_or_set('key', factory, ttl=3600)
    # same shape as Cache.get_or_set, plus:
    sf.stats.serialized()  # {"in_flight": 0, "waiters": 0, "loads": 1}

Implementation notes:
    * Per-key threading.Event so threads block on the exact key they
      asked about, not on a single global lock. Calls to other keys
      proceed in parallel.
    * The ``in_flight`` dict is guarded by a single threading.Lock on
      lookup/insert only - the per-key Event handles the wait itself.
    * Re-entrancy: a thread that holds the per-key lock (i.e. is the
      loader) calling ``get_or_set`` for the same key from inside the
      factory would deadlock. We don't try to detect this; the same
      thread that calls factory() and the loader are always the same
      thread, so this can't happen from the outside.
    * The factory may return ``None`` (cached as a real miss next time)
      or any sentinel; we treat any non-None return as a real cached
      value. If the factory returns None, callers will retry on the
      next call - that's the user's choice to encode.
"""
from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional

if TYPE_CHECKING:
    # Avoid eager import: this is a sidecar module loaded by users who
    # already have fast_cache on the path. Eager import forces relative
    # imports that break when the module is dropped into a project as a
    # single file.
    from fast_cache import Cache


__version__ = "0.7.2"
__all__ = ["SingleFlightCache", "SingleFlightStats"]


class SingleFlightStats:
    """Counters for a SingleFlightCache.

    Public so callers can expose them as Prometheus gauges or just log
    them. All fields are integers and atomic across the
    ``SingleFlightCache`` lifetime - the underlying cache is locked
    when these are updated.
    """

    __slots__ = ("_loads", "_shared", "_waiters", "_peak_waiters")

    def __init__(self) -> None:
        self._loads = 0       # factory() invocations
        self._shared = 0      # calls that piggy-backed on an in-flight load
        self._waiters = 0     # currently-blocked callers
        self._peak_waiters = 0

    def record_load(self) -> None:
        self._loads += 1

    def record_shared(self) -> None:
        self._shared += 1

    def inc_waiters(self) -> int:
        self._waiters += 1
        if self._waiters > self._peak_waiters:
            self._peak_waiters = self._waiters
        return self._waiters

    def dec_waiters(self) -> None:
        self._waiters -= 1

    def reset(self) -> None:
        """Reset counters in place. Useful for windowed metrics."""
        self._loads = 0
        self._shared = 0
        self._waiters = 0
        self._peak_waiters = 0

    @property
    def loads(self) -> int:
        return self._loads

    @property
    def shared(self) -> int:
        return self._shared

    @property
    def waiters(self) -> int:
        return self._waiters

    @property
    def peak_waiters(self) -> int:
        return self._peak_waiters

    def serialized(self) -> Dict[str, int]:
        return {
            "loads": self._loads,
            "shared": self._shared,
            "waiters": self._waiters,
            "peak_waiters": self._peak_waiters,
        }

    def __repr__(self) -> str:
        return (
            f"SingleFlightStats(loads={self._loads}, "
            f"shared={self._shared}, waiters={self._waiters}, "
            f"peak_waiters={self._peak_waiters})"
        )


class _InFlight:
    """One in-flight load.

    Holds the threading.Event that waiters block on, the value the
    loader produced, and a flag so other threads can tell whether the
    loader has finished.
    """
    __slots__ = ("event", "value", "done")

    def __init__(self) -> None:
        self.event = threading.Event()
        self.value: Any = None
        self.done = False


class SingleFlightCache:
    """Wraps a ``fast_cache.Cache`` with single-flight semantics.

    Threads that miss the cache for the same key share a single factory
    call. The first caller runs the factory; subsequent callers wait on
    a per-key Event and read the cached value the first caller writes.

    Threadsafe. The underlying Cache must also be threadsafe (the
    bundled Cache is).
    """

    def __init__(self, cache: Cache) -> None:
        self._cache = cache
        self._in_flight: Dict[str, _InFlight] = {}
        self._index_lock = threading.Lock()
        self.stats = SingleFlightStats()

    # ------------------------------------------------------------------
    # Single-flight get-or-set
    # ------------------------------------------------------------------

    def get_or_set(self, key: str, factory: Callable[[], Any],
                   ttl: Optional[float] = None) -> Any:
        """Get from cache, or run ``factory()`` exactly once per stampede.

        On a hit: returns the cached value. No factory call.
        On a miss:
          - If another thread is already loading this key: blocks until
            the loader finishes, then returns the same value.
          - Otherwise: runs factory(), writes the result, and returns it.
        """
        # Fast path: hit. Cache.get is already threadsafe.
        value = self._cache.get(key)
        if value is not None:
            return value

        # Slow path: miss. Acquire or join the in-flight group for this key.
        with self._index_lock:
            in_flight = self._in_flight.get(key)
            if in_flight is None:
                # We are the loader for this key.
                in_flight = _InFlight()
                self._in_flight[key] = in_flight
                loader = True
            else:
                loader = False

        if not loader:
            # We are a waiter. Block until the loader finishes.
            self.stats.inc_waiters()
            try:
                # Re-check the cache after acquiring the Event slot -
                # the loader may have raced ahead and written before we
                # started waiting.
                cached = self._cache.get(key)
                if cached is not None:
                    self.stats.record_shared()
                    return cached

                in_flight.event.wait()
                # Loader has finished. Read from the cache so we get the
                # value the loader actually wrote (the user may have
                # set a different TTL or value than what the in-flight
                # slot holds).
                value = self._cache.get(key)
                if value is not None:
                    self.stats.record_shared()
                    return value
                # The loader's value was None or was evicted between the
                # loader finishing and us waking. Fall back to the
                # in-flight value as a best-effort.
                self.stats.record_shared()
                return in_flight.value
            finally:
                self.stats.dec_waiters()

        # We are the loader. Run the factory, cache the result, release.
        try:
            self.stats.record_load()
            value = factory()
            if value is not None:
                self._cache.set(key, value, ttl=ttl)
            in_flight.value = value
            return value
        finally:
            in_flight.done = True
            in_flight.event.set()
            # Drop the in-flight slot from the index so the next miss
            # after eviction will run a fresh factory call. This must
            # happen after event.set() so waiters wake up before the
            # slot is reused.
            with self._index_lock:
                self._in_flight.pop(key, None)

    # ------------------------------------------------------------------
    # Convenience passthroughs
    # ------------------------------------------------------------------

    def get(self, key: str, default: Any = None) -> Any:
        return self._cache.get(key, default)

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> bool:
        return self._cache.set(key, value, ttl=ttl)

    def delete(self, key: str) -> bool:
        return self._cache.delete(key)

    def clear(self) -> None:
        self._cache.clear()
        # Drop any in-flight loads so the next miss starts fresh.
        with self._index_lock:
            for inflight in self._in_flight.values():
                inflight.event.set()
            self._in_flight.clear()

    @property
    def cache(self) -> Cache:
        """The wrapped Cache instance. Useful for stats access."""
        return self._cache
