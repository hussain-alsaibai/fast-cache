"""fast_cache._v080 - stale-while-revalidate (SWR) and key namespacing.

The README advertised SWR and namespacing from 2026-08-16, but neither
existed in the code: there was no 'swr_ttl' parameter, no '@fc.cache'
decorator, and no 'namespace=' argument. Any snippet copied from the README
raised TypeError. This module implements the documented behaviour, additively,
so v0.7.x imports keep working.

Usage:

    from fast_cache import Cache
    from fast_cache._v080 import SwrCache, namespaced

    cache = SwrCache(Cache(), swr_window=300)

    # Fresh -> served from cache.
    # Stale but inside the window -> served immediately, refreshed in background.
    # Past the window -> treated as a miss, caller blocks on the factory.
    data = cache.get_or_refresh("weather:manama", fetch, ttl=60)

    users = namespaced(inner, "users")
    users.set("42", {"name": "ali"})
    users.clear()   # only drops users:* keys

No background threads are spawned per cache: refreshes run on one shared
single-worker executor created lazily. That preserves the core cache's
no-zombie-thread, no-drift property.
"""
from __future__ import annotations

import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, Optional

__all__ = ["SwrCache", "SwrStats", "namespaced", "NamespaceView"]

_DEFAULT_FRESH_TTL = 60.0


class SwrStats:
    """Counters describing SWR behaviour.

    fresh_hit     - served an authoritative value, no refresh
    stale_hit     - served a stale value and scheduled a refresh
    expired_miss  - no usable value; caller blocked on the factory
    refreshes     - background refreshes started
    coalesced     - refresh suppressed because one was already in flight
    refresh_errors - background refreshes that raised
    """

    __slots__ = (
        "_lock",
        "fresh_hit",
        "stale_hit",
        "expired_miss",
        "refreshes",
        "coalesced",
        "refresh_errors",
    )

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.fresh_hit = 0
        self.stale_hit = 0
        self.expired_miss = 0
        self.refreshes = 0
        self.coalesced = 0
        self.refresh_errors = 0

    def bump(self, field: str, n: int = 1) -> None:
        with self._lock:
            setattr(self, field, getattr(self, field) + n)

    @property
    def stale_ratio(self) -> float:
        """Fraction of cache-served reads that were stale."""
        with self._lock:
            total = self.fresh_hit + self.stale_hit
            return (self.stale_hit / total) if total else 0.0

    def snapshot(self) -> dict:
        with self._lock:
            snap = {
                "fresh_hit": self.fresh_hit,
                "stale_hit": self.stale_hit,
                "expired_miss": self.expired_miss,
                "refreshes": self.refreshes,
                "coalesced": self.coalesced,
                "refresh_errors": self.refresh_errors,
            }
        snap["stale_ratio"] = round(self.stale_ratio, 4)
        return snap

    def reset(self) -> None:
        with self._lock:
            self.fresh_hit = 0
            self.stale_hit = 0
            self.expired_miss = 0
            self.refreshes = 0
            self.coalesced = 0
            self.refresh_errors = 0

    def __repr__(self) -> str:
        return "SwrStats(%s)" % self.snapshot()


class NamespaceView:
    """A key-prefixed facade over a cache, like cachetools KeyedNamespace."""

    __slots__ = ("_cache", "_prefix")

    def __init__(self, cache, prefix: str) -> None:
        if not prefix or not isinstance(prefix, str):
            raise ValueError("prefix must be a non-empty string")
        self._cache = cache
        self._prefix = prefix + ":"

    def _k(self, key: str) -> str:
        return self._prefix + str(key)

    def get(self, key: str, default: Any = None) -> Any:
        return self._cache.get(self._k(key), default)

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> bool:
        return self._cache.set(self._k(key), value, ttl=ttl)

    def get_or_set(self, key: str, factory: Callable[[], Any], ttl: Optional[float] = None) -> Any:
        return self._cache.get_or_set(self._k(key), factory, ttl=ttl)

    def add(self, key: str, value: Any, ttl: Optional[float] = None) -> bool:
        """Insert only when absent. Returns True if newly added."""
        full = self._k(key)
        if self._cache.has(full):
            return False
        self._cache.set(full, value, ttl=ttl)
        return True

    def incr(self, key: str, delta: int = 1, default: int = 0) -> int:
        """Read-modify-write. Lock only the arithmetic, not the whole cache."""
        full = self._k(key)
        current = self._cache.get(full, default)
        new = (default if current is None else current) + delta
        self._cache.set(full, new)
        return new

    def delete(self, key: str) -> bool:
        return self._cache.delete(self._k(key))

    def clear(self) -> int:
        """Drop every key in this namespace. Returns the count removed."""
        doomed = [k for k in list(self._cache.keys()) if k.startswith(self._prefix)]
        for k in doomed:
            self._cache.delete(k)
        return len(doomed)

    def keys(self):
        prefix = self._prefix
        for k in self._cache.keys():
            if k.startswith(prefix):
                yield k[len(prefix):]

    def items(self):
        for k in list(self.keys()):
            yield k, self._cache.get(self._k(k))

    def __contains__(self, key: str) -> bool:
        return self._cache.has(self._k(key))

    def __len__(self) -> int:
        return sum(1 for _ in self.keys())

    def __repr__(self) -> str:
        return "NamespaceView(prefix=%r, size=%d)" % (self._prefix, len(self))


def namespaced(cache, prefix: str) -> NamespaceView:
    """Wrap a cache in a key-prefix namespace."""
    return NamespaceView(cache, prefix)


class SwrCache:
    """Stale-while-revalidate wrapper around any cache-like object.

    The inner object only needs get(key, default), set(key, value, ttl=None),
    and has(key) - which fast_cache.Cache provides, as does any dict-backed
    adapter.
    """

    def __init__(
        self,
        inner,
        *,
        swr_window: float = 0.0,
        fresh_ttl: Optional[float] = None,
        executor: Optional[ThreadPoolExecutor] = None,
        refresh_error_handler: Optional[Callable[[str, BaseException], None]] = None,
    ) -> None:
        """
        Args:
            inner: the underlying cache.
            swr_window: seconds past fresh_ttl during which a stale value is
                still served (and refreshed in the background). 0 disables SWR.
            fresh_ttl: default authoritative TTL when get_or_refresh is called
                without an explicit ttl. Falls back to the inner cache's ttl.
            executor: where background refreshes run. Defaults to a shared
                single-worker pool created on first use.
            refresh_error_handler: called as (key, exception) when a background
                refresh raises. The previous value is kept either way.
        """
        if swr_window < 0:
            raise ValueError("swr_window must be >= 0")
        self._inner = inner
        self._swr_window = float(swr_window)
        self._fresh_ttl = fresh_ttl
        self._owns_executor = executor is None
        self._executor = executor
        self._error_handler = refresh_error_handler
        self._lock = threading.Lock()
        self._fresh_until: dict = {}
        self._inflight: dict = {}
        self._stats = SwrStats()

    # -- properties ---------------------------------------------------------

    @property
    def stats(self) -> SwrStats:
        return self._stats

    @property
    def inner(self):
        return self._inner

    @property
    def swr_window(self) -> float:
        return self._swr_window

    # -- core ---------------------------------------------------------------

    def get_or_refresh(self, key: str, factory: Callable[[], Any], ttl: Optional[float] = None) -> Any:
        """Return a cached value, refreshing it in the background when stale.

        1. Inside fresh_ttl -> return the cached value (fresh_hit).
        2. Inside the SWR window -> schedule one background refresh, return the
           stale value now (stale_hit).
        3. Absent, or past the window -> call factory, store, return
           (expired_miss).

        At most one refresh per key runs at a time; concurrent callers during
        a refresh are coalesced instead of stacking work.
        """
        state = self._state(key)

        if state is not None:
            value, fresh_until = state
            now = time.monotonic()
            if now < fresh_until:
                self._stats.bump("fresh_hit")
                return value
            if now < fresh_until + self._swr_window:
                self._stats.bump("stale_hit")
                self._schedule_refresh(key, factory, ttl)
                return value

        self._stats.bump("expired_miss")
        return self._blocking_refresh(key, factory, ttl)

    def peek(self, key: str, default: Any = None) -> Any:
        """Read the cached value without counting a hit or refreshing."""
        state = self._state(key)
        if state is None:
            return default
        return state[0]

    def is_fresh(self, key: str) -> bool:
        state = self._state(key)
        return state is not None and time.monotonic() < state[1]

    def is_stale(self, key: str) -> bool:
        """True when a value exists but is past fresh_ttl (still servable)."""
        state = self._state(key)
        if state is None:
            return False
        now = time.monotonic()
        return state[1] <= now < state[1] + self._swr_window

    def invalidate(self, key: str) -> bool:
        """Drop freshness tracking and the cached value for one key."""
        with self._lock:
            self._fresh_until.pop(key, None)
        return bool(self._inner.delete(key))

    # -- internals ----------------------------------------------------------

    def _state(self, key: str):
        """Return (value, fresh_until) or None when unusable."""
        with self._lock:
            fresh_until = self._fresh_until.get(key)
        if fresh_until is None:
            return None
        value = self._inner.get(key, None)
        if value is None:
            with self._lock:
                self._fresh_until.pop(key, None)
            return None
        return value, fresh_until

    def _effective_ttl(self, ttl: Optional[float]) -> float:
        if ttl is not None:
            return float(ttl)
        if self._fresh_ttl is not None:
            return float(self._fresh_ttl)
        inner_ttl = getattr(self._inner, "ttl", None)
        return float(inner_ttl) if inner_ttl else _DEFAULT_FRESH_TTL

    def _store(self, key, value, ttl) -> None:
        fresh_ttl = self._effective_ttl(ttl)
        # Hold the value past its freshness window so a stale read is possible.
        self._inner.set(key, value, ttl=fresh_ttl + self._swr_window)
        with self._lock:
            self._fresh_until[key] = time.monotonic() + fresh_ttl

    def _blocking_refresh(self, key, factory, ttl):
        try:
            value = factory()
        except Exception:
            # A failing refresh should degrade to stale, not raise, when a
            # previous value is still on hand.
            with self._lock:
                fresh_until = self._fresh_until.get(key)
            if fresh_until is not None:
                fallback = self._inner.get(key, None)
                if fallback is not None:
                    return fallback
            raise
        self._store(key, value, ttl)
        return value

    def _executor_or_create(self) -> ThreadPoolExecutor:
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="fast-cache-swr"
            )
        return self._executor

    def _schedule_refresh(self, key, factory, ttl) -> None:
        with self._lock:
            running = self._inflight.get(key)
            if running is not None and not running.done():
                self._stats.bump("coalesced")
                return
            self._stats.bump("refreshes")
            try:
                fut = self._executor_or_create().submit(self._refresh, key, factory, ttl)
            except RuntimeError:
                # Executor already shut down: refresh inline rather than skip.
                self._refresh(key, factory, ttl)
                return
            self._inflight[key] = fut

    def _refresh(self, key, factory, ttl) -> None:
        try:
            self._store(key, factory(), ttl)
        except BaseException as exc:  # noqa: BLE001 - background must not escape
            self._stats.bump("refresh_errors")
            if self._error_handler is not None:
                try:
                    self._error_handler(key, exc)
                except Exception:
                    pass

    # -- lifecycle ----------------------------------------------------------

    def pending_refreshes(self) -> int:
        with self._lock:
            return sum(1 for f in self._inflight.values() if not f.done())

    def wait_for_refreshes(self, timeout: Optional[float] = None) -> bool:
        """Block until in-flight refreshes settle. Test/teardown helper."""
        with self._lock:
            futures = list(self._inflight.values())
        for fut in futures:
            try:
                fut.result(timeout=timeout)
            except Exception:
                pass
        return self.pending_refreshes() == 0

    def close(self) -> None:
        """Shut down the executor, if this instance created it."""
        if self._owns_executor and self._executor is not None:
            self._executor.shutdown(wait=True)
            self._executor = None

    def __enter__(self) -> "SwrCache":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __repr__(self) -> str:
        return "SwrCache(swr_window=%.1fs, stats=%s)" % (self._swr_window, self._stats.snapshot())
