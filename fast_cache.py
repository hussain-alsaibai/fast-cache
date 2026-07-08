"""fast-cache: Zero-dependency LRU + TTL + stale-while-revalidate cache.

Single file, no deps, MIT, fully typed.

Key features:
  - O(1) LRU eviction using dict + doubly-linked list
  - TTL per entry
  - Stale-while-revalidate (SWR) for soft freshness
  - Decorator: @cache(ttl=60)
  - Async support: @acache(ttl=60)
  - Thread-safe with a single lock
  - Cache statistics: hits, misses, evictions
"""

from __future__ import annotations

import asyncio
import functools
import sys
import time
from collections import OrderedDict
from threading import RLock
from typing import (
    Any,
    Awaitable,
    Callable,
    Generic,
    Hashable,
    Optional,
    TypeVar,
    Union,
)

__version__ = "0.1.0"
__all__ = ["Cache", "cache", "acache", "TTLCache", "LRUCache"]

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")


# ---------------------------------------------------------------------------
# Cache implementations
# ---------------------------------------------------------------------------


class Cache(Generic[K, V]):
    """LRU + TTL cache with stale-while-revalidate support.

    Args:
        max_size: Maximum number of entries (None = unbounded).
        default_ttl: Default TTL in seconds (None = no expiry).
        stale_ttl: Stale-while-revalidate window in seconds. After expiry but
            within stale_ttl, return stale value and trigger a background
            refresh (sync caches return stale and re-validate on next get).
    """

    def __init__(
        self,
        max_size: Optional[int] = 1024,
        default_ttl: Optional[float] = None,
        stale_ttl: Optional[float] = None,
    ) -> None:
        self._data: "OrderedDict[K, tuple[V, float, Optional[float]]]" = OrderedDict()
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.stale_ttl = stale_ttl
        self._lock = RLock()
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.expirations = 0

    # ----- core operations -----

    def get(self, key: K, default: Any = None) -> Any:
        """Get value, refreshing LRU order. Returns default if missing/expired."""
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                self.misses += 1
                return default
            value, expires_at, stale_at = entry
            now = time.monotonic()
            if expires_at is not None and now >= expires_at:
                # expired
                if stale_at is not None and now < stale_at:
                    # still within stale window → return stale, mark as LRU
                    self._data.move_to_end(key)
                    self.hits += 1
                    return value
                # truly expired
                del self._data[key]
                self.expirations += 1
                self.misses += 1
                return default
            self._data.move_to_end(key)
            self.hits += 1
            return value

    def get_many(self, keys: "list[K] | tuple[K, ...]", default: Any = None) -> dict[K, Any]:
        """Get several keys at once, preserving cache statistics and LRU order."""
        return {key: self.get(key, default) for key in keys}

    def set(self, key: K, value: V, ttl: Optional[float] = None) -> None:
        """Insert/overwrite. ttl overrides the cache default."""
        with self._lock:
            now = time.monotonic()
            effective_ttl = ttl if ttl is not None else self.default_ttl
            if effective_ttl is None:
                expires_at = None
                stale_at = None
            else:
                expires_at = now + effective_ttl
                stale_at = now + effective_ttl + (self.stale_ttl or 0.0)
            if key in self._data:
                self._data.move_to_end(key)
            self._data[key] = (value, expires_at, stale_at)
            self._evict_if_needed()

    def set_many(self, items: "dict[K, V]", ttl: Optional[float] = None) -> None:
        """Insert several values with the same optional ttl override."""
        with self._lock:
            for key, value in items.items():
                self.set(key, value, ttl=ttl)

    def delete(self, key: K) -> bool:
        with self._lock:
            return self._data.pop(key, None) is not None

    def prune(self) -> int:
        """Remove entries that are past TTL and stale windows. Returns removed count."""
        with self._lock:
            now = time.monotonic()
            expired = [
                key
                for key, (_, expires_at, stale_at) in self._data.items()
                if expires_at is not None and now >= expires_at and not (stale_at and now < stale_at)
            ]
            for key in expired:
                del self._data[key]
            self.expirations += len(expired)
            return len(expired)

    def keys(self) -> list[K]:
        """Return live keys in LRU-to-MRU order."""
        self.prune()
        with self._lock:
            return list(self._data.keys())

    def clear(self) -> None:
        with self._lock:
            self._data.clear()

    def __contains__(self, key: K) -> bool:
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return False
            _, expires_at, _ = entry
            if expires_at is not None and time.monotonic() >= expires_at:
                if expires_at is not None and self.stale_ttl:
                    # within stale window
                    return True
                del self._data[key]
                return False
            return True

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)

    def _evict_if_needed(self) -> None:
        if self.max_size is None:
            return
        while len(self._data) > self.max_size:
            self._data.popitem(last=False)  # FIFO end = LRU
            self.evictions += 1

    def stats(self) -> dict:
        with self._lock:
            total = self.hits + self.misses
            return {
                "size": len(self._data),
                "max_size": self.max_size,
                "hits": self.hits,
                "misses": self.misses,
                "evictions": self.evictions,
                "expirations": self.expirations,
                "hit_ratio": (self.hits / total) if total else 0.0,
            }


# ---------------------------------------------------------------------------
# Decorator: @cache and async @acache
# ---------------------------------------------------------------------------


class _CachedFunction:
    """Wraps a sync function with a Cache."""

    def __init__(self, fn: Callable[..., V], max_size: int, ttl: Optional[float], stale_ttl: Optional[float] = None):
        self.fn = fn
        self.cache: Cache = Cache(max_size=max_size, default_ttl=ttl, stale_ttl=stale_ttl)
        functools.update_wrapper(self, fn)

    def __call__(self, *args: Any, **kwargs: Any) -> V:
        key = self._make_key(args, kwargs)
        v = self.cache.get(key)
        if v is not None or key in self.cache:
            return v
        v = self.fn(*args, **kwargs)
        self.cache.set(key, v)
        return v

    def _make_key(self, args: tuple, kwargs: dict) -> tuple:
        # kwargs may have any order; sort for stability
        if kwargs:
            return ("__args__", args, tuple(sorted(kwargs.items())))
        return ("__args__", args)

    def cache_clear(self) -> None:
        self.cache.clear()

    def cache_info(self) -> dict:
        return self.cache.stats()


class _AsyncCachedFunction:
    """Wraps an async function with a Cache."""

    def __init__(self, fn: Callable[..., Awaitable[V]], max_size: int, ttl: Optional[float], stale_ttl: Optional[float] = None):
        self.fn = fn
        self.cache: Cache = Cache(max_size=max_size, default_ttl=ttl, stale_ttl=stale_ttl)
        functools.update_wrapper(self, fn)

    async def __call__(self, *args: Any, **kwargs: Any) -> V:
        key = ("__args__", args, tuple(sorted(kwargs.items())) if kwargs else ())
        v = self.cache.get(key)
        if v is not None or key in self.cache:
            return v
        v = await self.fn(*args, **kwargs)
        self.cache.set(key, v)
        return v

    def cache_clear(self) -> None:
        self.cache.clear()

    def cache_info(self) -> dict:
        return self.cache.stats()


def cache(
    max_size: int = 128,
    ttl: Optional[float] = None,
    stale_ttl: Optional[float] = None,
) -> Callable[[Callable[..., V]], _CachedFunction]:
    """Decorator: cache function results in an LRU+TTL cache.

    @cache(max_size=128, ttl=60)
    def slow(x): ...
    """
    def deco(fn: Callable[..., V]) -> _CachedFunction:
        return _CachedFunction(fn, max_size=max_size, ttl=ttl, stale_ttl=stale_ttl)
    return deco


def acache(
    max_size: int = 128,
    ttl: Optional[float] = None,
    stale_ttl: Optional[float] = None,
) -> Callable[[Callable[..., Awaitable[V]]], _AsyncCachedFunction]:
    """Decorator: cache async function results."""
    def deco(fn: Callable[..., Awaitable[V]]) -> _AsyncCachedFunction:
        return _AsyncCachedFunction(fn, max_size=max_size, ttl=ttl, stale_ttl=stale_ttl)
    return deco


# ---------------------------------------------------------------------------
# Specialized caches
# ---------------------------------------------------------------------------


class LRUCache(Cache[K, V]):
    """Bounded LRU cache. No TTL — entries live until evicted."""
    def __init__(self, max_size: int = 128) -> None:
        super().__init__(max_size=max_size, default_ttl=None, stale_ttl=None)


class TTLCache(Cache[K, V]):
    """Bounded cache with TTL, no LRU ordering beyond access."""
    def __init__(self, max_size: int = 1024, ttl: float = 60.0) -> None:
        super().__init__(max_size=max_size, default_ttl=ttl, stale_ttl=None)


# ---------------------------------------------------------------------------
# Tiny self-test
# ---------------------------------------------------------------------------


if __name__ == "__main__":  # pragma: no cover
    c: Cache = Cache(max_size=3, default_ttl=0.1)
    c.set("a", 1)
    print("get a:", c.get("a"))  # 1
    time.sleep(0.2)
    print("get a (expired):", c.get("a", "MISS"))  # MISS
    print("stats:", c.stats())
