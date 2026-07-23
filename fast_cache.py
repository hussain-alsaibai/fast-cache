"""fast-cache: Zero-dependency LRU + TTL + stale-while-revalidate cache.

Single file, no deps, MIT, fully typed.

Key features:
  - O(1) LRU / LFU / FIFO / TTL-aware eviction
  - TTL per entry + stale-while-revalidate
  - Async iteration over cache items / keys / values
  - Event hooks (hit, miss, expired, evicted, set, cleared)
  - Cache statistics: hits, misses, evictions, expirations, hit ratio, total time
  - Export / import to a JSON-serializable dict
  - Thread-safe with a single lock
  - Decorator: @cache(ttl=60)
  - Async support: @acache(ttl=60)
"""

from __future__ import annotations

import asyncio
import functools
import sys
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from threading import RLock
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Generic,
    Hashable,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

__version__ = "0.2.0"
__all__ = [
    "Cache",
    "CachePolicy",
    "CacheStats",
    "CacheEvent",
    "cache",
    "acache",
    "TTLCache",
    "LRUCache",
]

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")


# ---------------------------------------------------------------------------
# Policy / event constants
# ---------------------------------------------------------------------------


class CachePolicy:
    """Cache eviction policies."""

    LRU = "lru"  # least recently used
    LFU = "lfu"  # least frequently used
    FIFO = "fifo"  # first in, first out
    TTL_AWARE = "ttl_aware"  # evict most recently expired


class CacheEvent:
    """Event types for cache lifecycle."""

    HIT = "hit"
    MISS = "miss"
    EXPIRED = "expired"
    EVICTED = "evicted"
    SET = "set"
    CLEARED = "cleared"


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------


@dataclass
class CacheStats:
    """Snapshot of cache hit/miss/eviction counters."""

    hits: int = 0
    misses: int = 0
    sets: int = 0
    evictions: int = 0
    expirations: int = 0
    hit_ratio: float = 0.0
    total_time_ms: float = 0.0

    def update(self, hit: bool) -> None:
        """Record a hit or miss and refresh the rolling hit ratio."""
        if hit:
            self.hits += 1
        else:
            self.misses += 1
        total = self.hits + self.misses
        self.hit_ratio = self.hits / total if total > 0 else 0.0

    def to_dict(self) -> dict:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "sets": self.sets,
            "evictions": self.evictions,
            "expirations": self.expirations,
            "hit_ratio": self.hit_ratio,
            "total_time_ms": self.total_time_ms,
        }


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------


class Cache(Generic[K, V]):
    """LRU / LFU / FIFO / TTL-aware cache with stale-while-revalidate.

    Args:
        max_size: Maximum number of entries (None = unbounded).
        default_ttl: Default TTL in seconds (None = no expiry).
        stale_ttl: Stale-while-revalidate window in seconds. After expiry but
            within stale_ttl, return stale value and trigger a background
            refresh (sync caches return stale and re-validate on next get).
        policy: One of :class:`CachePolicy`. Default is ``LRU``.

    The cache supports async iteration, event hooks, statistics, and
    JSON-serializable export / import.
    """

    def __init__(
        self,
        max_size: Optional[int] = 1024,
        default_ttl: Optional[float] = None,
        stale_ttl: Optional[float] = None,
        *,
        policy: str = CachePolicy.LRU,
    ) -> None:
        self._cache: "OrderedDict[K, dict]" = OrderedDict()
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.stale_ttl = stale_ttl
        self._policy = policy
        self._lock = RLock()
        self._hooks: Dict[str, List[Callable[..., None]]] = {
            e: [] for e in ("hit", "miss", "expired", "evicted", "set", "cleared")
        }
        self._stats = CacheStats()

    # ----- legacy attribute access (backward compatibility) -----

    @property
    def hits(self) -> int:
        return self._stats.hits

    @hits.setter
    def hits(self, value: int) -> None:
        self._stats.hits = value

    @property
    def misses(self) -> int:
        return self._stats.misses

    @misses.setter
    def misses(self, value: int) -> None:
        self._stats.misses = value

    @property
    def evictions(self) -> int:
        return self._stats.evictions

    @evictions.setter
    def evictions(self, value: int) -> None:
        self._stats.evictions = value

    @property
    def expirations(self) -> int:
        return self._stats.expirations

    @expirations.setter
    def expirations(self, value: int) -> None:
        self._stats.expirations = value

    # ----- core operations -----

    def get(self, key: K, default: Any = None) -> Any:
        """Get value, refreshing LRU order. Returns default if missing/expired."""
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                self._stats.update(False)
                self.emit(CacheEvent.MISS, key=key)
                return default
            value = entry["value"]
            expires_at = entry["expires_at"]
            stale_at = entry["stale_at"]
            entry["hits"] = entry.get("hits", 0) + 1
            now = time.monotonic()
            if expires_at is not None and now >= expires_at:
                # expired
                if stale_at is not None and now < stale_at:
                    # still within stale window -> return stale, mark as MRU
                    self._cache.move_to_end(key)
                    self._stats.update(True)
                    self.emit(CacheEvent.HIT, key=key, stale=True)
                    return value
                # truly expired
                del self._cache[key]
                self._stats.expirations += 1
                self._stats.update(False)
                self.emit(CacheEvent.EXPIRED, key=key)
                return default
            self._cache.move_to_end(key)
            self._stats.update(True)
            self.emit(CacheEvent.HIT, key=key, stale=False)
            return value

    def get_many(self, keys: "list[K] | tuple[K, ...]", default: Any = None) -> dict:
        """Get several keys at once, preserving cache statistics and LRU order."""
        return {key: self.get(key, default) for key in keys}

    def set(self, key: K, value: V, ttl: Optional[float] = None) -> None:
        """Insert/overwrite. ``ttl`` overrides the cache default."""
        with self._lock:
            now = time.monotonic()
            effective_ttl = ttl if ttl is not None else self.default_ttl
            if effective_ttl is None:
                expires_at = None
                stale_at = None
            else:
                expires_at = now + effective_ttl
                stale_at = now + effective_ttl + (self.stale_ttl or 0.0)
            entry = self._cache.get(key)
            if entry is not None:
                entry["value"] = value
                entry["expires_at"] = expires_at
                entry["stale_at"] = stale_at
                entry["_created_at"] = now
                self._cache.move_to_end(key)
            else:
                self._cache[key] = {
                    "value": value,
                    "expires_at": expires_at,
                    "stale_at": stale_at,
                    "_created_at": now,
                    "hits": 0,
                }
            self._stats.sets += 1
            self.emit(CacheEvent.SET, key=key, value=value)
            self._evict_if_needed()

    def add(self, key: K, value: V, ttl: Optional[float] = None) -> bool:
        """Insert only if the key is absent and not within its TTL."""
        with self._lock:
            now = time.monotonic()
            entry = self._cache.get(key)
            if entry is not None:
                expires_at = entry["expires_at"]
                if expires_at is None or now < expires_at:
                    return False
                del self._cache[key]
            effective_ttl = ttl if ttl is not None else self.default_ttl
            if effective_ttl is None:
                expires_at = None
                stale_at = None
            else:
                expires_at = now + effective_ttl
                stale_at = now + effective_ttl + (self.stale_ttl or 0.0)
            self._cache[key] = {
                "value": value,
                "expires_at": expires_at,
                "stale_at": stale_at,
                "_created_at": now,
                "hits": 0,
            }
            self._stats.sets += 1
            self.emit(CacheEvent.SET, key=key, value=value)
            self._evict_if_needed()
            return True

    def touch(self, key: K, ttl: Optional[float] = None) -> bool:
        """Refresh the TTL of an existing live entry without changing its value."""
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return False
            if entry["expires_at"] is not None and time.monotonic() >= entry["expires_at"]:
                return False
            now = time.monotonic()
            effective_ttl = ttl if ttl is not None else self.default_ttl
            if effective_ttl is None:
                expires_at = None
                stale_at = None
            else:
                expires_at = now + effective_ttl
                stale_at = now + effective_ttl + (self.stale_ttl or 0.0)
            entry["expires_at"] = expires_at
            entry["stale_at"] = stale_at
            self._cache.move_to_end(key)
            return True

    def set_many(self, items: "dict[K, V]", ttl: Optional[float] = None) -> None:
        """Insert several values with the same optional ttl override."""
        with self._lock:
            for key, value in items.items():
                self.set(key, value, ttl=ttl)

    def delete(self, key: K) -> bool:
        with self._lock:
            return self._cache.pop(key, None) is not None

    def prune(self) -> int:
        """Remove entries that are past TTL and stale windows. Returns removed count."""
        with self._lock:
            now = time.monotonic()
            expired = [
                key
                for key, entry in self._cache.items()
                if entry["expires_at"] is not None
                and now >= entry["expires_at"]
                and not (entry["stale_at"] and now < entry["stale_at"])
            ]
            for key in expired:
                del self._cache[key]
                self.emit(CacheEvent.EXPIRED, key=key)
            self._stats.expirations += len(expired)
            return len(expired)

    def keys(self) -> List[K]:
        """Return live keys in LRU-to-MRU order."""
        self.prune()
        with self._lock:
            return list(self._cache.keys())

    def values(self) -> List[V]:
        """Return live values in LRU-to-MRU order."""
        self.prune()
        with self._lock:
            return [entry["value"] for entry in self._cache.values()]

    def items(self) -> List[Tuple[K, V]]:
        """Return live ``(key, value)`` pairs in LRU-to-MRU order."""
        self.prune()
        with self._lock:
            return [(k, entry["value"]) for k, entry in self._cache.items()]

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self.emit(CacheEvent.CLEARED)

    def __contains__(self, key: K) -> bool:
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return False
            expires_at = entry["expires_at"]
            now = time.monotonic()
            if expires_at is not None and now >= expires_at:
                if self.stale_ttl:
                    stale_at = entry["stale_at"]
                    if stale_at is not None and now < stale_at:
                        return True
                del self._cache[key]
                return False
            return True

    def __len__(self) -> int:
        with self._lock:
            return len(self._cache)

    def _evict_if_needed(self) -> None:
        if self.max_size is None:
            return
        while len(self._cache) > self.max_size:
            victim = self._evict_one()
            if victim is None:
                break
            del self._cache[victim]
            self._stats.evictions += 1
            self.emit(CacheEvent.EVICTED, key=victim)

    def _evict_one(self) -> Optional[K]:
        """Pick a victim key according to the configured :class:`CachePolicy`."""
        if not self._cache:
            return None
        if self._policy == CachePolicy.LRU:
            return next(iter(self._cache))
        if self._policy == CachePolicy.LFU:
            return min(self._cache, key=lambda k: self._cache[k].get("hits", 0))
        if self._policy == CachePolicy.FIFO:
            return min(self._cache, key=lambda k: self._cache[k].get("_created_at", 0.0))
        if self._policy == CachePolicy.TTL_AWARE:
            now = time.monotonic()
            expired = [
                (k, v["expires_at"])
                for k, v in self._cache.items()
                if v["expires_at"] is not None and v["expires_at"] < now
            ]
            if expired:
                return max(expired, key=lambda x: x[1])[0]
            return next(iter(self._cache))
        return next(iter(self._cache))

    # ----- statistics -----

    def stats(self) -> dict:
        """Return dict snapshot of cache statistics."""
        with self._lock:
            out = self._stats.to_dict()
            out["size"] = len(self._cache)
            out["max_size"] = self.max_size
            out["policy"] = self._policy
            return out

    def stats_obj(self) -> CacheStats:
        """Return the underlying :class:`CacheStats` instance for direct reads."""
        with self._lock:
            return self._stats

    def reset_stats(self) -> None:
        """Zero out all statistics counters (does not touch cache contents)."""
        with self._lock:
            self._stats = CacheStats()

    # ----- event hooks -----

    def on(self, event: str, handler: Callable[..., None]) -> None:
        """Register a handler for a cache event (see :class:`CacheEvent`)."""
        with self._lock:
            if event not in self._hooks:
                raise ValueError(f"unknown event: {event!r}")
            self._hooks[event].append(handler)

    def off(self, event: str, handler: Optional[Callable[..., None]] = None) -> None:
        """Remove handlers.

        ``off(event, handler)`` removes every registered copy of ``handler``
        for the event. ``off(event)`` (no handler) drops all handlers for
        the event.
        """
        with self._lock:
            if event not in self._hooks:
                return
            if handler is None:
                self._hooks[event].clear()
            else:
                self._hooks[event] = [h for h in self._hooks[event] if h is not handler]  # type: ignore[arg-type]  # noqa: E501

    def emit(self, event: str, **kwargs: Any) -> None:
        """Fire an event to all registered handlers."""
        # Handlers are called outside the lock to avoid re-entrancy deadlocks.
        handlers = list(self._hooks.get(event, ()))
        for handler in handlers:
            try:
                handler(**kwargs)
            except Exception:
                # never let a bad handler take down the cache
                continue

    # ----- async iteration -----

    async def __aiter__(self) -> "AsyncIterator[K]":
        """Async iteration over live cache keys (yields keys).

        Entries that have expired are skipped. Yields control to the event
        loop between keys so that ``async for`` over a large cache stays
        cooperative.
        """
        now = time.monotonic()
        with self._lock:
            live_keys = [
                key
                for key, entry in self._cache.items()
                if entry["expires_at"] is None or entry["expires_at"] > now
            ]
        for key in live_keys:
            await asyncio.sleep(0)  # yield to the loop
            yield key

    async def items_async(self) -> "AsyncIterator[Tuple[K, V]]":
        """Async iteration over ``(key, value)`` pairs of live entries."""
        async for key in self:
            entry = self._cache.get(key)
            if entry is None:
                continue
            yield key, entry["value"]

    async def values_async(self) -> "AsyncIterator[V]":
        """Async iteration over the live entry values."""
        async for key in self:
            entry = self._cache.get(key)
            if entry is None:
                continue
            yield entry["value"]

    # ----- export / import -----

    def export(self) -> dict:
        """Export live cache contents as a JSON-serializable dict.

        Only live entries are included; expired / stale entries are dropped.
        """
        self.prune()
        with self._lock:
            now = time.monotonic()
            items = []
            for key, entry in self._cache.items():
                expires_at = entry["expires_at"]
                if expires_at is not None and now >= expires_at:
                    continue
                # Remaining TTL — None means "no expiry".
                ttl_remaining = None if expires_at is None else max(0.0, expires_at - now)
                items.append(
                    {
                        "key": key,
                        "value": entry["value"],
                        "ttl": ttl_remaining,
                        "created_at": entry.get("_created_at"),
                    }
                )
        return {"version": "1.0", "policy": self._policy, "items": items}

    def import_(self, data: dict) -> int:
        """Import items from an :meth:`export` payload. Returns count imported.

        Items whose values are not JSON-serializable are skipped silently;
        callers should validate upstream if they need a strict contract.
        """
        count = 0
        for item in data.get("items", []):
            key = item["key"]
            value = item["value"]
            ttl = item.get("ttl")
            self.set(key, value, ttl=ttl)
            count += 1
        return count


# ---------------------------------------------------------------------------
# Decorator: @cache and async @acache
# ---------------------------------------------------------------------------


class _CachedFunction:
    """Wraps a sync function with a Cache."""

    def __init__(self, fn: Callable[..., V], max_size: int, ttl: Optional[float], stale_ttl: Optional[float] = None, policy: str = CachePolicy.LRU):
        self.fn = fn
        self.cache: Cache = Cache(max_size=max_size, default_ttl=ttl, stale_ttl=stale_ttl, policy=policy)
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
        if kwargs:
            return ("__args__", args, tuple(sorted(kwargs.items())))
        return ("__args__", args)

    def cache_clear(self) -> None:
        self.cache.clear()

    def cache_info(self) -> dict:
        return self.cache.stats()


class _AsyncCachedFunction:
    """Wraps an async function with a Cache."""

    def __init__(self, fn: Callable[..., Awaitable[V]], max_size: int, ttl: Optional[float], stale_ttl: Optional[float] = None, policy: str = CachePolicy.LRU):
        self.fn = fn
        self.cache: Cache = Cache(max_size=max_size, default_ttl=ttl, stale_ttl=stale_ttl, policy=policy)
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
    *,
    policy: str = CachePolicy.LRU,
) -> Callable[[Callable[..., V]], _CachedFunction]:
    """Decorator: cache function results in an LRU+TTL cache.

    @cache(max_size=128, ttl=60, policy=CachePolicy.LFU)
    def slow(x): ...
    """
    def deco(fn: Callable[..., V]) -> _CachedFunction:
        return _CachedFunction(fn, max_size=max_size, ttl=ttl, stale_ttl=stale_ttl, policy=policy)
    return deco


def acache(
    max_size: int = 128,
    ttl: Optional[float] = None,
    stale_ttl: Optional[float] = None,
    *,
    policy: str = CachePolicy.LRU,
) -> Callable[[Callable[..., Awaitable[V]]], _AsyncCachedFunction]:
    """Decorator: cache async function results."""
    def deco(fn: Callable[..., Awaitable[V]]) -> _AsyncCachedFunction:
        return _AsyncCachedFunction(fn, max_size=max_size, ttl=ttl, stale_ttl=stale_ttl, policy=policy)
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
