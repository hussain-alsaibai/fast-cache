"""
fast-cache — Zero-dependency, high-performance in-memory cache for Python.

Features:
- TTL (time-to-live) with automatic expiration
- LRU (least recently used) eviction
- Size and memory limits
- Thread-safe operations
- Statistics tracking
- Optional persistence
- Pythonic API

Usage:
    from fast_cache import Cache

    cache = Cache(maxsize=1000, ttl=3600)
    cache.set('key', 'value')
    value = cache.get('key')  # 'value'
"""

from __future__ import annotations

import threading
import time
import json
import os
from collections import OrderedDict
from typing import Any, Optional, Callable, Iterator, Awaitable

__version__ = "0.2.0"
__all__ = ["Cache", "CacheStats"]


class CacheStats:
    """Statistics for cache operations."""

    def __init__(self) -> None:
        self.hits: int = 0
        self.misses: int = 0
        self.evictions: int = 0
        self.expirations: int = 0
        self.inserts: int = 0
        self.deletes: int = 0
        self._lock = threading.Lock()

    def hit(self) -> None:
        with self._lock:
            self.hits += 1

    def miss(self) -> None:
        with self._lock:
            self.misses += 1

    def eviction(self) -> None:
        with self._lock:
            self.evictions += 1

    def expiration(self) -> None:
        with self._lock:
            self.expirations += 1

    def insert(self) -> None:
        with self._lock:
            self.inserts += 1

    def delete(self) -> None:
        with self._lock:
            self.deletes += 1

    @property
    def total_requests(self) -> int:
        with self._lock:
            return self.hits + self.misses

    @property
    def hit_rate(self) -> float:
        with self._lock:
            total = self.hits + self.misses
            return self.hits / total if total > 0 else 0.0

    def to_dict(self) -> dict:
        with self._lock:
            return {
                "hits": self.hits,
                "misses": self.misses,
                "evictions": self.evictions,
                "expirations": self.expirations,
                "inserts": self.inserts,
                "deletes": self.deletes,
                "hit_rate": round(self.hit_rate, 4),
            }

    def __repr__(self) -> str:
        return f"CacheStats(hits={self.hits}, misses={self.misses}, hit_rate={self.hit_rate:.2%})"


class _CacheEntry:
    """Internal entry holding value, expiry, and metadata."""

    __slots__ = ("value", "expires_at", "size", "access_count")

    def __init__(self, value: Any, ttl: Optional[float], size: int):
        self.value = value
        self.expires_at = time.time() + ttl if ttl else None
        self.size = size
        self.access_count = 0

    @property
    def is_expired(self) -> bool:
        return self.expires_at is not None and time.time() > self.expires_at

    def touch(self) -> None:
        self.access_count += 1


class Cache:
    """
    High-performance in-memory cache with TTL, LRU, and size limits.

    Args:
        maxsize: Maximum number of entries. None = unlimited.
        ttl: Default time-to-live in seconds. None = no expiration.
        maxmemory: Maximum total value size in bytes. None = unlimited.
        evict_callback: Optional callback when an entry is evicted.
        thread_safe: Enable internal locking (default True).
    """

    def __init__(
        self,
        maxsize: Optional[int] = None,
        ttl: Optional[float] = None,
        maxmemory: Optional[int] = None,
        evict_callback: Optional[Callable[[str, Any], None]] = None,
        thread_safe: bool = True,
    ):
        self.maxsize = maxsize
        self.ttl = ttl
        self.maxmemory = maxmemory
        self._evict_callback = evict_callback

        # OrderedDict for LRU: most-recently-used at the end
        self._data: OrderedDict[str, _CacheEntry] = OrderedDict()
        self._total_size: int = 0
        self._stats = CacheStats()

        if thread_safe:
            self._lock = threading.RLock()
        else:
            self._lock = _NullLock()

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def get(self, key: str, default: Any = None) -> Any:
        """Get value by key. Returns default if missing or expired."""
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                self._stats.miss()
                return default

            if entry.is_expired:
                self._remove(key)
                self._stats.expiration()
                self._stats.miss()
                return default

            # LRU: move to end (most recently used)
            self._data.move_to_end(key)
            entry.touch()
            self._stats.hit()
            return entry.value

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None,
    ) -> bool:
        """Set key → value. Returns True if inserted, False if updated."""
        size = self._estimate_size(value)
        ttl = ttl if ttl is not None else self.ttl

        with self._lock:
            existed = key in self._data
            if existed:
                self._total_size -= self._data[key].size

            entry = _CacheEntry(value, ttl, size)
            self._data[key] = entry
            self._total_size += size

            # Move to end for LRU
            self._data.move_to_end(key)

            # Evict if over limits
            self._evict_if_needed()

            if not existed:
                self._stats.insert()
            return not existed

    def delete(self, key: str) -> bool:
        """Delete a key. Returns True if existed."""
        with self._lock:
            if key in self._data:
                self._remove(key)
                self._stats.delete()
                return True
            return False

    def pop(self, key: str, default: Any = None) -> Any:
        """Get and delete a key."""
        with self._lock:
            entry = self._data.get(key)
            if entry is None or entry.is_expired:
                self._stats.miss()
                return default
            value = entry.value
            self._remove(key)
            self._stats.delete()
            return value

    def has(self, key: str) -> bool:
        """Check if key exists and is not expired (without updating LRU)."""
        with self._lock:
            entry = self._data.get(key)
            if entry is None or entry.is_expired:
                return False
            return True

    def get_or_set(self, key: str, factory: Callable[[], Any], ttl: Optional[float] = None) -> Any:
        """Get value, or set from factory if missing."""
        value = self.get(key)
        if value is not None:
            return value
        value = factory()
        self.set(key, value, ttl=ttl)
        return value

    def get_or_set_async(self, key: str, afactory: Callable[[], Awaitable[Any]], ttl: Optional[float] = None) -> Awaitable[Any]:
        """Get value, or set from an async factory if missing.

        Always returns an awaitable (even when the value is already cached),
        so callers can uniformly ``await`` it. Built for AI agents that cache
        LLM responses / embeddings without blocking the event loop when the
        value isn't cached yet.

        .. code-block:: python

            resp = await cache.get_or_set_async(
                f"llm:{hashlib.sha256(prompt.encode()).hexdigest()}",
                lambda: llm.acomplete(prompt),
                ttl=3600,
            )
        """
        value = self.get(key)
        if value is not None:
            return self._immediate(value)
        return self._set_after_await(key, afactory(), ttl)

    async def _immediate(self, value: Any) -> Any:
        return value

    async def _set_after_await(self, key: str, coro, ttl: Optional[float]) -> Any:
        value = await coro
        self.set(key, value, ttl=ttl)
        return value

    async def populate_missing(self, keys: list[str],
                               afactory: Callable[[str], Awaitable[Any]],
                               ttl: Optional[float] = None) -> dict[str, Any]:
        """Fetch missing keys in parallel via an async factory.

        Returns {key: value} for every requested key, populating the cache
        for any that were absent. Uses ``asyncio.gather`` so agent workloads
        (embedding batches, LLM prefetch) fan out instead of serializing.
        """
        import asyncio
        got = self.mget(keys)
        missing = [k for k in keys if k not in got]
        values = await asyncio.gather(*[afactory(k) for k in missing])
        for k, v in zip(missing, values):
            self.set(k, v, ttl=ttl)
            got[k] = v
        return got

    # ------------------------------------------------------------------
    # Batch operations
    # ------------------------------------------------------------------

    def mget(self, keys: list[str]) -> dict[str, Any]:
        """Get multiple keys. Returns {key: value} for existing keys."""
        result: dict[str, Any] = {}
        with self._lock:
            for key in keys:
                entry = self._data.get(key)
                if entry and not entry.is_expired:
                    self._data.move_to_end(key)
                    entry.touch()
                    result[key] = entry.value
                    self._stats.hit()
                else:
                    self._stats.miss()
        return result

    def mset(self, items: dict[str, Any], ttl: Optional[float] = None) -> int:
        """Set multiple keys. Returns count of new insertions."""
        count = 0
        for key, value in items.items():
            if self.set(key, value, ttl=ttl):
                count += 1
        return count

    def mdelete(self, keys: list[str]) -> int:
        """Delete multiple keys. Returns count deleted."""
        count = 0
        with self._lock:
            for key in keys:
                if key in self._data:
                    self._remove(key)
                    count += 1
        self._stats.delete()  # approximate
        return count

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    def expire(self, key: str, ttl: float) -> bool:
        """Update TTL for an existing key. Returns True if key exists."""
        with self._lock:
            entry = self._data.get(key)
            if entry is None or entry.is_expired:
                return False
            entry.expires_at = time.time() + ttl
            return True

    def ttl_remaining(self, key: str) -> Optional[float]:
        """Return remaining TTL in seconds, or None if no TTL."""
        with self._lock:
            entry = self._data.get(key)
            if entry is None or entry.is_expired:
                return None
            if entry.expires_at is None:
                return None
            return max(0.0, entry.expires_at - time.time())

    def clear(self) -> int:
        """Clear all entries. Returns count removed."""
        with self._lock:
            count = len(self._data)
            self._data.clear()
            self._total_size = 0
            return count

    def cleanup(self) -> int:
        """Remove all expired entries. Returns count removed."""
        with self._lock:
            expired = [k for k, e in self._data.items() if e.is_expired]
            for key in expired:
                self._remove(key)
            self._stats.expiration()  # approximate
            return len(expired)

    def keys(self) -> Iterator[str]:
        """Iterate over non-expired keys."""
        with self._lock:
            # Snapshot to avoid mutation during iteration
            snapshot = list(self._data.items())
        for key, entry in snapshot:
            if not entry.is_expired:
                yield key

    def values(self) -> Iterator[Any]:
        """Iterate over non-expired values."""
        with self._lock:
            snapshot = list(self._data.items())
        for key, entry in snapshot:
            if not entry.is_expired:
                yield entry.value

    def items(self) -> Iterator[tuple[str, Any]]:
        """Iterate over (key, value) for non-expired entries."""
        with self._lock:
            snapshot = list(self._data.items())
        for key, entry in snapshot:
            if not entry.is_expired:
                yield (key, entry.value)

    # ------------------------------------------------------------------
    # Info
    # ------------------------------------------------------------------

    @property
    def stats(self) -> CacheStats:
        return self._stats

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._data)

    @property
    def memory_usage(self) -> int:
        with self._lock:
            return self._total_size

    def info(self) -> dict:
        """Return cache statistics and metadata."""
        with self._lock:
            return {
                "entries": len(self._data),
                "maxsize": self.maxsize,
                "memory_usage": self._total_size,
                "maxmemory": self.maxmemory,
                "default_ttl": self.ttl,
                **self._stats.to_dict(),
            }

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)

    def __contains__(self, key: str) -> bool:
        return self.has(key)

    def __repr__(self) -> str:
        return f"Cache(entries={self.size}, maxsize={self.maxsize}, hit_rate={self._stats.hit_rate:.1%})"

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, path: str) -> int:
        """Save cache to JSON file. Returns bytes written."""
        with self._lock:
            data = {}
            for key, entry in self._data.items():
                if entry.is_expired:
                    continue
                data[key] = {
                    "value": entry.value,
                    "ttl": entry.expires_at - time.time() if entry.expires_at else None,
                    "size": entry.size,
                }
            payload = json.dumps(data, default=str, indent=2)
            with open(path, "w") as f:
                f.write(payload)
            return len(payload.encode())

    def load(self, path: str) -> int:
        """Load cache from JSON file. Returns count loaded."""
        with self._lock:
            if not os.path.exists(path):
                return 0
            with open(path, "r") as f:
                data = json.load(f)
            count = 0
            for key, item in data.items():
                ttl = item.get("ttl")
                if ttl is not None and ttl <= 0:
                    continue
                self.set(key, item["value"], ttl=ttl)
                count += 1
            return count

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _estimate_size(self, value: Any) -> int:
        """Estimate memory size of a value (approximate)."""
        try:
            return len(json.dumps(value, default=str).encode())
        except (TypeError, ValueError):
            return 64  # fallback

    def _remove(self, key: str) -> None:
        """Remove key without locking (caller must hold lock)."""
        entry = self._data.pop(key, None)
        if entry:
            self._total_size -= entry.size
            if self._evict_callback:
                try:
                    self._evict_callback(key, entry.value)
                except Exception:
                    pass

    def _evict_if_needed(self) -> None:
        """Evict entries if over maxsize or maxmemory."""
        # Evict expired first
        expired = [k for k, e in self._data.items() if e.is_expired]
        for key in expired:
            self._remove(key)
            self._stats.expiration()

        # Evict LRU if over maxsize
        if self.maxsize is not None:
            while len(self._data) > self.maxsize:
                key, entry = self._data.popitem(last=False)
                self._total_size -= entry.size
                self._stats.eviction()
                if self._evict_callback:
                    try:
                        self._evict_callback(key, entry.value)
                    except Exception:
                        pass

        # Evict LRU if over maxmemory
        if self.maxmemory is not None:
            while self._total_size > self.maxmemory and self._data:
                key, entry = self._data.popitem(last=False)
                self._total_size -= entry.size
                self._stats.eviction()
                if self._evict_callback:
                    try:
                        self._evict_callback(key, entry.value)
                    except Exception:
                        pass


class _NullLock:
    """No-op lock for thread_safe=False."""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass
