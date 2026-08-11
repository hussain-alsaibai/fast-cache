"""
fast-cache v0.2.0 — Zero-Dependency, High-Performance In-Memory Cache for Python

Features:
- TTL (time-to-live) with automatic expiration
- LRU (least recently used) eviction
- Size and memory limits
- Thread-safe operations
- Statistics tracking
- Optional persistence (save/load JSON)
- Batch operations (mget, mset, mdelete)
- Tags for grouped invalidation
- Async decorators for aiohttp / asyncio-based LLM calls
- Zero dependencies

Usage:
    from fast_cache import Cache

    cache = Cache(maxsize=1000, ttl=3600)
    cache.set('key', 'value')
    value = cache.get('key')  # 'value'

    # With tags (group invalidation)
    cache.set('user:42:profile', data, tags=['user:42', 'profiles'])
    cache.invalidate_tag('user:42')  # removes all 'user:42:*' entries

    # Async decorator
    @cache.async_cache(ttl=300)
    async def fetch_embedding(text: str) -> list[float]:
        return await embedding_model.encode(text)
"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import threading
import time
import json
import os
from collections import OrderedDict
from typing import Any, Callable, Optional, TypeVar, Union

__version__ = "0.2.0"
__all__ = ["Cache", "CacheStats"]

T = TypeVar("T")

# ─── CacheStats ────────────────────────────────────────────────────────────────

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


# ─── CacheEntry ───────────────────────────────────────────────────────────────

class _CacheEntry:
    """Internal entry holding value, expiry, metadata, and tags."""

    __slots__ = ("value", "expires_at", "size", "access_count", "tags")

    def __init__(
        self,
        value: Any,
        ttl: Optional[float],
        size: int,
        tags: Optional[list[str]] = None,
    ):
        self.value = value
        self.expires_at = time.time() + ttl if ttl is not None else None
        self.size = size
        self.access_count = 0
        self.tags: set[str] = set(tags or [])

    @property
    def is_expired(self) -> bool:
        return self.expires_at is not None and time.time() > self.expires_at

    def touch(self) -> None:
        self.access_count += 1

    def add_tag(self, tag: str) -> None:
        self.tags.add(tag)

    def remove_tag(self, tag: str) -> None:
        self.tags.discard(tag)

    def matches_tag(self, tag: str) -> bool:
        """Return True if any of this entry's tags start with the given tag prefix."""
        return any(t == tag or t.startswith(tag + ":") for t in self.tags)


# ─── Cache ────────────────────────────────────────────────────────────────────

class Cache:
    """
    High-performance in-memory cache with TTL, LRU, size limits, and tag-based
    grouped invalidation.

    Args:
        maxsize: Maximum number of entries. None = unlimited.
        ttl: Default time-to-live in seconds. None = no expiration.
        maxmemory: Maximum total value size in bytes. None = unlimited.
        evict_callback: Optional callback(key, value) when an entry is evicted.
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

        self._data: OrderedDict[str, _CacheEntry] = OrderedDict()
        self._total_size: int = 0
        self._stats = CacheStats()

        # Tag index: tag -> set of keys
        self._tag_index: dict[str, set[str]] = {}

        if thread_safe:
            self._lock = threading.RLock()
        else:
            self._lock = _NullLock()

    # ── Core operations ─────────────────────────────────────────────────────────

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

            self._data.move_to_end(key)
            entry.touch()
            self._stats.hit()
            return entry.value

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None,
        tags: Optional[list[str]] = None,
    ) -> bool:
        """Set key → value. Returns True if inserted, False if updated."""
        size = self._estimate_size(value)
        ttl = ttl if ttl is not None else self.ttl

        with self._lock:
            existed = key in self._data
            if existed:
                old_entry = self._data[key]
                self._total_size -= old_entry.size
                # Remove old tags from index
                for tag in old_entry.tags:
                    self._tag_index.get(tag, set()).discard(key)
                    if not self._tag_index[tag]:
                        del self._tag_index[tag]

            entry = _CacheEntry(value, ttl, size, tags=tags)
            self._data[key] = entry
            self._total_size += size
            self._data.move_to_end(key)

            # Index tags
            if tags:
                for tag in tags:
                    if tag not in self._tag_index:
                        self._tag_index[tag] = set()
                    self._tag_index[tag].add(key)

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
            return entry is not None and not entry.is_expired

    def get_or_set(
        self,
        key: str,
        factory: Callable[[], Any],
        ttl: Optional[float] = None,
        tags: Optional[list[str]] = None,
    ) -> Any:
        """Get value, or set from factory if missing."""
        value = self.get(key)
        if value is not None:
            return value
        value = factory()
        self.set(key, value, ttl=ttl, tags=tags)
        return value

    # ── Tag-based invalidation ─────────────────────────────────────────────────

    def add_tags(self, key: str, tags: list[str]) -> bool:
        """Add tags to an existing key. Returns True if key exists."""
        with self._lock:
            entry = self._data.get(key)
            if entry is None or entry.is_expired:
                return False
            for tag in tags:
                entry.add_tag(tag)
                if tag not in self._tag_index:
                    self._tag_index[tag] = set()
                self._tag_index[tag].add(key)
            return True

    def remove_tags(self, key: str, tags: list[str]) -> bool:
        """Remove tags from an existing key. Returns True if key exists."""
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return False
            for tag in tags:
                entry.remove_tag(tag)
                if tag in self._tag_index:
                    self._tag_index[tag].discard(key)
                    if not self._tag_index[tag]:
                        del self._tag_index[tag]
            return True

    def invalidate_tag(self, tag: str) -> int:
        """
        Remove all entries that have the given tag (exact match or prefix).
        Returns count of removed entries.
        """
        with self._lock:
            removed = 0
            # Exact match
            keys = set(self._tag_index.pop(tag, set()))
            # Prefix match: "user:42" matches "user:42:profile"
            prefix = tag + ":"
            to_remove_prefix: set[str] = set()
            for t, ks in list(self._tag_index.items()):
                if t.startswith(prefix):
                    keys.update(ks)
                    to_remove_prefix.add(t)
            for t in to_remove_prefix:
                del self._tag_index[t]

            for key in keys:
                if key in self._data:
                    self._remove(key)
                    removed += 1
            return removed

    def invalidate_matching(self, pattern: str) -> int:
        """
        Remove all entries whose keys match a pattern.
        Supports glob-style: "*" matches anything within a segment.
        Examples: "user:*", "embed:*:v1", "session:*"
        Returns count of removed entries.
        """
        with self._lock:
            removed = 0
            # Simple glob: split by "*" and check each segment
            segments = pattern.split("*")
            keys_to_remove = []

            for key in list(self._data.keys()):
                if self._key_matches_glob(key, segments):
                    keys_to_remove.append(key)

            for key in keys_to_remove:
                self._remove(key)
                removed += 1
            return removed

    def _key_matches_glob(self, key: str, segments: list[str]) -> bool:
        """Check if key matches glob segments."""
        if len(segments) == 1:
            return key == segments[0]
        pos = 0
        for i, seg in enumerate(segments):
            if i == 0:
                if not key.startswith(seg):
                    return False
                pos = len(seg)
            elif i == len(segments) - 1:
                if not key[pos:].endswith(seg) if seg else True:
                    return False
            else:
                next_pos = key.find(seg, pos)
                if next_pos == -1:
                    return False
                pos = next_pos + len(seg)
        return True

    def tags_for(self, key: str) -> list[str]:
        """Return tags for a key (empty list if not found)."""
        with self._lock:
            entry = self._data.get(key)
            if entry is None or entry.is_expired:
                return []
            return list(entry.tags)

    def all_tags(self) -> list[str]:
        """Return all unique tags in the cache."""
        with self._lock:
            return list(self._tag_index.keys())

    # ── Batch operations ───────────────────────────────────────────────────────

    def mget(self, keys: list[str]) -> dict[str, Any]:
        """Get multiple keys. Returns {key: value} for existing non-expired keys."""
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

    def mset(
        self,
        items: dict[str, Any],
        ttl: Optional[float] = None,
        tags: Optional[list[str]] = None,
    ) -> int:
        """Set multiple keys. Returns count of new insertions (not updates)."""
        count = 0
        for key, value in items.items():
            if self.set(key, value, ttl=ttl, tags=tags):
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
        return count

    # ── Maintenance ────────────────────────────────────────────────────────────

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
            self._tag_index.clear()
            return count

    def cleanup(self) -> int:
        """Remove all expired entries. Returns count removed."""
        with self._lock:
            expired = [k for k, e in self._data.items() if e.is_expired]
            for key in expired:
                self._remove(key)
            return len(expired)

    def keys(self) -> list[str]:
        """Return list of non-expired keys."""
        with self._lock:
            return [k for k, e in self._data.items() if not e.is_expired]

    def values(self) -> list[Any]:
        """Return list of non-expired values."""
        with self._lock:
            return [e.value for e in self._data.values() if not e.is_expired]

    def items(self) -> list[tuple[str, Any]]:
        """Return list of (key, value) for non-expired entries."""
        with self._lock:
            return [(k, e.value) for k, e in self._data.items() if not e.is_expired]

    # ── Info ─────────────────────────────────────────────────────────────────

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
                "version": __version__,
                "entries": len(self._data),
                "maxsize": self.maxsize,
                "memory_usage": self._total_size,
                "maxmemory": self.maxmemory,
                "default_ttl": self.ttl,
                "tags": len(self._tag_index),
                **self._stats.to_dict(),
            }

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)

    def __contains__(self, key: str) -> bool:
        return self.has(key)

    def __repr__(self) -> str:
        return f"Cache(entries={self.size}, maxsize={self.maxsize}, hit_rate={self._stats.hit_rate:.1%})"

    # ── Persistence ──────────────────────────────────────────────────────────

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
                    "tags": list(entry.tags),
                }
            payload = json.dumps(data, default=str, indent=2)
            encoded = payload.encode("utf-8")
            with open(path, "wb") as f:
                f.write(encoded)
            return len(encoded)

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
                tags = item.get("tags", [])
                self.set(key, item["value"], ttl=ttl, tags=tags)
                count += 1
            return count

    # ── Async cache decorator ─────────────────────────────────────────────────

    def async_cache(
        self,
        ttl: Optional[float] = None,
        key_fn: Optional[Callable[..., str]] = None,
    ) -> Callable:
        """
        Decorator for async functions that caches results.

        Usage:
            cache = Cache(ttl=300)

            @cache.async_cache(ttl=600)
            async def fetch_embedding(text: str) -> list[float]:
                return await model.encode(text)

            @cache.async_cache(key_fn=lambda text, model="default": f"{model}:{hash(text)}")
            async def call_llm(prompt: str, model: str = "gpt-4o") -> str:
                ...

        The default key is the function name + a hash of positional args.
        For LLM calls, include the prompt hash in key_fn for safe deduplication.
        """
        def decorator(fn: Callable) -> Callable:
            @functools.wraps(fn)
            async def wrapper(*args, **kwargs):
                # Build cache key
                if key_fn:
                    cache_key = key_fn(*args, **kwargs)
                else:
                    # Default: fn_name + hash of str(args+kwargs)
                    sig = f"{fn.__module__}.{fn.__qualname__}:{str(args)}:{str(kwargs)}"
                    cache_key = hashlib.sha1(sig.encode()).hexdigest()

                # Check cache
                result = self.get(cache_key)
                if result is not None:
                    return result

                # Call function
                value = await fn(*args, **kwargs)
                self.set(cache_key, value, ttl=ttl)
                return value

            return wrapper
        return decorator

    # ── Internal ──────────────────────────────────────────────────────────────

    def _estimate_size(self, value: Any) -> int:
        """Estimate memory size of a value (approximate)."""
        try:
            return len(json.dumps(value, default=str).encode("utf-8"))
        except (TypeError, ValueError):
            return 64

    def _remove(self, key: str) -> None:
        """Remove key without locking (caller must hold lock)."""
        entry = self._data.pop(key, None)
        if entry:
            self._total_size -= entry.size
            # Remove from tag index
            for tag in entry.tags:
                if tag in self._tag_index:
                    self._tag_index[tag].discard(key)
                    if not self._tag_index[tag]:
                        del self._tag_index[tag]
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
                for tag in entry.tags:
                    if tag in self._tag_index:
                        self._tag_index[tag].discard(key)
                        if not self._tag_index[tag]:
                            del self._tag_index[tag]
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
                for tag in entry.tags:
                    if tag in self._tag_index:
                        self._tag_index[tag].discard(key)
                        if not self._tag_index[tag]:
                            del self._tag_index[tag]
                if self._evict_callback:
                    try:
                        self._evict_callback(key, entry.value)
                    except Exception:
                        pass


# ─── NullLock ─────────────────────────────────────────────────────────────────

class _NullLock:
    """No-op lock for thread_safe=False."""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass
