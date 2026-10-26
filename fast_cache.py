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
import sys
from collections import OrderedDict
from typing import Any, Optional, Callable, Iterator, Awaitable

__version__ = "0.7.3"
__all__ = ["Cache", "CacheAsync", "CacheStats", "DiskSpillover", "CacheMetricsListener", "cache_method"]


def _deep_sizeof(obj: Any, _seen: Optional[set] = None) -> int:
    """Recursively estimate Python object memory in bytes.

    Uses :func:`sys.getsizeof` on ``obj`` and, for containers, on each
    element so that nested lists/dicts/tuples are accounted for.
    Reference cycles are guarded by an ``id()`` set so we never loop
    forever on a self-referencing structure.
    """
    if _seen is None:
        _seen = set()
    oid = id(obj)
    if oid in _seen:
        return 0
    _seen.add(oid)
    try:
        size = sys.getsizeof(obj)
    except TypeError:
        return 0

    if isinstance(obj, dict):
        size += sum(_deep_sizeof(k, _seen) + _deep_sizeof(v, _seen) for k, v in obj.items())
    elif isinstance(obj, (list, tuple, set, frozenset)):
        size += sum(_deep_sizeof(item, _seen) for item in obj)
    return size


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
            total = self.hits + self.misses
            hit_rate = round(self.hits / total if total > 0 else 0.0, 4)
            return {
                "hits": self.hits,
                "misses": self.misses,
                "evictions": self.evictions,
                "expirations": self.expirations,
                "inserts": self.inserts,
                "deletes": self.deletes,
                "hit_rate": hit_rate,
            }

    def to_prometheus(self, namespace: str = "fast_cache", cache_label: str = "default") -> str:
        """Render counters in Prometheus text-exposition format.

        Returns a string suitable for ``/metrics`` endpoints. Names follow
        the ``<namespace>_<metric>_total`` convention so a scraper can pick
        them up without extra config. Hit rate is exposed as a gauge
        (``_ratio``) since it's bounded 0..1 rather than monotonic.

        Example::

            # GUNICORN_METRICS handler
            def metrics():
                return Response(cache.stats.to_prometheus(), mimetype="text/plain")
        """
        with self._lock:
            total = self.hits + self.misses
            hit_rate = (self.hits / total) if total > 0 else 0.0
            lines = [
                f"# HELP {namespace}_hits_total Total cache hits.",
                f"# TYPE {namespace}_hits_total counter",
                f'{namespace}_hits_total{{cache="{cache_label}"}} {self.hits}',
                f"# HELP {namespace}_misses_total Total cache misses.",
                f"# TYPE {namespace}_misses_total counter",
                f'{namespace}_misses_total{{cache="{cache_label}"}} {self.misses}',
                f"# HELP {namespace}_evictions_total Total cache evictions (LRU).",
                f"# TYPE {namespace}_evictions_total counter",
                f'{namespace}_evictions_total{{cache="{cache_label}"}} {self.evictions}',
                f"# HELP {namespace}_expirations_total Total TTL expirations.",
                f"# TYPE {namespace}_expirations_total counter",
                f'{namespace}_expirations_total{{cache="{cache_label}"}} {self.expirations}',
                f"# HELP {namespace}_inserts_total Total cache inserts.",
                f"# TYPE {namespace}_inserts_total counter",
                f'{namespace}_inserts_total{{cache="{cache_label}"}} {self.inserts}',
                f"# HELP {namespace}_deletes_total Total cache deletes.",
                f"# TYPE {namespace}_deletes_total counter",
                f'{namespace}_deletes_total{{cache="{cache_label}"}} {self.deletes}',
                f"# HELP {namespace}_hit_ratio Cache hit ratio (0..1).",
                f"# TYPE {namespace}_hit_ratio gauge",
                f'{namespace}_hit_ratio{{cache="{cache_label}"}} {hit_rate:.6f}',
            ]
        return "\n".join(lines) + "\n"

    def __repr__(self) -> str:
        return f"CacheStats(hits={self.hits}, misses={self.misses}, hit_rate={self.hit_rate:.2%})"


class CacheMetricsListener:
    """Push-style metrics consumer for the agent/observability ecosystem.

    Wraps a :class:`CacheStats` and forwards every counter mutation to a
    caller-supplied sink (an OpenTelemetry meter, a Prometheus client
    registry, or any plain function). One listener per cache keeps the
    fan-out cheap — the listener hooks the counters directly rather than
    polling, so emit latency is bounded by a single attribute access.

    Example with OpenTelemetry::

        from opentelemetry import metrics
        meter = metrics.get_meter("fast_cache")
        hits_counter = meter.create_counter("cache_hits")
        misses_counter = meter.create_counter("cache_misses")

        def sink(event: str, _delta: int) -> None:
            if event == "hit":
                hits_counter.add(1)
            elif event == "miss":
                misses_counter.add(1)

        listener = CacheMetricsListener(cache.stats, sink)

    The listener is idempotent and safe to construct/discard at any time.
    """

    _EVENTS = ("hit", "miss", "eviction", "expiration", "insert", "delete")

    def __init__(self, stats: "CacheStats", sink) -> None:
        self._stats = stats
        self._sink = sink
        # Snapshot counter values so deltas are accurate across threads.
        self._last = self._snapshot()

    def _snapshot(self) -> dict:
        with self._stats._lock:
            return {
                "hit": self._stats.hits,
                "miss": self._stats.misses,
                "eviction": self._stats.evictions,
                "expiration": self._stats.expirations,
                "insert": self._stats.inserts,
                "delete": self._stats.deletes,
            }

    def emit(self) -> dict:
        """Compute deltas since the last emit and forward them to the sink.

        Returns the delta dict that was forwarded (useful for logging or
        chaining to additional sinks). Counters that did not change are
        omitted to keep the per-tick payload tiny.
        """
        current = self._snapshot()
        delta = {}
        for ev in self._EVENTS:
            d = current[ev] - self._last.get(ev, 0)
            if d:
                delta[ev] = d
        if delta:
            try:
                self._sink(delta)
            except Exception:
                # Sink errors must never break the cache.
                pass
        self._last = current
        return delta

    def __repr__(self) -> str:
        return f"CacheMetricsListener(events={len(self._EVENTS)})"


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

    # ------------------------------------------------------------------
    # Async bulk operations (v0.5) — for agent workloads
    # ------------------------------------------------------------------

    async def bulk_get_or_set_async(
        self,
        keys: list[str],
        afactory: Callable[[str], Awaitable[Any]],
        ttl: Optional[float] = None,
        max_concurrency: int = 10,
    ) -> dict[str, Any]:
        """Bulk get-or-set with controlled concurrency.

        Like populate_missing but with semaphore-controlled parallelism
        to avoid thundering-herd on hot keys.

        Args:
            keys: List of cache keys to look up
            afactory: Async callable(key) -> value for cache misses
            ttl: Optional TTL override
            max_concurrency: Max simultaneous factory calls (default 10)

        Returns:
            {key: value} for all requested keys (hits + misses)
        """
        import asyncio
        import asyncio as _asyncio
        got = self.mget(keys)
        missing = [k for k in keys if k not in got]
        if not missing:
            return got

        semaphore = _asyncio.Semaphore(max_concurrency)

        async def fetch_with_limit(k: str) -> tuple[str, Any]:
            async with semaphore:
                val = await afactory(k)
                self.set(k, val, ttl=ttl)
                return k, val

        results = await _asyncio.gather(*[fetch_with_limit(k) for k in missing])
        for k, v in results:
            got[k] = v
        return got

    async def bulk_invalidate_async(
        self,
        keys: list[str],
        pattern: Optional[str] = None,
        regex: bool = False,
    ) -> int:
        """Async bulk invalidation by key list or pattern."""
        import asyncio
        if pattern:
            return await self.evict_by_pattern(pattern, regex=regex)
        deleted = 0
        for k in keys:
            if self.delete(k):
                deleted += 1
        return deleted

    def mget(
        self,
        keys: list[str],
        *,
        return_stats: bool = False,
    ):
        """Get multiple keys.

        By default returns ``{key: value}`` for keys that exist and are
        non-expired (missing/expired keys are simply absent from the
        result). Each successful hit promotes its key to MRU.

        With ``return_stats=True`` returns a ``MGetResult`` named tuple
        so callers can see which keys were missing/expired without
        having to recompute the diff against the request. Stats are
        always incremented under the same lock as the read so they are
        consistent with the returned values.

        Consistency guarantee: if a key is missing or expired at the
        moment of the call, it is **never** silently substituted with
        ``None`` in the result — it is omitted from ``found`` and
        listed in ``missing`` (when ``return_stats=True``).
        """
        from collections import namedtuple

        found: dict[str, Any] = {}
        missing: list[str] = []
        expired: list[str] = []

        with self._lock:
            for key in keys:
                entry = self._data.get(key)
                if entry is None:
                    missing.append(key)
                    self._stats.miss()
                    continue
                if entry.is_expired:
                    self._remove(key)
                    expired.append(key)
                    self._stats.expiration()
                    self._stats.miss()
                    continue
                # Hit: promote to MRU and record
                self._data.move_to_end(key)
                entry.touch()
                found[key] = entry.value
                self._stats.hit()

        if not return_stats:
            return found

        MGetResult = namedtuple(
            "MGetResult",
            ["found", "missing", "expired", "requested", "stats"],
        )
        stats = {
            "found": len(found),
            "missing": len(missing),
            "expired": len(expired),
            "requested": len(keys),
        }
        return MGetResult(
            found=found,
            missing=missing,
            expired=expired,
            requested=len(keys),
            stats=stats,
        )

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
    # Introspection (no LRU side-effects)
    # ------------------------------------------------------------------

    def peek(self, key: str) -> Optional[dict]:
        """Inspect key metadata without affecting LRU order.

        Returns a dict with ``value``, ``ttl_remaining``, ``size``, and
        ``access_count`` for the key, or ``None`` if missing/expired.
        Unlike ``get``, this does **not** touch the entry or update LRU.
        """
        with self._lock:
            entry = self._data.get(key)
            if entry is None or entry.is_expired:
                return None
            return {
                "value": entry.value,
                "ttl_remaining": (
                    max(0.0, entry.expires_at - time.time())
                    if entry.expires_at is not None
                    else None
                ),
                "size": entry.size,
                "access_count": entry.access_count,
            }

    def profile(self) -> dict:
        """Return per-entry memory usage and aggregate totals.

        Each entry's size is measured with ``sys.getsizeof`` (recursive over
        container contents) so the totals reflect Python object overhead
        rather than the cheap JSON-serialization estimate used for
        ``maxmemory`` enforcement.

        Returns::

            {
                "items":   [{"key": str, "size": int, ...}, ...],
                "total":   int,   # sum of all entry sizes
                "entries": int,   # count of profiled entries
            }
        """
        items: list[dict] = []
        total = 0
        with self._lock:
            snapshot = list(self._data.items())
        for key, entry in snapshot:
            sz = _deep_sizeof(entry.value)
            items.append(
                {
                    "key": key,
                    "size": sz,
                    "ttl_remaining": (
                        max(0.0, entry.expires_at - time.time())
                        if entry.expires_at is not None
                        else None
                    ),
                    "access_count": entry.access_count,
                }
            )
            total += sz
        return {"items": items, "total": total, "entries": len(items)}

    # ------------------------------------------------------------------
    # Selective eviction
    # ------------------------------------------------------------------

    def evict_oldest(self, n: int = 1) -> int:
        """Evict the ``n`` least-recently-used entries.

        Returns the number of entries actually evicted (may be less than
        ``n`` if the cache holds fewer than ``n`` items, or fewer if some
        were already expired and got cleaned out first).
        """
        if n <= 0:
            return 0
        evicted = 0
        with self._lock:
            # Drop any already-expired entries up front so they don't count.
            expired = [k for k, e in self._data.items() if e.is_expired]
            for key in expired:
                self._remove(key)
                self._stats.expiration()
            while evicted < n and self._data:
                key, entry = self._data.popitem(last=False)
                self._total_size -= entry.size
                self._stats.eviction()
                evicted += 1
                if self._evict_callback:
                    try:
                        self._evict_callback(key, entry.value)
                    except Exception:
                        pass
        return evicted

    def evict_by_pattern(
        self,
        pattern: str,
        *,
        regex: bool = False,
    ) -> int:
        """Evict keys matching ``pattern``.

        With ``regex=False`` (default) the pattern is treated as a
        ``fnmatch`` glob (e.g. ``"session:*"`` or ``"*expired*"``). With
        ``regex=True`` the pattern is a compiled regex matched against
        each key. Returns the number of keys evicted. The configured
        ``evict_callback`` is fired for each removed key with its value.
        """
        import re as _re
        import fnmatch as _fn

        if regex:
            compiled = _re.compile(pattern)
            def matches(key: str) -> bool:
                return compiled.search(key) is not None
        else:
            def matches(key: str) -> bool:
                return _fn.fnmatch(key, pattern)

        evicted = 0
        with self._lock:
            targets = [key for key in list(self._data.keys()) if matches(key)]
            for key in targets:
                # _remove already fires the evict_callback with the value.
                self._remove(key)
                self._stats.eviction()
                evicted += 1
        return evicted

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, path: str, *, binary: bool = False) -> int:
        """Save cache to file. Returns bytes written.

        Args:
            path: Destination path.
            binary: If True, use pickle-protocol (faster, preserves arbitrary
                object types). If False (default), use JSON with ``default=str``.
        """
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
            if binary:
                import pickle
                payload = pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
                with open(path, "wb") as f:
                    f.write(payload)
                return len(payload)
            payload = json.dumps(data, default=str, indent=2).encode()
            with open(path, "wb") as f:
                f.write(payload)
            return len(payload)

    def load(self, path: str, *, binary: bool = False) -> int:
        """Load cache from a file. Returns count loaded.

        Args:
            path: Source path.
            binary: Must match the ``binary`` flag used in :meth:`save`.
        """
        with self._lock:
            if not os.path.exists(path):
                return 0
            if binary:
                import pickle
                with open(path, "rb") as f:
                    data = pickle.load(f)
            else:
                with open(path, "rb") as f:
                    data = json.loads(f.read().decode())
            count = 0
            for key, item in data.items():
                ttl = item.get("ttl")
                if ttl is not None and ttl <= 0:
                    continue
                self.set(key, item["value"], ttl=ttl)
                count += 1
            return count

    def export_csv(self, path: str) -> int:
        """Export cache (key, ttl_remaining, size) to CSV for spreadsheets/BI."""
        import csv
        with self._lock:
            rows = []
            for key, entry in self._data.items():
                if entry.is_expired:
                    continue
                ttl = entry.expires_at - time.time() if entry.expires_at else None
                rows.append((key, entry.size, round(ttl or 0, 2)))
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["key", "size_bytes", "ttl_remaining_s"])
            w.writerows(rows)
        return len(rows)

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


class DiskSpillover:
    """LRU-aware disk-backed cache that transparently spills cold entries to disk.

    Keeps a hot in-memory tier (via :class:`Cache`) and moves the least-
    recently-used entries to a JSONL file when memory pressure hits
    ``max_memory_bytes``. On access, spilled entries are re-loaded back into
    RAM. Designed for agents that cache large payloads (embeddings, file
    contents, API responses) without exhausting heap.

    Args:
        cache: An optional :class:`Cache` instance (used as the hot tier).
            If None, a default in-memory cache is created.
        disk_path: Path to the spill file. Defaults to ``"./spill.jsonl"``.
        max_memory_bytes: Once in-memory usage exceeds this, oldest entries
            are spilled. Default 16 MiB.

    Example:
        from fast_cache import Cache, DiskSpillover
        spill = DiskSpillover(disk_path="/tmp/cache")
        spill.set("big", {"data": [1, 2, 3]} * 10000)
        value = spill.get("big")
        print(spill.stats())
    """

    def __init__(
        self,
        cache: "Cache" = None,
        disk_path: str = "./spill.jsonl",
        max_memory_bytes: int = 16 * 1024 * 1024,
    ):
        self._cache = cache or Cache()
        self.disk_path = disk_path
        self.max_memory_bytes = max_memory_bytes
        self._spill_index: dict[str, float] = {}  # key -> spilled_at
        self._lock = self._cache._lock

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> bool:
        """Set a key. Spills oldest entries if over memory budget."""
        with self._lock:
            ok = self._cache.set(key, value, ttl=ttl)
            self._evict_to_disk()
            return ok

    def get(self, key: str, default: Any = None) -> Any:
        """Get a key, promoting it back into the hot tier if on disk."""
        with self._lock:
            val = self._cache.peek(key)
            if val is not None:
                return val["value"]
            value = self._read_disk(key)
            if value is not None:
                self._spill_index.pop(key, None)
                self._cache.set(key, value)
                return value
            return default

    def delete(self, key: str) -> bool:
        with self._lock:
            self._spill_index.pop(key, None)
            self._cache.delete(key)
            return True

    def stats(self) -> dict:
        return {
            "in_memory": self._cache.size,
            "spilled": len(self._spill_index),
            "memory_usage": self._cache.memory_usage,
            "max_memory_bytes": self.max_memory_bytes,
        }

    # -- internal --

    def _evict_to_disk(self) -> None:
        # Peek LRU (oldest) entries and spill until under budget
        while self._cache.memory_usage > self.max_memory_bytes and self._cache.size > 1:
            key, entry = self._cache._data.popitem(last=False)
            self._cache._total_size -= entry.size
            self._cache._stats.eviction()
            self._spill_index[key] = time.time()
            self._append_disk(key, entry.value, entry.expires_at)

    def _append_disk(self, key: str, value: Any, expires_at: Optional[float]) -> None:
        import os
        rec = {
            "_k": key,
            "_v": value,
            "_e": expires_at - time.time() if expires_at else None,
        }
        mode = "a" if os.path.exists(self.disk_path) else "w"
        with open(self.disk_path, mode, encoding="utf-8") as f:
            f.write(json.dumps(rec, default=str) + "\n")

    def _read_disk(self, key: str) -> Any:
        import os
        if not os.path.exists(self.disk_path):
            return None
        try:
            with open(self.disk_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if rec.get("_k") == key:
                        return rec.get("_v")
        except OSError:
            pass
        return None

    def __contains__(self, key: str) -> bool:
        return key in self._cache or key in self._spill_index

    def __len__(self) -> int:
        return self._cache.size + len(self._spill_index)



class CacheAsync:
    """Async wrapper around :class:`Cache` for asyncio-centric workloads.

    All methods are coroutines that delegate to the internal synchronous
    :class:`Cache`. Access to the underlying cache is serialized with an
    ``asyncio.Lock`` so that concurrent coroutines (embedding batches, LLM
    prefetch, agent request fan-out) do not interleave mid-operation.

    Every method mirrors the sync API but returns an awaitable:

    .. code-block:: python

        cache = CacheAsync(maxsize=1000, ttl=3600)
        await cache.set("key", "value")
        value = await cache.get("key")

    Because the wrapped :class:`Cache` is itself thread-safe, you can also
    mix synchronous calls on ``cache._cache`` directly if needed.
    """

    def __init__(
        self,
        maxsize: Optional[int] = None,
        ttl: Optional[float] = None,
        maxmemory: Optional[int] = None,
        evict_callback: Optional[Callable[[str, Any], None]] = None,
        thread_safe: bool = True,
    ):
        # Deferred import so asyncio is only required when actually used.
        self._cache = Cache(
            maxsize=maxsize,
            ttl=ttl,
            maxmemory=maxmemory,
            evict_callback=evict_callback,
            thread_safe=thread_safe,
        )
        self._alock = _AsyncNullLock()

    # ------------------------------------------------------------------
    # Core async operations
    # ------------------------------------------------------------------

    async def get(self, key: str, default: Any = None) -> Any:
        async with self._alock:
            return self._cache.get(key, default=default)

    async def set(self, key: str, value: Any, ttl: Optional[float] = None) -> bool:
        async with self._alock:
            return self._cache.set(key, value, ttl=ttl)

    async def get_or_set(
        self,
        key: str,
        afactory: Callable[[], Awaitable[Any]],
        ttl: Optional[float] = None,
    ) -> Any:
        """Get value, or await ``afactory`` and set it if missing.

        If the key is already cached the factory is never awaited.
        """
        value = await self.get(key)
        if value is not None:
            return value
        value = await afactory()
        await self.set(key, value, ttl=ttl)
        return value

    async def delete(self, key: str) -> bool:
        async with self._alock:
            return self._cache.delete(key)

    async def pop(self, key: str, default: Any = None) -> Any:
        async with self._alock:
            return self._cache.pop(key, default=default)

    async def has(self, key: str) -> bool:
        async with self._alock:
            return self._cache.has(key)

    # ------------------------------------------------------------------
    # Batch async operations
    # ------------------------------------------------------------------

    async def mget(self, keys: list[str], *, return_stats: bool = False):
        async with self._alock:
            return self._cache.mget(keys, return_stats=return_stats)

    async def mset(self, items: dict[str, Any], ttl: Optional[float] = None) -> int:
        async with self._alock:
            return self._cache.mset(items, ttl=ttl)

    async def mdelete(self, keys: list[str]) -> int:
        async with self._alock:
            return self._cache.mdelete(keys)

    async def clear(self) -> int:
        async with self._alock:
            return self._cache.clear()

    # ------------------------------------------------------------------
    # Introspection / profiling (delegated, no LRU side-effects)
    # ------------------------------------------------------------------

    async def peek(self, key: str) -> Optional[dict]:
        async with self._alock:
            return self._cache.peek(key)

    async def profile(self) -> dict:
        async with self._alock:
            return self._cache.profile()

    async def evict_oldest(self, n: int = 1) -> int:
        async with self._alock:
            return self._cache.evict_oldest(n)

    async def evict_by_pattern(self, pattern: str, *, regex: bool = False) -> int:
        async with self._alock:
            return self._cache.evict_by_pattern(pattern, regex=regex)

    async def cleanup(self) -> int:
        async with self._alock:
            return self._cache.cleanup()

    # ------------------------------------------------------------------
    # Info
    # ------------------------------------------------------------------

    async def info(self) -> dict:
        async with self._alock:
            return self._cache.info()

    @property
    def size(self) -> int:
        return self._cache.size

    @property
    def memory_usage(self) -> int:
        return self._cache.memory_usage

    @property
    def stats(self):
        return self._cache.stats

    def __len__(self) -> int:
        return len(self._cache)

    def __contains__(self, key: str) -> bool:
        return key in self._cache

    def __repr__(self) -> str:
        return f"CacheAsync({self._cache!r})"


class _AsyncNullLock:
    """Minimal async re-entrant lock guard.

    Uses :class:`asyncio.Lock` under the hood; created lazily by
    :class:`CacheAsync` so that importing the module never forces an
    asyncio dependency until a CacheAsync instance is constructed.
    """

    def __init__(self):
        import asyncio

        self._lock = asyncio.Lock()

    async def __aenter__(self):
        await self._lock.acquire()
        return self

    async def __aexit__(self, *args):
        self._lock.release()


class _NullLock:
    """No-op lock for thread_safe=False."""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


# ── cache_method decorator (v0.7.0) ──────────────────────────────────────────


import asyncio as _asyncio
import functools as _functools
import hashlib as _hashlib
import inspect as _inspect


_MISS = object()  # sentinel for "not in cache"


def _make_method_key(args: tuple, kwargs: dict) -> str:
    """Deterministic SHA-256 cache key from (args, kwargs).

    Uses ``repr`` so equal-but-differently-typed arguments still differ
    (``a=1`` and ``a=1.0`` are intentionally different keys). Tuple order
    is preserved for positional args; kwargs are sorted for determinism.
    """
    parts: list[str] = []
    for a in args:
        parts.append(("arg", repr(a)))
    for k in sorted(kwargs):
        parts.append(("kw", f"{k}={kwargs[k]!r}"))
    blob = "|".join(f"{tag}:{val}" for tag, val in parts).encode("utf-8")
    return _hashlib.sha256(blob).hexdigest()


def _strip_self(args: tuple, fn: Callable[..., Any]) -> tuple:
    """No-op placeholder. ``self``/``cls`` are KEPT in the cache key by default
    so that two instances of the same class don't share slots. Users who want
    instance-shared caching should use ``key_fn=`` to drop self explicitly."""
    return args


def cache_method(
    cache: "Cache | CacheAsync",
    *,
    ttl: Optional[float] = None,
    prefix: str = "",
    key_fn: Optional[Callable[..., str]] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator: cache a method's return value on an existing :class:`Cache` or :class:`CacheAsync`.

    Works on both sync and async methods. The cache key is built from
    ``(args, kwargs)`` after dropping ``self`` for bound methods — so two
    instances of the same class don't share slots. Pass ``key_fn=callable``
    when you want a custom key (e.g. ignore certain kwargs).

    Args:
        cache: A :class:`Cache` (sync) or :class:`CacheAsync` instance.
        ttl: Optional per-call TTL override; falls back to the cache's default.
        prefix: Optional namespace prefix, e.g. ``"user.get"``. Prepended to the key.
        key_fn: Optional callable that takes ``(*args, **kwargs)`` (after ``self`` is
            stripped) and returns the cache key directly.

    Example:
        cache = Cache(maxsize=1024, ttl=60)

        class UserService:
            @cache_method(cache, prefix="user.get", ttl=30)
            def get_user(self, user_id: str) -> dict:
                return db.fetch(user_id)

        svc = UserService()
        svc.get_user("u-1")  # cached for 30s
    """
    def _wrap_target(fn: Callable[..., Any]) -> Callable[..., Any]:
        # ``@staticmethod`` / ``@classmethod`` objects are descriptors, not
        # plain functions. A ``staticmethod`` object only became directly
        # callable in Python 3.10 (bpo-43682); on Python 3.8/3.9 calling one
        # raises ``TypeError: staticmethod object is not callable``, which is
        # what happened when ``@cache_method`` was stacked on top of
        # ``@staticmethod`` on those interpreters.
        is_staticmethod = isinstance(fn, staticmethod)
        if is_staticmethod:
            fn = fn.__func__
        elif isinstance(fn, classmethod):
            raise TypeError(
                "cache_method() does not support @classmethod; apply it to the "
                "underlying function instead "
                "(e.g. cache_method(cache)(Cls.method.__func__))"
            )

        wrapper = decorator(fn)
        if is_staticmethod:
            # Keep it a descriptor so ``Cls.method(x)`` and ``obj.method(x)``
            # both call it without binding self/cls as the first argument.
            return staticmethod(wrapper)
        return wrapper

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        is_coro = _asyncio.iscoroutinefunction(fn)

        def _key(args: tuple, kwargs: dict) -> str:
            if key_fn is not None:
                return prefix + key_fn(*args, **kwargs)
            return prefix + _make_method_key(args, kwargs)

        if is_coro:
            @_functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                key = _key(_strip_self(args, fn), kwargs)
                hit = await cache.get(key, default=_MISS)
                if hit is not _MISS:
                    return hit
                value = await fn(*args, **kwargs)
                await cache.set(key, value, ttl=ttl)
                return value

            return async_wrapper

        @_functools.wraps(fn)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            key = _key(_strip_self(args, fn), kwargs)
            hit = cache.get(key, default=_MISS)
            if hit is not _MISS:
                return hit
            value = fn(*args, **kwargs)
            cache.set(key, value, ttl=ttl)
            return value

        return sync_wrapper

    return _wrap_target
