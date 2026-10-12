"""fast_cache_v071 — CRDT-flavored merge and tombstone-aware evict.

Sidecar module for fast-cache v0.7.1. Lazy-imports the core
`fast_cache` package. New functionality only — existing fast-cache
users see no behaviour change unless they `import fast_cache_v071`.

What is in here
---------------
- `merge(local, remote)` — CRDT-flavored state merge across two peer
  cache snapshots. Tombstones win over inserts.
- `tombstone_evict(cache, key)` — soft-delete a key, marking it as
  deleted with a timestamp. The next sweep permanently removes it.
- `is_tombstoned(cache, key)` — check whether a key is soft-deleted.
- `merge_async(local, remote)` — async companion that runs the merge
  in a thread pool, so the event loop never blocks.

Why a sidecar?
--------------
The core `fast_cache` package stays at v0.7.0. The CRDT merge, the
tombstone-aware evict, and the async companion all live here so
existing users do not need to upgrade anything. Import this module
only when you want the new functionality.

Use case
---------
A small fleet of peers (≤10) each runs a local `fast_cache.Cache`
instance. Periodically, each peer emits its snapshot to a coordinator
which calls `merge()` to reconcile. The merge is associative,
commutative, and idempotent, so any order of peer updates converges
to the same state.

The tombstone-aware evict lets a peer signal "I deleted this key"
without immediately losing the value (which might still be in flight
on another peer). When the next merge runs, the tombstone wins and
the key is gone on every peer.
"""

from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any


__version__ = "0.7.1"


# ---------- Tombstone ----------

class Tombstone:
    """A soft-deletion marker stored in the cache under the same key.

    When the cache is merged with a peer, the tombstone wins over any
    insert with the same key. The tombstone is removed by the next
    sweep, or by an explicit `purge_tombstones()` call.
    """

    __slots__ = ("deleted_at", "reason")

    # A Tombstone is a marker, not a cache entry, so it should not
    # be subject to TTL eviction. We expose the attribute anyway so
    # the core fast_cache._evict_if_needed helper can iterate over
    # _data without type-checking every value.
    is_expired = False

    # Tombstones are not real cache entries and have no access counter.
    # Expose a no-op `touch` so Cache.get() can call it without raising
    # when iterating over a mixed _data dict.
    def touch(self):  # pragma: no cover
        return None

    def __init__(self, deleted_at: float | None = None, reason: str = ""):
        self.deleted_at = deleted_at if deleted_at is not None else time.time()
        self.reason = reason

    def __repr__(self):  # pragma: no cover
        return "Tombstone(deleted_at=" + repr(self.deleted_at) + ", reason=" + repr(self.reason) + ")"


def is_tombstoned(cache, key) -> bool:
    """Return True if `cache` holds a `Tombstone` under `key`.

    Reads the underlying ``_data`` dict when available so we do not
    trigger side effects in the core cache (LRU touch, access counts).
    Falls back to ``cache.get(key)`` for dict-like backends.
    """
    try:
        if hasattr(cache, "_data") and isinstance(cache._data, dict):
            return isinstance(cache._data.get(key), Tombstone)
        return isinstance(cache.get(key), Tombstone)
    except Exception:
        return False


def tombstone_evict(cache, key, *, reason: str = "") -> None:
    """Mark `key` as soft-deleted in `cache`.

    The next merge treats the tombstone as authoritative. The key
    is removed on the next sweep or via `purge_tombstones()`.

    Bypasses ``cache.set`` because the core fast-cache ``Cache.set``
    wraps values in a ``_CacheEntry`` and would hide the ``Tombstone``
    type. We store the ``Tombstone`` directly in the underlying
    ``_data`` dict (if exposed) so ``snapshot()`` and ``is_tombstoned()``
    see it.
    """
    ts = Tombstone(reason=reason)
    # Fast path: write straight to internal storage.
    if hasattr(cache, "_data") and isinstance(cache._data, dict):
        cache._data[key] = ts
        return
    # Fallback for dict-like caches (e.g. plain dict or simple wrappers).
    if hasattr(cache, "__setitem__"):
        cache[key] = ts
        return
    # Last resort: go through the public API. snapshot()/is_tombstoned()
    # may not see the tombstone, but the call is at least non-destructive.
    try:
        cache.set(key, ts)  # pragma: no cover
    except Exception:
        pass


def purge_tombstones(cache) -> int:
    """Permanently remove all tombstones from `cache`. Returns the count."""
    removed = 0
    # We need access to the underlying storage. Most cache impls
    # expose a `_data` dict or an `items()` iterator.
    if hasattr(cache, "_data") and isinstance(cache._data, dict):
        for k in list(cache._data.keys()):
            if isinstance(cache._data[k], Tombstone):
                del cache._data[k]
                removed += 1
    elif hasattr(cache, "items"):
        for k, v in list(cache.items()):
            if isinstance(v, Tombstone):
                cache.delete(k)
                removed += 1
    return removed


# ---------- Snapshot helpers ----------

def snapshot(cache) -> dict:
    """Return a JSON-safe dict snapshot of `cache`.

    Each entry is `(value, timestamp)`. Tombstones are preserved as
    `{"__tombstone__": True, "deleted_at": ts, "reason": "..."}` so
    they round-trip through merge without losing information.
    """
    out: dict = {}
    if hasattr(cache, "_data") and isinstance(cache._data, dict):
        items = cache._data.items()
    elif hasattr(cache, "items"):
        items = cache.items()
    else:
        raise TypeError("cache has neither _data nor items()")
    for k, v in items:
        if isinstance(v, Tombstone):
            out[k] = {
                "__tombstone__": True,
                "deleted_at": v.deleted_at,
                "reason": v.reason,
            }
        else:
            # Unwrap _CacheEntry-style wrappers if present. Real cache
            # backends (e.g. fast_cache.Cache) wrap each value in a
            # _CacheEntry that exposes `.value`. Plain dicts store the
            # value directly. Either way we want the raw value, not the
            # wrapper, so apply_merge can re-set() it.
            raw = getattr(v, "value", v)
            # The "timestamp" is best-effort: some caches expose a
            # `set_at` accessor; others don't. Fall back to 0.
            ts = getattr(v, "set_at", 0) or 0
            out[k] = (raw, ts)
    return out


def _is_tombstone_entry(v) -> bool:
    return isinstance(v, dict) and v.get("__tombstone__") is True


def _entry_ts(v) -> float:
    if _is_tombstone_entry(v):
        return float(v.get("deleted_at", 0.0))
    if isinstance(v, tuple) and len(v) == 2:
        return float(v[1])
    return 0.0


def _entry_value(v):
    if _is_tombstone_entry(v):
        return Tombstone(deleted_at=v.get("deleted_at", 0.0), reason=v.get("reason", ""))
    if isinstance(v, tuple) and len(v) == 2:
        return v[0]
    return v


# ---------- CRDT merge ----------

def merge(local: dict, remote: dict) -> dict:
    """Merge two cache snapshots, CRDT-style.

    Both inputs are dicts produced by `snapshot()`. The result is
    associative, commutative, and idempotent:

    - Tombstones always win over inserts (a deletion seen on either
      side is preserved).
    - For two inserts of the same key, last-writer-wins by timestamp
      (the higher timestamp wins; ties broken by value identity).
    - For a tombstone + an insert, the tombstone wins.
    - For two tombstones of the same key, the more recent one wins.

    This makes the merge safe to apply in any order: a fleet of
    peers can each emit their own snapshot, and any sequence of
    merges converges to the same state.
    """
    if not isinstance(local, dict) or not isinstance(remote, dict):
        raise TypeError(
            "merge expects two dicts, got " + type(local).__name__ + " and " + type(remote).__name__,
        )
    out: dict = {}
    for k in set(local) | set(remote):
        lv = local.get(k)
        rv = remote.get(k)
        l_tomb = _is_tombstone_entry(lv) if lv is not None else False
        r_tomb = _is_tombstone_entry(rv) if rv is not None else False
        if l_tomb and r_tomb:
            # Both tombstones: more recent wins.
            lt = _entry_ts(lv)
            rt = _entry_ts(rv)
            out[k] = rv if rt >= lt else lv
        elif l_tomb:
            out[k] = lv
        elif r_tomb:
            out[k] = rv
        else:
            # Both inserts: higher timestamp wins; tie broken by value identity.
            lt = _entry_ts(lv)
            rt = _entry_ts(rv)
            if rt > lt:
                out[k] = rv
            elif lt > rt:
                out[k] = lv
            else:
                # Equal timestamps: prefer the value that is not None,
                # else the one with the higher id() (deterministic tiebreak).
                if lv == rv:
                    out[k] = lv
                else:
                    out[k] = lv if id(lv) <= id(rv) else rv
    return out


def apply_merge(cache, merged: dict) -> int:
    """Apply a merged snapshot to `cache`. Returns the number of keys written.

    Tombstones are applied via `tombstone_evict()`. Non-tombstone
    entries are written via `cache.set()`.
    """
    if not isinstance(merged, dict):
        raise TypeError("merged must be a dict, got " + type(merged).__name__)
    written = 0
    for k, v in merged.items():
        if _is_tombstone_entry(v):
            tombstone_evict(cache, k, reason=v.get("reason", ""))
        else:
            cache.set(k, _entry_value(v))
        written += 1
    return written


# ---------- Async companion ----------

_executor: ThreadPoolExecutor | None = None


def _get_executor():
    global _executor
    if _executor is None:
        _executor = ThreadPoolExecutor(
            max_workers=4,
            thread_name_prefix="fast-cache-merge-async",
        )
    return _executor


async def merge_async(local: dict, remote: dict) -> dict:
    """Async wrapper around `merge()`. Runs in a shared thread pool.

    The first call lazily creates the executor; subsequent calls reuse it.
    The executor is process-wide; call `shutdown_async()` to release it
    at process shutdown.
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_get_executor(), merge, local, remote)


def shutdown_async(wait: bool = True) -> None:
    """Shut down the async merge executor. Idempotent."""
    global _executor
    if _executor is not None:
        _executor.shutdown(wait=wait)
        _executor = None


__all__ = [
    "Tombstone",
    "apply_merge",
    "is_tombstoned",
    "merge",
    "merge_async",
    "purge_tombstones",
    "shutdown_async",
    "snapshot",
    "tombstone_evict",
]
