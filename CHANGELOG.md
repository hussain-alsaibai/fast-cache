# Changelog

All notable changes to `fast-cache` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).


## [0.7.3] - 2026-10-26

### Fixed
- **`cache_method` now works on top of `@staticmethod` on every supported
  Python.** A `staticmethod` object only became directly callable in
  Python 3.10 (bpo-43682). On Python 3.8 and 3.9 the decorator held the
  descriptor and called it directly, so every call raised
  `TypeError: 'staticmethod' object is not callable`. The decorator now
  unwraps `fn.__func__` and re-wraps the result as a `staticmethod`, so
  class access (`Cls.method(x)`) and instance access (`obj.method(x)`)
  both work and neither binds `self`/`cls` as the first argument.
- Applying `cache_method` to a `classmethod` now raises a `TypeError`
  with an actionable message instead of failing later with a confusing
  signature error. Use `cache_method(cache)(Cls.method.__func__)`.

### Added
- `tests/test_v073_staticmethod_py38.py` — regression coverage that
  simulates the pre-3.10 non-callable `staticmethod` with a subclass.
  Verified to fail against the pre-fix code (3 failed) and pass after
  (4 passed).

### Changed
- CI now runs the new test file.
- Bumped `__version__` and `pyproject.toml` to `0.7.3`.

## [0.7.2] - 2026-10-18

### Added
- **`fast_cache_v072.py` (sidecar)** — `SingleFlightCache` protects
  against cache-stampede: when N concurrent requests miss the same
  key, only one runs the factory; the rest block on a per-key
  `threading.Event` and read the result the first thread writes.
  New functionality only; existing fast-cache users see no behaviour
  change unless they `import fast_cache_v072`.
  - `SingleFlightCache(cache).get_or_set(key, factory, ttl=...)` — same
    shape as `Cache.get_or_set`, with single-flight semantics.
  - `SingleFlightStats` — counters for `loads`, `shared`, `waiters`, `peak_waiters`. `serialized()` for Prometheus.
  - Different keys run in parallel — the lock is per-key, not global.
  - Factory exceptions propagate to every waiter and clear the
    in-flight slot so retries can run.
- 15 new tests in `test_fast_cache_v072.py` covering single-thread
  semantics, stampede protection (10 threads, 1 factory call),
  parallel different-keys, exception handling, `None` factory
  result, stats, `clear()` waiter release, and TTL re-run.

## [0.7.1] - 2026-10-12

### Added
- **`fast_cache_v071.py` (sidecar)** — CRDT-flavored `merge()` and
  tombstone-aware `evict()` for fleet-of-peers reconciliation. New
  functionality only; existing fast-cache users see no behaviour change
  unless they `import fast_cache_v071`.
  - `merge(local, remote)` — associative, commutative, idempotent
    reconciliation of two cache snapshots. Tombstones always win over
    inserts; for two inserts the higher timestamp wins; for two
    tombstones the more recent deletion wins.
  - `tombstone_evict(cache, key)` — soft-delete a key, marking it as
    deleted with a timestamp. The next sweep permanently removes it.
  - `is_tombstoned(cache, key)` — check whether a key is soft-deleted.
  - `merge_async(local, remote)` — async companion that runs the merge
    in a thread pool, so the event loop never blocks.
- 25 new tests in `test_fast_cache_v071.py` covering CRDT merge
  semantics, tombstone lifecycle, async companion, idempotency, and
  integration with the real `fast_cache.Cache` backend.

### Fixed
- `Tombstone` now exposes `is_expired = False` and a no-op `touch()`
  so the core `Cache._evict_if_needed` and `Cache.get` helpers can
  iterate over a mixed `_data` dict without raising.
- `snapshot()` unwraps `_CacheEntry`-style value wrappers (extracting
  the inner `.value`) so merged snapshots re-`set()` cleanly on the
  real `fast_cache.Cache` backend.
- `tombstone_evict()` writes directly to `_data` instead of going
  through `cache.set()`, which would wrap the `Tombstone` in a
  `_CacheEntry` and hide it from `is_tombstoned()` and `merge()`.

## [0.7.0] - 2026-09-25

### Added

- **`cache_method(cache, *, ttl=None, prefix='', key_fn=None)`** — decorator that caches a method's return value on an existing `Cache` or `CacheAsync`. Works on both sync and async methods. The cache key is built from `(self, args, kwargs)` by default (so two instances of the same class don't share slots). Pass `key_fn=callable(self, *args, **kwargs) -> str` for a custom key (e.g. ignore kwargs, bucket by id mod N).
  - SHA-256-based deterministic keying.
  - Caches `None`, `0`, `""`, `False` correctly (uses internal sentinel to distinguish "miss" from "cached falsy value").
  - Preserves `__name__`, `__doc__`, `__wrapped__` via `functools.wraps`.
  - Use case: cache DB lookups, API calls, expensive computations, LLM prompts — without restructuring existing code.
- New `tests/test_v070_cache_method.py` — 19 tests covering sync, async, instances, kwargs, TTL, prefix, custom key_fn, module-level functions, staticmethods, falsy results, and complex return values. Full suite 62/62.

### Notes

- `cache_method` does NOT support `@classmethod` directly because Python's classmethod descriptor protocol doesn't forward `cls` when called from inside the decorator. Wrap `cls.__func__` instead, or skip the decorator for classmethods.

## [0.6.1] - 2026-09-23

### Added
- **examples/token_budget_cache.py** — a self-contained `TokenBudgetCache`
  example that gates a `Cache` against a per-task token ceiling.
  Pairs naturally with `tiny-budget-ledger` (replace the simulated
  ledger) so an LLM-backed cache refuses to memorize results once a
  budget is exhausted.
- `BudgetLedger` / `BudgetDecision` dataclasses + `SimulatedLedger` alias
  for drop-in testing without the upstream budget library.

### Notes
- No core API changes; 0.6.1 is additive.
- Demonstrates three new patterns for power users:
  1. "Don't memoize something we can't afford to remember."
  2. "Cache hits are free; cache misses pay."
  3. "If the budget refuses, return the value anyway — don't punish the
     caller for the budget refusing to memorize."

## [0.6.0] - 2026-09-20

### Added
- `CacheAsync` wrapper around the sync `Cache` (asyncio-friendly).
- `mget` / `mset` / `mdelete` batch operations with chunking.
- `clean_keys` API for bulk namespace-style invalidation.
- Per-key `access_count` and `last_access_at` exposed via `info()`.
- `DiskSpillover` opt-in back-end (append-only JSONL, fsync-per-write).
- `CacheMetricsListener` extensible metrics sink.

### Performance
- 10K keys, single-threaded micro-bench: ~450K `set`/s, ~480K `get`/s,
  ~350K mixed ops/s. Overhead vs raw dict stays under 15%.
- Concurrent (10 threads × 100K ops): ~280K ops/s, zero errors.
- Async `mget` batches hit >2M ops/s on the chunked bulk path.

## [0.5.0] - 2026-08-28

### Added
- `swr_ttl` parameter: stale-while-revalidate window.
- Memory profiling (`_deep_sizeof`, `estimate_bytes()`).

## [0.4.0] - 2026-08-03

### Added
- Initial public release. TTL, LRU, stats, persistence.

[0.7.1]: https://github.com/hussain-alsaibai/fast-cache/compare/v0.7.0...v0.7.1
[0.6.1]: https://github.com/hussain-alsaibai/fast-cache/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/hussain-alsaibai/fast-cache/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/hussain-alsaibai/fast-cache/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/hussain-alsaibai/fast-cache/releases/tag/v0.4.0
