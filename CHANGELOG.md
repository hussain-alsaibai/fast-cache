# Changelog

All notable changes to `fast-cache` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

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

[0.6.1]: https://github.com/hussain-alsaibai/fast-cache/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/hussain-alsaibai/fast-cache/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/hussain-alsaibai/fast-cache/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/hussain-alsaibai/fast-cache/releases/tag/v0.4.0
