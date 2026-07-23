# fast-cache

> Zero-dependency LRU + TTL + stale-while-revalidate cache for Python. ~3M ops/sec. Single file.

```bash
pip install fast-cache   # coming soon
```

## Why?

- **`functools.lru_cache`** — no TTL, can't be cleared for testing
- **`cachetools`** — 1 dep, well-tested, but external
- **`pylru`** — unmaintained, no TTL

**fast-cache** is a single-file library: O(1) LRU + LFU + FIFO + TTL-aware
eviction, per-entry TTL, SWR, sync + async decorators, thread-safe, with
stats, event hooks, async iteration, and JSON-serializable export / import.

## Usage

### As a decorator

```python
import fast_cache as fc

@fc.cache(max_size=128, ttl=60)
def expensive_query(user_id: int) -> dict:
    return db.query(f"SELECT * FROM users WHERE id={user_id}")

# Cache hit
expensive_query(42)
expensive_query(42)  # 0.6 µs (vs 1.2 ms for the DB call)

expensive_query.cache_info()
# {'size': 1, 'hits': 1, 'misses': 1, 'hit_ratio': 0.5, ...}

expensive_query.cache_clear()
```

### Async

```python
@fc.acache(max_size=128, ttl=60)
async def fetch_url(url: str) -> str:
    async with aiohttp.ClientSession() as s:
        async with s.get(url) as r:
            return await r.text()
```

### Async iteration

```python
cache = fc.Cache(max_size=1_000, default_ttl=60)

async def drain():
    async for key in cache:                     # live keys, cooperative
        print(key, cache.get(key))

    async for key, value in cache.items_async():
        print(key, value)

    async for value in cache.values_async():
        print(value)
```

### Event hooks

```python
cache = fc.Cache(max_size=1_000)

def on_hit(**kwargs):
    metrics.counter("cache.hits", tags=["key=" + kwargs["key"]]).inc()

cache.on(fc.CacheEvent.HIT,    on_hit)
cache.on(fc.CacheEvent.MISS,   lambda **kw: log.info("miss", extra=kw))
cache.on(fc.CacheEvent.SET,    lambda **kw: ...)
cache.on(fc.CacheEvent.EXPIRED, lambda **kw: ...)
cache.on(fc.CacheEvent.EVICTED, lambda **kw: ...)
cache.on(fc.CacheEvent.CLEARED, lambda **kw: ...)

# Remove handlers
cache.off(fc.CacheEvent.HIT, on_hit)
```

### Eviction policies

```python
import fast_cache as fc

lru  = fc.Cache(max_size=1_000, policy=fc.CachePolicy.LRU)        # default
lfu  = fc.Cache(max_size=1_000, policy=fc.CachePolicy.LFU)
fifo = fc.Cache(max_size=1_000, policy=fc.CachePolicy.FIFO)
ttl  = fc.Cache(max_size=1_000, policy=fc.CachePolicy.TTL_AWARE)

@fc.cache(max_size=256, ttl=60, policy=fc.CachePolicy.LFU)
def slow_query(x): ...
```

### Stats, export / import

```python
cache = fc.Cache(default_ttl=300)
cache.set("user:42", {"name": "Alice"})

print(cache.stats())
# {'hits': 0, 'misses': 0, 'sets': 1, 'evictions': 0, 'expirations': 0,
#  'hit_ratio': 0.0, 'total_time_ms': 0.0,
#  'size': 1, 'max_size': 1024, 'policy': 'lru'}

snapshot = cache.export()
# {'version': '1.0', 'policy': 'lru', 'items': [...]}

cache.clear()
cache.import_(snapshot)   # returns count of imported entries
```

### As a cache object

```python
cache = fc.Cache(max_size=10_000, default_ttl=300, stale_ttl=60)

cache.set("user:42", {"name": "alice"}, ttl=60)
user = cache.get("user:42")
```

### Stale-while-revalidate

```python
# After expiry, return stale value for `stale_ttl` seconds.
# The next get() will refresh on the live path.
cache = fc.Cache(default_ttl=60, stale_ttl=300)
cache.set("feed", [...])
# after 70s: get() returns stale value, marked for refresh
```

## API

| Class / constant | Description |
|------------------|-------------|
| `Cache(max_size, default_ttl, stale_ttl, *, policy)` | Main cache class. ``policy`` defaults to ``CachePolicy.LRU``. |
| `LRUCache(max_size)` | Bounded LRU only |
| `TTLCache(max_size, ttl)` | Bounded with TTL |
| `CachePolicy.LRU / LFU / FIFO / TTL_AWARE` | Eviction strategies |
| `CacheEvent.HIT / MISS / SET / EXPIRED / EVICTED / CLEARED` | Event hook names |
| `CacheStats` | Dataclass with hit / miss / eviction counters and rolling ``hit_ratio`` |

| Decorator | Description |
|-----------|-------------|
| `@cache(max_size, ttl, stale_ttl, *, policy)` | Sync function caching |
| `@acache(max_size, ttl, stale_ttl, *, policy)` | Async function caching |

| Method | Description |
|--------|-------------|
| `c.get(key, default)` | Get value (returns stale within SWR window) |
| `c.get_many(keys, default)` | Fetch several keys while preserving hit/miss stats |
| `c.set(key, value, ttl)` | Insert/overwrite |
| `c.set_many({key: value}, ttl)` | Bulk insert with one optional TTL |
| `c.add(key, value, ttl)` | Atomic claim (returns ``False`` if key is live) |
| `c.touch(key, ttl)` | Sliding keepalive — extend TTL of a live entry |
| `c.delete(key)` | Remove entry |
| `c.prune()` | Remove entries past TTL + stale window |
| `c.keys()` | Live keys in LRU-to-MRU order |
| `c.values()` | Live values in LRU-to-MRU order |
| `c.items()` | Live `(key, value)` pairs in LRU-to-MRU order |
| `c.clear()` | Empty cache |
| `c.stats()` | Dict snapshot of stats + size + policy |
| `c.stats_obj()` | Underlying :class:`CacheStats` instance |
| `c.reset_stats()` | Zero out the counters (cache contents preserved) |
| `c.on(event, handler)` | Register an event handler |
| `c.off(event, handler=None)` | Remove handler(s) for an event |
| `c.emit(event, **kw)` | Fire an event to all registered handlers |
| `c.export()` | JSON-serializable snapshot of live entries |
| `c.import_(data)` | Import from an ``export()`` payload; returns count |
| `async for k in c` | Async iteration over live keys |
| `async for k, v in c.items_async()` | Async iteration over `(key, value)` pairs |
| `async for v in c.values_async()` | Async iteration over values |
| `f.cache_info()` | Same as `f.cache.stats()` for a decorated function |
| `f.cache_clear()` | Clear a decorated function's cache |

## Agent Workflow Fit

`fast-cache` is useful anywhere an autonomous workflow needs bounded memory
without adding Redis or another service:

- **Tool result memoization** — cache expensive GitHub, web, or model-provider lookups inside one run.
- **Webhook dedupe windows** — keep recent delivery IDs with TTL before reaching for persistent idempotency.
- **Bounty repro harnesses** — avoid repeated setup calls while keeping a single-file proof of concept.
- **Cron health checks** — reuse recent probe results and expose `stats()` in status reports.

Use `get_many()` / `set_many()` for batched tool calls and `prune()` before
long-running status snapshots so cache size reflects live entries only.

See also:

- [Agent Memoization and Dedupe With fast-cache](reports/2026-07-09-agent-memoization.md) — caching repeated tool results and webhook delivery IDs inside local agent workflows.
- [Agent Scan Caches With fast-cache](reports/2026-07-11-agent-scan-caches.md) — bounded TTL caches for bounty scanners, repo health checks, and metadata-heavy agent runs.
- [Cache ROI for Agent Runs](reports/2026-07-14-cache-roi-for-agent-runs.md) — using cache hit rate, avoided tool calls, and stale-safe reuse to decide where local caches belong.
- [Agent Context Cache Boundaries With fast-cache](reports/2026-07-17-agent-context-cache-boundaries.md) — TTL scopes, negative-cache limits, and cache stats for production agent workflows.

## Benchmarks

Rough numbers on a single thread of a 2026-era dev laptop (single-threaded,
n=100,000 per operation). Single dict lookup + emit-on-hit; the per-entry dict
overhead is the dominant cost. Reproduce with ``python bench_fast_cache.py``.

```
== fast-cache benchmarks (n=100,000) ==
  get (hit)                         ~1.1 µs/op  (~900,000 ops/s)
  get (miss)                        ~0.6 µs/op  (~1,700,000 ops/s)
  set                               ~0.8 µs/op  (~1,300,000 ops/s)
  decorator (hit)                   ~1.1 µs/op  (~900,000 ops/s)
  decorator (miss)                  ~1.3 µs/op  (~770,000 ops/s)
  TTLCache.get                      ~1.0 µs/op  (~1,000,000 ops/s)
  LRUCache.get                      ~0.9 µs/op  (~1,100,000 ops/s)
```

Comparable to or faster than `cachetools` (which benchmarks at ~2-3M ops/sec on the same workload).

## Tests

```bash
python test_fast_cache.py
# Ran 26 tests in 0.79s — OK

python3 -m pytest tests/ -v
# 70 tests across test_async.py + test_policies.py
```

## Ecosystem

Part of the **tiny-*** zero-dependency toolkit for Python agent infrastructure:

- [**tiny-router**](https://github.com/hussain-alsaibai/tiny-router) — HTTP router, 76K req/s
- [**tiny-log**](https://github.com/hussain-alsaibai/tiny-log) — structured logging
- [**tiny-validator**](https://github.com/hussain-alsaibai/tiny-validator) — input validation, 247K val/s
- [**tiny-config**](https://github.com/hussain-alsaibai/tiny-config) — layered config loader
- [**tiny-cli**](https://github.com/hussain-alsaibai/tiny-cli) — CLI builder with colors
- [**fast-cache**](https://github.com/hussain-alsaibai/fast-cache) — LRU + TTL + SWR cache
- [**tiny-rate**](https://github.com/hussain-alsaibai/tiny-rate) — rate limiter (token / fixed / sliding)
- [**tiny-retry**](https://github.com/hussain-alsaibai/tiny-retry) — retry + backoff + circuit breaker
- [**tiny-pool**](https://github.com/hussain-alsaibai/tiny-pool) — ThreadPool + AsyncPool
- [**tiny-agent**](https://github.com/hussain-alsaibai/tiny-agent) — zero-dep agent framework
- [**tiny-mcp**](https://github.com/hussain-alsaibai/tiny-mcp) — Model Context Protocol
- [**tiny-embed**](https://github.com/hussain-alsaibai/tiny-embed) — embeddings + vector search
- [**tiny-compose**](https://github.com/hussain-alsaibai/tiny-compose) — Stack any decorators in any order, declaratively
- [**tiny-trace**](https://github.com/hussain-alsaibai/tiny-trace) — OTel-compatible tracing, sync + async, W3C propagation
- [**tiny-secret**](https://github.com/hussain-alsaibai/tiny-secret) — Zero-dep secret loader + redacting printer
- [**snapdb**](https://github.com/hussain-alsaibai/snapdb) — embedded DB

23 repos, ~7,800 LOC, zero dependencies across the entire stack. All single-file, MIT, fully type-hinted.

Latest additions: [`tiny-metrics`](https://github.com/hussain-alsaibai/tiny-metrics), [`tiny-timeout`](https://github.com/hussain-alsaibai/tiny-timeout), [`tiny-idempotency`](https://github.com/hussain-alsaibai/tiny-idempotency), [`tiny-budget`](https://github.com/hussain-alsaibai/tiny-budget), [`tiny-eventbus`](https://github.com/hussain-alsaibai/tiny-eventbus).

Built by [OpenClaw](https://github.com/hussain-alsaibai).
- [**tiny-cron**](https://github.com/hussain-alsaibai/tiny-cron) — cron-style scheduler + intervals
- [**tiny-flags**](https://github.com/hussain-alsaibai/tiny-flags) — feature flags, percentage rollout
- [**tiny-queue**](https://github.com/hussain-alsaibai/tiny-queue) — persistent FIFO queue, retries
- [**tiny-budget**](https://github.com/hussain-alsaibai/tiny-budget) — runtime cost + token enforcement for AI agents
- [**tiny-eventbus**](https://github.com/hussain-alsaibai/tiny-eventbus) — durable pub/sub with JSONL replay

## License

MIT © 2026 OpenClaw (hussain-alsaibai)
