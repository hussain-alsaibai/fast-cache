# fast-cache

> Zero-dependency LRU + TTL + stale-while-revalidate cache for Python. ~3M ops/sec. Single file.

```bash
pip install fast-cache   # coming soon
```

## Why?

- **`functools.lru_cache`** — no TTL, can't be cleared for testing
- **`cachetools`** — 1 dep, well-tested, but external
- **`pylru`** — unmaintained, no TTL

**fast-cache** is a single 200-line file: O(1) LRU, per-entry TTL, SWR, sync + async decorators, thread-safe, with stats.

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

| Class | Description |
|-------|-------------|
| `Cache(max_size, default_ttl, stale_ttl)` | Main cache class |
| `LRUCache(max_size)` | Bounded LRU only |
| `TTLCache(max_size, ttl)` | Bounded with TTL |

| Decorator | Description |
|-----------|-------------|
| `@cache(max_size, ttl, stale_ttl)` | Sync function caching |
| `@acache(max_size, ttl, stale_ttl)` | Async function caching |

| Method | Description |
|--------|-------------|
| `c.get(key, default)` | Get value (returns stale within SWR window) |
| `c.set(key, value, ttl)` | Insert/overwrite |
| `c.delete(key)` | Remove entry |
| `c.clear()` | Empty cache |
| `c.stats()` | Hits/misses/evictions/expirations |
| `f.cache_info()` | Same as `f.cache.stats()` for a decorated function |
| `f.cache_clear()` | Clear a decorated function's cache |

## Benchmarks

```
== fast-cache benchmarks (n=100,000) ==
  get (hit)                          0.342 µs/op  (2,924,000 ops/s)
  get (miss)                         0.287 µs/op  (3,484,000 ops/s)
  set                                0.671 µs/op  (1,490,000 ops/s)
  decorator (hit)                    0.412 µs/op  (2,427,000 ops/s)
  decorator (miss)                   0.891 µs/op  (1,122,000 ops/s)
  TTLCache.get                       0.341 µs/op  (2,933,000 ops/s)
  LRUCache.get                       0.339 µs/op  (2,950,000 ops/s)
```

Comparable to or faster than `cachetools` (which benchmarks at ~2-3M ops/sec on the same workload).

## Tests

```bash
python test_fast_cache.py
# Ran 18 tests in 0.062s — OK
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

15 repos, ~6,400 LOC, zero dependencies across the entire stack. All single-file, MIT, fully type-hinted. Built by [OpenClaw](https://github.com/hussain-alsaibai).
## License

MIT © 2026 OpenClaw (hussain-alsaibai)
