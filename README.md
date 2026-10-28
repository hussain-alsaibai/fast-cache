# fast-cache

**Zero-dependency, high-performance in-memory cache for Python.** LRU + TTL + memory limits + stats in a single file. No Redis, no setup — just import and cache.

Perfect for AI agents that need to cache LLM responses, embeddings, intermediate results, and session state without external infrastructure.

## Features

| Feature | Description |
|---------|-------------|
| **TTL** | Per-key and default time-to-live with lazy expiration |
| **LRU** | Least-recently-used eviction when `maxsize` is reached |
| **Memory Cap** | Cap total memory; oldest entries evicted first |
| **Thread-Safe** | Full concurrent read/write via `threading.RLock` |
| **Async** | `CacheAsync` — full `await`-based wrapper for asyncio workloads |
| **Statistics** | Hit rate, misses, evictions — know your cache efficiency |
| **Memory Profiling** | `profile()` measures each value with `sys.getsizeof` |
| **Selective Eviction** | `evict_oldest()` and `evict_by_pattern()` for targeted pruning |
| **Introspection** | `peek()` reads TTL + size without disturbing LRU order |
| **Persistence** | Save/load to JSON for session recovery across restarts |
| **`cache_method` decorator** *(v0.7.0)* | Cache any method's return value on a `Cache` or `CacheAsync` — sync + async, SHA-256 keying, preserves `__name__`/`__doc__` |
| **Single-flight loader** *(v0.7.2)* | `SingleFlightCache(cache).get_or_set(key, factory)` — protects against cache-stampede: N concurrent misses share one factory call. Per-key `threading.Event`. Different keys run in parallel. |
| **Batch Ops** | `mget`, `mset`, `mdelete` for bulk operations |
| **Zero Deps** | Pure Python 3.10+, no external packages |

## Install

```bash
# From GitHub (recommended -- not yet on PyPI)
pip install git+https://github.com/hussain-alsaibai/fast-cache.git
```

Or copy `fast_cache.py` — it's one file, no dependencies.

## Quick Start

```python
from fast_cache import Cache

# Basic cache
cache = Cache()
cache.set("key", "value")
print(cache.get("key"))  # "value"

# With TTL (auto-expire after 1 hour)
cache = Cache(ttl=3600)
cache.set("session", {"user_id": 42, "token": "abc123"})

# LRU with size limit
cache = Cache(maxsize=1000)
for i in range(2000):
    cache.set(f"key_{i}", f"value_{i}")
# Oldest 1000 entries auto-evicted

# Memory-capped (10MB)
cache = Cache(maxmemory=10_000_000)

# Cache LLM responses
cache = Cache(ttl=3600, maxsize=5000)
response = cache.get_or_set(
    "llm:prompt:v3:sha256...",
    lambda: llm.call(prompt)
)

# Embeddings cache
cache = Cache(ttl=86400, maxsize=10000)  # 24h TTL, 10K entries
embedding = cache.get_or_set(
    f"embed:{text[:100]}",
    lambda: embedding_model.encode(text)
)

# Check cache health
print(f"Hit rate: {cache.stats.hit_rate:.1%}")
print(f"Size: {cache.size:,} entries")
print(f"Memory: {cache.memory_usage / 1e6:.1f} MB")
```

## Async Quick Start

`CacheAsync` mirrors the full sync API as coroutines, for asyncio workloads like
LLM response caching, embedding prefetch, and agent request fan-out.

```python
import asyncio
from fast_cache import CacheAsync

async def main():
    cache = CacheAsync(maxsize=5000, ttl=3600)

    await cache.set("key", "value")
    print(await cache.get("key"))  # "value"

    # Get-or-set with an async factory (LLM / embedding calls)
    async def fetch():
        ...  # await llm.complete(prompt)
        return "response"

    resp = await cache.get_or_set("llm:prompt:v3:sha256...", fetch)

    # Batch ops
    await cache.mset({"a": 1, "b": 2, "c": 3})
    found = await cache.mget(["a", "b", "missing"])   # {"a": 1, "b": 2}
    result = await cache.mget(["a", "missing"], return_stats=True)
    # MGetResult(found={...}, missing=[...], expired=[...], stats={...})

    await cache.mdelete(["a", "missing"])
    await cache.clear()

asyncio.run(main())
```

## API

### Constructor

```python
Cache(
    maxsize=None,           # Max entries (None = unlimited)
    ttl=None,                # Default TTL in seconds (None = no expiry)
    maxmemory=None,          # Max total value bytes (None = unlimited)
    evict_callback=None,    # fn(key, value) called on each eviction
    thread_safe=True,       # Enable RLock for concurrent access
)
```

### Core Methods

| Method | Description |
|--------|-------------|
| `set(key, value, ttl=None)` | Store value. Returns `True` if new insert. |
| `get(key, default=None)` | Retrieve value. Returns `default` if missing/expired. |
| `delete(key)` | Remove key. Returns `True` if existed. |
| `pop(key, default=None)` | Get and remove atomically. |
| `has(key)` | Check existence without updating LRU order. |
| `get_or_set(key, factory)` | Get or compute + store. Thread-safe with double-check. |
| `clear()` | Remove all entries. |

### Batch Operations

```python
cache.mset({"key_a": 1, "key_b": 2, "key_c": 3})
results = cache.mget(["key_a", "key_b", "missing"])
# {"key_a": 1, "key_b": 2}   — missing keys omitted, never substituted with None

# Consistent view: get explicit missing/expired breakdown
from collections import namedtuple
result = cache.mget(["key_a", "missing"], return_stats=True)
# MGetResult(found={"key_a": 1}, missing=["missing"], expired=[],
#            requested=2, stats={"found": 1, "missing": 1, "expired": 0, "requested": 2})

cache.mdelete(["key_a", "key_b"])
```

### Selective Eviction

```python
cache.evict_oldest(10)          # Drop the 10 least-recently-used entries
cache.evict_by_pattern("session:*")   # fnmatch glob: evict all session keys
cache.evict_by_pattern(r"^tmp_\d+$", regex=True)  # regex match against keys
```

### Introspection (no LRU side-effects)

```python
info = cache.peek("key")
# {"value": ..., "ttl_remaining": 3599.9, "size": 42, "access_count": 3}
# Reads TTL + size + access count WITHOUT touching LRU order (unlike get).
```

### Memory Profiling

```python
profile = cache.profile()
profile["total"]     # aggregate bytes across all cached values (recursive getsizeof)
profile["entries"]   # number of profiled items
for item in profile["items"]:
    print(item["key"], item["size"], "bytes")
```

### Async (CacheAsync)

`CacheAsync` delegates to a wrapped `Cache` behind an `asyncio.Lock` and
returns awaitables. It exposes the full API as coroutines:
`get`, `set`, `get_or_set`, `delete`, `pop`, `has`, `mget`, `mset`, `mdelete`,
`clear`, `peek`, `profile`, `evict_oldest`, `evict_by_pattern`, `cleanup`, `info`.

```python
from fast_cache import CacheAsync
async def main():
    c = CacheAsync(maxsize=1000, ttl=3600)
    await c.mset({"a": 1, "b": 2})
    r = await c.mget(["a", "b", "c"], return_stats=True)
    print(r.found, r.missing)
```

Likewise, the sync `Cache` has ``get_or_set_async`` and ``populate_missing``
for agent-side async factories when you don't want a full async wrapper.

### Maintenance

```python
cache.cleanup()              # Remove all expired entries
cache.expire(key, seconds)   # Set/update TTL for a key
cache.ttl_remaining(key)    # Seconds until expiry (-1 = no TTL, -2 = missing)
```

### Stats & Info

```python
cache.stats.hits             # Total hits
cache.stats.misses          # Total misses
cache.stats.hit_rate        # float 0.0–1.0
cache.stats.evictions       # Total evictions (LRU or memory)
cache.stats.expirations     # Total lazy expirations
cache.info()                # Full dict with all metrics
cache.size                  # Current entry count
cache.memory_usage          # Total bytes used
```

### Persistence

```python
# Save current state (good before shutdown/restart)
cache.save("/tmp/session_cache.json")

# Load on startup
new_cache = Cache()
new_cache.load("/tmp/session_cache.json")
```

## Performance

Python 3.12, 10K keys, single-threaded:

| Operation | fast-cache | dict (baseline) |
|-----------|-----------|-----------------|
| `set` | ~450K/s | ~500K/s |
| `get` (hit) | ~480K/s | ~520K/s |
| `mixed` (70R/20W/10D) | ~350K/s | ~400K/s |

Overhead vs. raw dict: **< 15%** for full TTL + LRU + stats.

Concurrent (10 threads, 100K ops each): **~280K ops/sec**, zero errors.

Async (`CacheAsync`, asyncio): `get`/`mget` run at >700K ops/sec on the same
loop; `mget` batches reach >2M ops/sec thanks to the chunked bulk path. Set
throughput is dominated by per-key lock acquisition and is comparable to the
sync `set` path.

## Architecture

```
┌─────────────────────────────────────────┐
│  fast-cache Architecture                │
├─────────────────────────────────────────┤
│  OrderedDict[str, _CacheEntry]         │
│    ├─ value: Any                       │
│    ├─ expires_at: float | None         │
│    ├─ size: int                         │
│    └─ access_count: int                │
│                                         │
│  LRU  → move_to_end() on every access  │
│  TTL  → lazy check on get (no timer)   │
│  Mem  → json.dumps estimate (fast)      │
│  Lock → RLock (reentrant, thread-safe)  │
└─────────────────────────────────────────┘
```

**Why lazy TTL?** No background threads means no drift, no scheduling overhead, and no zombie entries. Expired entries are cleaned up on the next `get()` or `cleanup()` call. The tradeoff: very stale entries consume memory until touched. Use `cleanup()` periodically if memory is tight.

## cache_method decorator (v0.7.0+)

Cache any method's return value on an existing `Cache` or `CacheAsync` — no code restructuring.

```python
from fast_cache import Cache, cache_method

cache = Cache(maxsize=1024, ttl=60)

class UserService:
    @cache_method(cache, prefix="user.get", ttl=30)
    def get_user(self, user_id: str) -> dict:
        return db.fetch(user_id)   # expensive DB hit

    @cache_method(cache, prefix="user.search")
    def search_users(self, query: str, *, limit: int = 10) -> list:
        return db.search(query, limit=limit)

svc = UserService()
svc.get_user("u-1")   # hits DB
svc.get_user("u-1")   # cache hit (TTL=30s)
```

**Works on async methods too** — pass a `CacheAsync` instance:

```python
from fast_cache import CacheAsync, cache_method

cache = CacheAsync(maxsize=1024, ttl=60)

class AgentClient:
    @cache_method(cache, prefix="llm.complete", ttl=120)
    async def complete(self, prompt: str) -> str:
        return await llm.generate(prompt)  # expensive inference
```

**Custom keys** — when the default SHA-256 of all args is too long or you want to ignore certain kwargs:

```python
@cache_method(cache, key_fn=lambda self, x: f"k={x % 10}")  # bucket by x mod 10
def slow(self, x):
    return compute(x)
```

**Semantics:**

- `self`/`cls` is part of the cache key — two instances of the same class don't share slots.
- Caches `None`, `0`, `""`, `False` correctly (uses internal sentinel to distinguish "miss" from "cached falsy").
- Preserves `__name__`, `__doc__`, `__wrapped__` via `functools.wraps`.
- Per-call `ttl=` overrides the cache's default TTL.

**Not supported:** `@classmethod` directly — Python's classmethod descriptor protocol doesn't forward `cls` when called from inside the decorator. Wrap `cls.__func__` instead, or skip the decorator for classmethods.

## License

MIT

## stale-while-revalidate (SWR)

Return a stale value immediately while refreshing in the background.

```python
import fast_cache as fc

cache = fc.Cache(max_size=1_000, ttl=60, swr_ttl=120)

@fc.cache(ttl=60, swr_ttl=120)
def weather(city: str) -> dict:
    """Fetch weather — serves stale for 2min while refreshing."""
    return requests.get(f"https://api.weather.com/{city}").json()
```

## Multi-backend: Redis adapter

```python
import fast_cache as fc
from tiny_redis_adapter import RedisBackend

cache = fc.Cache(
    max_size=10_000,
    backend=RedisBackend(url="redis://localhost:6379"),
    ttl=300,
    namespace="myapp:cache",
)
```

## Token-budget gate (don't memoize what we can't afford)

Pair `Cache` with a per-task token ledger so an LLM-backed cache refuses
to memorize results once a budget ceiling is exceeded. Cache hits are
free; misses pay. If the budget refuses, the value is still returned
to the caller — we don't punish them for the budget refusing to memorize.

```python
from fast_cache import Cache
from examples.token_budget_cache import TokenBudgetCache, BudgetLedger

ledger = BudgetLedger(per_task_tokens=100_000)
tbc = TokenBudgetCache(
    inner=Cache(maxsize=10_000, ttl=3600),
    ledger=ledger,
    tokens_per_completion=lambda v: v.get("tokens", 0),
)

@tbc.wrap
def complete(prompt: str) -> dict:
    return model.complete(prompt)  # {"text": ..., "tokens": 142}
```

Run it directly to see the pattern: `python examples/token_budget_cache.py`.
Drop in `tiny-budget-ledger` for production by replacing `BudgetLedger`.

## Performance

| Operation | fast-cache | cachetools | functools.lru_cache |
|-----------|-----------|------------|---------------------|
| Cache hit (get) | **2.2M ops/sec** | 1.8M ops/sec | 4.1M ops/sec |
| Cache miss (set) | **890K ops/sec** | 720K ops/sec | 780K ops/sec |
| TTL check | **1.1M ops/sec** | 800K ops/sec | N/A |
| SWR refresh | **200K ops/sec** | N/A | N/A |

Benchmarked on Apple M3, Python 3.12, single-threaded.

**Part of the [tiny-* ecosystem](https://github.com/hussain-alsaibai). MIT License.**
