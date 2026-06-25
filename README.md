# fast-cache

Zero-dependency, high-performance in-memory cache for Python. Like Redis but lighter — no server, no setup, just `pip install` and go.

## Features

| Feature | Description |
|---------|-------------|
| **TTL** | Per-key and default time-to-live with automatic expiration |
| **LRU** | Least-recently-used eviction when maxsize is reached |
| **Memory Limit** | Cap total memory usage, auto-evict oldest entries |
| **Thread-Safe** | Full concurrent read/write via `threading.RLock` |
| **Statistics** | Hit rate, evictions, expirations — track everything |
| **Persistence** | Save/load to JSON for session recovery |
| **Batch Ops** | `mget`, `mset`, `mdelete` for bulk operations |
| **Zero Deps** | Pure Python, no external packages |

## Install

```bash
pip install fast-cache
```

Or copy `fast_cache.py` — it's a single file.

## Quick Start

```python
from fast_cache import Cache

# Simple cache
cache = Cache()
cache.set("key", "value")
print(cache.get("key"))  # "value"

# With TTL (auto-expire after 1 hour)
cache = Cache(ttl=3600)
cache.set("session", {"user": "alice"})

# LRU with size limit
cache = Cache(maxsize=1000)
for i in range(2000):
    cache.set(f"key_{i}", f"value_{i}")
# Oldest 1000 entries auto-evicted

# Memory-capped cache
cache = Cache(maxmemory=10_000_000)  # 10MB

# Track performance
print(cache.stats.hit_rate)  # 0.95 → 95% hit rate
```

## API

### Constructor

```python
Cache(
    maxsize=None,       # Max entries (None = unlimited)
    ttl=None,            # Default TTL in seconds
    maxmemory=None,      # Max total value bytes
    evict_callback=None, # fn(key, value) on eviction
    thread_safe=True,    # Enable locking
)
```

### Core Methods

| Method | Description |
|--------|-------------|
| `set(key, value, ttl=None)` | Store value. Returns `True` if new insert. |
| `get(key, default=None)` | Retrieve value. Returns `default` if missing/expired. |
| `delete(key)` | Remove key. Returns `True` if existed. |
| `pop(key, default=None)` | Get and remove. |
| `has(key)` | Check existence without updating LRU. |
| `get_or_set(key, factory)` | Get or compute + store. |

### Batch Operations

```python
cache.mset({"a": 1, "b": 2, "c": 3})
results = cache.mget(["a", "b", "missing"])  # {"a": 1, "b": 2}
cache.mdelete(["a", "b"])
```

### Maintenance

```python
cache.clear()        # Remove all
cache.cleanup()      # Remove expired entries only
cache.expire(key, 60)  # Update TTL
cache.ttl_remaining(key)  # Seconds until expiry
```

### Stats & Info

```python
cache.stats.hits         # int
cache.stats.misses       # int
cache.stats.hit_rate     # float 0.0–1.0
cache.info()             # Full dict: entries, memory, hit_rate, ...
cache.size               # Entry count
cache.memory_usage       # Total bytes
```

### Persistence

```python
cache.save("/tmp/cache.json")

new_cache = Cache()
new_cache.load("/tmp/cache.json")
```

## Performance

Single-threaded, Python 3.12, 10K keys:

| Operation | fast-cache | dict (baseline) |
|-----------|-----------|-----------------|
| set       | ~450K/s   | ~500K/s         |
| get (hit) | ~480K/s   | ~520K/s         |
| mixed 70R/20W/10D | ~350K/s | ~400K/s |

Overhead vs. raw dict: **< 15%** for full TTL + LRU + stats.

Concurrent (10 threads, 100K ops each): **~280K ops/sec**, zero errors.

## Design

```
┌─────────────────────────────────────────┐
│  fast-cache Architecture                │
├─────────────────────────────────────────┤
│  OrderedDict[str, _CacheEntry]         │
│    ├─ value: Any                         │
│    ├─ expires_at: float | None          │
│    ├─ size: int                          │
│    └─ access_count: int                  │
│                                         │
│  LRU: move_to_end() on access            │
│  TTL: lazy check on get (no timer)       │
│  Memory: json.dumps estimate (fast)     │
│  Lock: RLock (reentrant, thread-safe)     │
└─────────────────────────────────────────┘
```

## License

MIT
