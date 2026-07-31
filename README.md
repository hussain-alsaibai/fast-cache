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
| **Statistics** | Hit rate, misses, evictions — know your cache efficiency |
| **Persistence** | Save/load to JSON for session recovery across restarts |
| **Batch Ops** | `mget`, `mset`, `mdelete` for bulk operations |
| **Zero Deps** | Pure Python 3.10+, no external packages |

## Install

```bash
pip install fast-cache
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
# {"key_a": 1, "key_b": 2}

cache.mdelete(["key_a", "key_b"])
```

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

## License

MIT
