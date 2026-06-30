"""Benchmarks for fast-cache. Run with `python bench_fast_cache.py`."""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import fast_cache as fc


def bench(name, fn, n=100_000):
    fn()  # warmup
    t0 = time.perf_counter()
    for _ in range(n):
        fn()
    dt = (time.perf_counter() - t0) / n * 1e6
    print(f"  {name:30s} {dt:8.3f} µs/op  ({1e6 / dt:,.0f} ops/s)")


def main():
    print("== fast-cache benchmarks (n=100,000) ==")
    c = fc.Cache(max_size=10_000)

    # Pre-populate
    for i in range(10_000):
        c.set(f"k{i}", i)

    bench("get (hit)", lambda: c.get("k5000"))
    bench("get (miss)", lambda: c.get("k_xxx"))
    bench("set", lambda: c.set("k_5000", 5000))

    # decorator
    @fc.cache(max_size=10_000, ttl=60.0)
    def add(x: int) -> int:
        return x * 2

    # Prime the cache
    for i in range(10_000):
        add(i)

    bench("decorator (hit)", lambda: add(5000))
    bench("decorator (miss)", lambda: add(99_999))

    # TTL cache
    ttl_c = fc.TTLCache(max_size=10_000, ttl=60.0)
    for i in range(10_000):
        ttl_c.set(f"k{i}", i)
    bench("TTLCache.get", lambda: ttl_c.get("k5000"))

    # LRU
    lru_c = fc.LRUCache(max_size=10_000)
    for i in range(10_000):
        lru_c.set(f"k{i}", i)
    bench("LRUCache.get", lambda: lru_c.get("k5000"))


if __name__ == "__main__":
    main()
