"""Reproducible benchmark for fast-cache v0.8.0 (SWR + namespacing).

Run:  python benchmark_swr.py

Every number in the README Performance table comes from this script. It
compares the cache paths against functools.lru_cache, which is the honest
baseline: lru_cache is faster on a pure hit because it does no clock read
and no memory accounting, and this script is meant to show that rather than
hide it.
"""
from __future__ import annotations

import functools
import sys
import timeit
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from _v080 import SwrCache, namespaced  # noqa: E402
from fast_cache import Cache  # noqa: E402

ITER = 50_000
WARM = 100


def _fmt(label: str, secs: float) -> str:
    return "%-24s %10s ops/sec" % (label, format(round(ITER / secs), ","))


def _best(fn, repeat: int = 3):
    return min(timeit.repeat(fn, number=1, repeat=repeat))


def main() -> int:
    cache = Cache()
    for i in range(WARM):
        cache.set("k%d" % i, i)

    def cache_hit():
        for i in range(ITER):
            cache.get("k%d" % (i % WARM))

    lru = functools.lru_cache(maxsize=WARM)(lambda i: i)
    for i in range(WARM):
        lru(i)

    def lru_hit():
        for i in range(ITER):
            lru(i % WARM)

    def cache_set():
        for i in range(ITER):
            cache.set("s%d" % (i % WARM), i)

    small = Cache(maxsize=50)

    def cache_evict():
        for i in range(ITER):
            small.set("e%d" % (i % WARM), i)

    swr = SwrCache(Cache(), swr_window=1e9)
    swr.get_or_refresh("w", lambda: 1, ttl=1e9)

    def swr_stale_serve():
        for i in range(ITER):
            swr.get_or_refresh("w", lambda: 2, ttl=1e9)

    ns = namespaced(Cache(), "bench")

    def ns_roundtrip():
        for i in range(ITER):
            ns.set("n%d" % (i % WARM), i)
            ns.get("n%d" % (i % WARM))

    print("fast-cache benchmark: ITER=%d, warm=%d, repeat=3 (best of)"
          % (ITER, WARM))
    print("-" * 46)
    for label, fn in [
        ("cache hit (+TTL check)", cache_hit),
        ("lru_cache hit", lru_hit),
        ("cache set", cache_set),
        ("cache set (evicting)", cache_evict),
        ("SWR stale serve", swr_stale_serve),
        ("namespace set+get", ns_roundtrip),
    ]:
        print(_fmt(label, _best(fn)))

    swr.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
