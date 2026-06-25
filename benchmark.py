"""Benchmark for fast-cache — compare against pure dict and functools.lru_cache."""

import time
import random
import string
import functools
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fast_cache import Cache


def random_string(n=16):
    return ''.join(random.choices(string.ascii_letters, k=n))


def benchmark_set(cache, keys, values):
    start = time.perf_counter()
    for k, v in zip(keys, values):
        cache.set(k, v) if hasattr(cache, 'set') else cache.__setitem__(k, v)
    return time.perf_counter() - start


def benchmark_get(cache, keys):
    start = time.perf_counter()
    for k in keys:
        _ = cache.get(k) if hasattr(cache, 'get') else cache[k]
    return time.perf_counter() - start


def benchmark_mixed(cache, keys, values, ops=100_000):
    start = time.perf_counter()
    for i in range(ops):
        op = random.random()
        idx = i % len(keys)
        if op < 0.7:
            # 70% reads
            if hasattr(cache, 'get'):
                cache.get(keys[idx])
            else:
                _ = cache.get(keys[idx], None)
        elif op < 0.9:
            # 20% writes
            if hasattr(cache, 'set'):
                cache.set(keys[idx], values[idx])
            else:
                cache[keys[idx]] = values[idx]
        else:
            # 10% deletes
            if hasattr(cache, 'delete'):
                cache.delete(keys[idx])
            else:
                cache.pop(keys[idx], None)
    return time.perf_counter() - start


def run_benchmark(name, n_keys=10_000, ops=100_000):
    print(f"\n{'=' * 60}")
    print(f"Benchmark: {name}")
    print(f"Keys: {n_keys:,} | Mixed ops: {ops:,}")
    print(f"{'=' * 60}\n")

    # Generate keys and values
    keys = [f"key_{i:06d}" for i in range(n_keys)]
    values = [random_string(32) for _ in range(n_keys)]

    # FastCache (unlimited)
    fc = Cache()
    t = benchmark_set(fc, keys, values)
    print(f"  fast_cache.set():     {t:.3f}s ({n_keys / t / 1000:.0f}K ops/sec)")

    # Pure dict
    d = {}
    t = benchmark_set(d, keys, values)
    print(f"  dict.__setitem__:      {t:.3f}s ({n_keys / t / 1000:.0f}K ops/sec)")

    # FastCache get (full hit)
    t = benchmark_get(fc, keys)
    print(f"  fast_cache.get():     {t:.3f}s ({n_keys / t / 1000:.0f}K ops/sec)")

    # Dict get (full hit)
    t = benchmark_get(d, keys)
    print(f"  dict.__getitem__:      {t:.3f}s ({n_keys / t / 1000:.0f}K ops/sec)")

    # Mixed workload
    fc2 = Cache(maxsize=n_keys)
    t = benchmark_mixed(fc2, keys, values, ops)
    print(f"\n  fast_cache mixed:       {t:.3f}s ({ops / t / 1000:.0f}K ops/sec)")

    d2 = {}
    t = benchmark_mixed(d2, keys, values, ops)
    print(f"  dict mixed:             {t:.3f}s ({ops / t / 1000:.0f}K ops/sec)")

    # FastCache info
    print(f"\n  fast_cache hit rate:    {fc2.stats.hit_rate:.1%}")
    print(f"  fast_cache evictions:   {fc2.stats.evictions}")

    # Size
    print(f"  fast_cache memory:      {fc2.memory_usage:,} bytes")


def benchmark_ttl():
    print(f"\n{'=' * 60}")
    print("Benchmark: TTL overhead")
    print(f"{'=' * 60}\n")

    keys = [f"key_{i:06d}" for i in range(10_000)]
    values = [random_string(32) for _ in range(10_000)]

    # With TTL
    fc_ttl = Cache(ttl=3600)
    t = benchmark_set(fc_ttl, keys, values)
    print(f"  fast_cache + TTL:       {t:.3f}s ({10_000 / t / 1000:.0f}K ops/sec)")

    # Without TTL
    fc_no = Cache()
    t = benchmark_set(fc_no, keys, values)
    print(f"  fast_cache no TTL:      {t:.3f}s ({10_000 / t / 1000:.0f}K ops/sec)")


def benchmark_concurrent():
    print(f"\n{'=' * 60}")
    print("Benchmark: Concurrent writes")
    print(f"{'=' * 60}\n")

    import threading

    cache = Cache(maxsize=100_000)
    errors = []
    per_thread = 10_000
    n_threads = 10

    def worker(thread_id):
        try:
            for i in range(per_thread):
                key = f"t{thread_id}_k{i}"
                cache.set(key, i)
                cache.get(key)
                if i % 100 == 0:
                    cache.delete(key)
        except Exception as e:
            errors.append(str(e))

    start = time.perf_counter()
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - start

    total_ops = per_thread * n_threads * 3  # set + get + delete every 100th
    print(f"  Threads: {n_threads} | Ops/thread: {per_thread}")
    print(f"  Elapsed: {elapsed:.3f}s")
    print(f"  Throughput: {total_ops / elapsed / 1000:.0f}K ops/sec")
    print(f"  Errors: {len(errors)}")
    print(f"  Final size: {cache.size}")


if __name__ == "__main__":
    print("🚀 fast-cache benchmark\n")

    run_benchmark("Small Dataset", n_keys=1_000, ops=10_000)
    run_benchmark("Medium Dataset", n_keys=10_000, ops=100_000)
    run_benchmark("Large Dataset", n_keys=100_000, ops=500_000)

    benchmark_ttl()
    benchmark_concurrent()

    print(f"\n{'=' * 60}")
    print("All benchmarks complete!")
    print(f"{'=' * 60}")
