"""Tests for fast-cache."""

import time
import threading
import json
import os
import tempfile

from fast_cache import Cache, CacheAsync, CacheStats


def test_basic_get_set():
    cache = Cache()
    assert cache.set("a", 1) is True
    assert cache.set("a", 2) is False  # update
    assert cache.get("a") == 2
    assert cache.get("b") is None
    assert cache.get("b", "default") == "default"
    print("✅ basic get/set")


def test_ttl_expiration():
    cache = Cache(ttl=0.1)
    cache.set("key", "value")
    assert cache.get("key") == "value"
    time.sleep(0.15)
    assert cache.get("key") is None
    assert cache.stats.expirations >= 1
    print("✅ TTL expiration")


def test_per_key_ttl():
    cache = Cache()
    cache.set("short", "x", ttl=0.05)
    cache.set("long", "y", ttl=10)
    time.sleep(0.1)
    assert cache.get("short") is None
    assert cache.get("long") == "y"
    print("✅ per-key TTL")


def test_lru_eviction():
    cache = Cache(maxsize=3)
    cache.set("a", 1)
    cache.set("b", 2)
    cache.set("c", 3)
    cache.set("d", 4)  # should evict 'a'
    assert cache.get("a") is None
    assert cache.get("b") == 2
    assert cache.get("c") == 3
    assert cache.get("d") == 4
    assert cache.stats.evictions >= 1
    print("✅ LRU eviction")


def test_lru_promotion():
    cache = Cache(maxsize=3)
    cache.set("a", 1)
    cache.set("b", 2)
    cache.set("c", 3)
    cache.get("a")  # promote 'a' to MRU
    cache.set("d", 4)  # should evict 'b' (LRU)
    assert cache.get("a") == 1
    assert cache.get("b") is None
    print("✅ LRU promotion")


def test_memory_limit():
    cache = Cache(maxmemory=100)
    cache.set("a", "x" * 40)  # ~42 bytes
    cache.set("b", "x" * 40)  # ~42 bytes, total ~84 (<100)
    assert cache.get("a") == "x" * 40  # promote 'a' to MRU
    cache.set("c", "x" * 40)  # total ~126 (>100), evict LRU ('b')
    assert cache.get("b") is None
    assert cache.get("a") == "x" * 40  # 'a' still there
    assert cache.get("c") == "x" * 40  # 'c' there
    print("✅ memory limit eviction")


def test_delete():
    cache = Cache()
    cache.set("a", 1)
    assert cache.delete("a") is True
    assert cache.delete("a") is False
    assert cache.get("a") is None
    print("✅ delete")


def test_pop():
    cache = Cache()
    cache.set("a", 1)
    assert cache.pop("a") == 1
    assert cache.pop("a", "gone") == "gone"
    print("✅ pop")


def test_has():
    cache = Cache()
    cache.set("a", 1)
    assert cache.has("a") is True
    assert cache.has("b") is False
    print("✅ has")


def test_contains():
    cache = Cache()
    cache.set("a", 1)
    assert "a" in cache
    assert "b" not in cache
    print("✅ contains")


def test_clear():
    cache = Cache()
    cache.set("a", 1)
    cache.set("b", 2)
    assert cache.clear() == 2
    assert cache.size == 0
    print("✅ clear")


def test_cleanup():
    cache = Cache()
    cache.set("a", 1, ttl=0.05)
    cache.set("b", 2)
    time.sleep(0.1)
    removed = cache.cleanup()
    assert removed >= 1
    assert cache.get("a") is None
    assert cache.get("b") == 2
    print("✅ cleanup")


def test_mget():
    cache = Cache()
    cache.set("a", 1)
    cache.set("b", 2)
    result = cache.mget(["a", "b", "c"])
    assert result == {"a": 1, "b": 2}
    print("✅ mget")


def test_mset():
    cache = Cache()
    count = cache.mset({"a": 1, "b": 2, "c": 3})
    assert count == 3
    assert cache.get("a") == 1
    print("✅ mset")


def test_mdelete():
    cache = Cache()
    cache.mset({"a": 1, "b": 2, "c": 3})
    count = cache.mdelete(["a", "b", "x"])
    assert count == 2
    assert cache.get("a") is None
    assert cache.get("c") == 3
    print("✅ mdelete")


def test_get_or_set():
    cache = Cache()
    # First call: factory runs
    v1 = cache.get_or_set("key", lambda: "computed")
    assert v1 == "computed"
    # Second call: cached
    v2 = cache.get_or_set("key", lambda: "ignored")
    assert v2 == "computed"
    print("✅ get_or_set")


def test_expire():
    cache = Cache()
    cache.set("a", 1, ttl=10)
    cache.expire("a", 0.05)
    time.sleep(0.1)
    assert cache.get("a") is None
    print("✅ expire")


def test_ttl_remaining():
    cache = Cache()
    cache.set("a", 1, ttl=10)
    remaining = cache.ttl_remaining("a")
    assert remaining is not None
    assert 5 < remaining <= 10
    print("✅ ttl_remaining")


def test_stats():
    cache = Cache()
    cache.set("a", 1)
    cache.get("a")
    cache.get("b")  # miss
    assert cache.stats.hits == 1
    assert cache.stats.misses == 1
    assert cache.stats.inserts == 1
    assert 0.0 < cache.stats.hit_rate <= 1.0
    print("✅ stats")


def test_info():
    cache = Cache(maxsize=100, ttl=60)
    cache.set("a", 1)
    info = cache.info()
    assert "entries" in info
    assert info["maxsize"] == 100
    assert info["default_ttl"] == 60
    print("✅ info")


def test_persistence():
    cache = Cache()
    cache.set("a", 1)
    cache.set("b", [2, 3])
    cache.set("c", {"nested": True})

    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        path = f.name

    try:
        cache.save(path)
        cache2 = Cache()
        count = cache2.load(path)
        assert count == 3
        assert cache2.get("a") == 1
        assert cache2.get("b") == [2, 3]
        assert cache2.get("c") == {"nested": True}
    finally:
        os.unlink(path)
    print("✅ persistence")


def test_evict_callback():
    evicted = []

    def on_evict(key, value):
        evicted.append((key, value))

    cache = Cache(maxsize=2, evict_callback=on_evict)
    cache.set("a", 1)
    cache.set("b", 2)
    cache.set("c", 3)  # evicts 'a'
    assert len(evicted) == 1
    assert evicted[0] == ("a", 1)
    print("✅ evict callback")


def test_thread_safety():
    cache = Cache(maxsize=1000)
    errors = []

    def worker(n):
        try:
            for i in range(100):
                key = f"worker{n}_key{i}"
                cache.set(key, i)
                cache.get(key)
                if i % 10 == 0:
                    cache.delete(key)
        except Exception as e:
            errors.append(str(e))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Thread errors: {errors}"
    assert cache.size > 0
    print("✅ thread safety")


def test_iteration():
    cache = Cache()
    cache.set("a", 1)
    cache.set("b", 2)
    keys = list(cache.keys())
    values = list(cache.values())
    items = list(cache.items())
    assert set(keys) == {"a", "b"}
    assert set(values) == {1, 2}
    assert set(items) == {("a", 1), ("b", 2)}
    print("✅ iteration")


def test_repr():
    cache = Cache(maxsize=100)
    assert "Cache" in repr(cache)
    assert "maxsize=100" in repr(cache)
    print("✅ repr")


def test_no_ttl_never_expires():
    cache = Cache()
    cache.set("forever", "value")
    time.sleep(0.1)
    assert cache.get("forever") == "value"
    print("✅ no-TTL never expires")


def test_empty_cache_behavior():
    cache = Cache()
    assert cache.size == 0
    assert len(cache) == 0
    assert cache.memory_usage == 0
    assert list(cache.keys()) == []
    assert cache.info()["entries"] == 0
    print("✅ empty cache behavior")


def test_stats_dict():
    stats = CacheStats()
    stats.hit()
    stats.miss()
    stats.insert()
    d = stats.to_dict()
    assert d["hits"] == 1
    assert d["misses"] == 1
    assert d["inserts"] == 1
    assert "hit_rate" in d
    print("✅ stats dict")


def test_peek_no_lru_effect():
    cache = Cache(maxsize=3)
    cache.set("a", 1)
    cache.set("b", 2)
    cache.set("c", 3)
    info = cache.peek("a")
    assert info is not None
    assert info["value"] == 1
    assert info["access_count"] == 0  # peek must NOT touch the entry
    assert "ttl_remaining" in info
    assert "size" in info
    # Peek must not promote 'a' to MRU: inserting 'd' evicts 'a'.
    cache.set("d", 4)
    assert cache.get("a") is None
    assert cache.get("b") == 2
    print("✅ peek (no LRU effect)")


def test_mget_consistency_stats():
    cache = Cache()
    cache.mset({"a": 1, "b": 2})
    cache.set("gone", 3, ttl=0.01)
    import time as _t
    _t.sleep(0.02)  # let 'gone' expire
    result = cache.mget(["a", "b", "gone", "missing"], return_stats=True)
    assert result.found == {"a": 1, "b": 2}
    assert "missing" in result.missing
    assert "gone" in result.expired
    assert result.stats["found"] == 2
    assert result.stats["missing"] == 1
    assert result.stats["expired"] == 1
    assert result.stats["requested"] == 4
    # Default (no stats) still returns a plain dict of found items only.
    plain = cache.mget(["a", "missing"])
    assert plain == {"a": 1}
    print("✅ mget consistency stats")


def test_profile():
    cache = Cache()
    cache.set("small", 42)
    cache.set("big", list(range(1000)))
    profile = cache.profile()
    assert profile["entries"] == 2
    sizes = {item["key"]: item["size"] for item in profile["items"]}
    assert sizes["big"] > sizes["small"]
    assert profile["total"] == sizes["small"] + sizes["big"]
    print("✅ profile")


def test_evict_oldest():
    cache = Cache()
    cache.mset({"a": 1, "b": 2, "c": 3, "d": 4})
    assert cache.evict_oldest(2) == 2
    assert cache.get("a") is None
    assert cache.get("b") is None
    assert cache.get("c") == 3
    assert cache.evict_oldest(100) == 2  # only 2 left
    assert cache.size == 0
    print("✅ evict_oldest")


def test_evict_by_pattern():
    cache = Cache()
    cache.mset({"session:a": 1, "session:b": 2, "other": 3})
    assert cache.evict_by_pattern("session:*") == 2
    assert cache.get("session:a") is None
    assert cache.get("other") == 3
    cache.mset({"tmp_x": 1, "tmp_2": 2})
    assert cache.evict_by_pattern(r"^tmp_\d+$", regex=True) == 1
    assert cache.get("tmp_x") == 1
    assert cache.get("tmp_2") is None
    print("✅ evict_by_pattern")


def test_cache_async():
    import asyncio

    async def _run():
        cache = CacheAsync(maxsize=10, ttl=60)
        assert await cache.set("a", 1) is True
        assert await cache.get("a") == 1
        assert await cache.get("missing", "d") == "d"
        await cache.mset({"x": 1, "y": 2})
        assert await cache.mget(["x", "y", "z"]) == {"x": 1, "y": 2}
        r = await cache.mget(["x", "z"], return_stats=True)
        assert "z" in r.missing
        assert await cache.mdelete(["x", "z"]) == 1
        p = await cache.profile()
        assert p["entries"] >= 2
        assert await cache.peek("y") is not None
        assert await cache.evict_by_pattern("*") >= 1
        assert await cache.clear() >= 0

    asyncio.run(_run())
    print("✅ CacheAsync")


def run_all_tests():
    print("🧪 Running fast-cache test suite...\n")
    tests = [
        test_basic_get_set,
        test_ttl_expiration,
        test_per_key_ttl,
        test_lru_eviction,
        test_lru_promotion,
        test_memory_limit,
        test_delete,
        test_pop,
        test_has,
        test_contains,
        test_clear,
        test_cleanup,
        test_mget,
        test_mset,
        test_mdelete,
        test_get_or_set,
        test_expire,
        test_ttl_remaining,
        test_stats,
        test_info,
        test_persistence,
        test_evict_callback,
        test_thread_safety,
        test_iteration,
        test_repr,
        test_no_ttl_never_expires,
        test_empty_cache_behavior,
        test_stats_dict,
        test_peek_no_lru_effect,
        test_mget_consistency_stats,
        test_profile,
        test_evict_oldest,
        test_evict_by_pattern,
        test_cache_async,
    ]

    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"❌ {test.__name__}: {e}")
            failed += 1

    print(f"\n{'=' * 40}")
    print(f"Results: {passed} passed, {failed} failed")
    return failed == 0


if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    ok = run_all_tests()
    sys.exit(0 if ok else 1)
