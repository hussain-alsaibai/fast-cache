"""Tests for fast-cache. Run with `python test_fast_cache.py`."""

import asyncio
import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import fast_cache as fc


class TestBasic(unittest.TestCase):
    def test_set_get(self):
        c = fc.Cache(max_size=10)
        c.set("a", 1)
        self.assertEqual(c.get("a"), 1)
        self.assertIsNone(c.get("b"))
        self.assertEqual(c.get("b", "default"), "default")

    def test_overwrite(self):
        c = fc.Cache(max_size=10)
        c.set("a", 1)
        c.set("a", 2)
        self.assertEqual(c.get("a"), 2)
        self.assertEqual(len(c), 1)

    def test_delete(self):
        c = fc.Cache(max_size=10)
        c.set("a", 1)
        self.assertTrue(c.delete("a"))
        self.assertIsNone(c.get("a"))
        self.assertFalse(c.delete("b"))

    def test_clear(self):
        c = fc.Cache(max_size=10)
        c.set("a", 1)
        c.set("b", 2)
        c.clear()
        self.assertEqual(len(c), 0)

    def test_bulk_get_set_and_keys(self):
        c = fc.Cache(max_size=10)
        c.set_many({"a": 1, "b": 2})
        self.assertEqual(c.get_many(["a", "b", "c"], default="MISS"), {"a": 1, "b": 2, "c": "MISS"})
        self.assertEqual(c.keys(), ["a", "b"])
        self.assertEqual(c.stats()["hits"], 2)
        self.assertEqual(c.stats()["misses"], 1)


class TestLRUEviction(unittest.TestCase):
    def test_evict_lru(self):
        c = fc.Cache(max_size=2)
        c.set("a", 1)
        c.set("b", 2)
        c.get("a")  # a is now MRU
        c.set("c", 3)  # evicts b
        self.assertEqual(c.get("a"), 1)
        self.assertIsNone(c.get("b"))
        self.assertEqual(c.get("c"), 3)
        self.assertEqual(c.stats()["evictions"], 1)


class TestTTL(unittest.TestCase):
    def test_ttl_expiry(self):
        c = fc.Cache(max_size=10, default_ttl=0.05)
        c.set("a", 1)
        self.assertEqual(c.get("a"), 1)
        time.sleep(0.1)
        self.assertEqual(c.get("a", "MISS"), "MISS")
        self.assertEqual(c.stats()["expirations"], 1)

    def test_per_entry_ttl(self):
        c = fc.Cache(max_size=10, default_ttl=None)
        c.set("short", 1, ttl=0.05)
        c.set("long", 2, ttl=10.0)
        time.sleep(0.1)
        self.assertEqual(c.get("short", "MISS"), "MISS")
        self.assertEqual(c.get("long"), 2)

    def test_prune_removes_expired_entries(self):
        c = fc.Cache(max_size=10, default_ttl=0.05)
        c.set_many({"a": 1, "b": 2})
        time.sleep(0.1)
        self.assertEqual(c.prune(), 2)
        self.assertEqual(len(c), 0)
        self.assertEqual(c.stats()["expirations"], 2)

    def test_stale_while_revalidate(self):
        c = fc.Cache(max_size=10, default_ttl=0.05, stale_ttl=0.2)
        c.set("a", 1)
        time.sleep(0.07)  # past expiry, within stale window
        # Should return stale value
        self.assertEqual(c.get("a"), 1)


class TestContains(unittest.TestCase):
    def test_contains(self):
        c = fc.Cache(max_size=10, default_ttl=0.05, stale_ttl=0.1)
        c.set("a", 1)
        self.assertIn("a", c)
        time.sleep(0.07)
        # within stale window
        self.assertIn("a", c)


class TestDecorator(unittest.TestCase):
    def test_decorator(self):
        calls = {"n": 0}

        @fc.cache(max_size=10, ttl=1.0)
        def slow(x: int) -> int:
            calls["n"] += 1
            return x * 2

        self.assertEqual(slow(5), 10)
        self.assertEqual(slow(5), 10)
        self.assertEqual(slow(5), 10)
        self.assertEqual(calls["n"], 1)
        info = slow.cache_info()
        self.assertEqual(info["hits"], 2)
        self.assertEqual(info["misses"], 1)

    def test_decorator_kwargs(self):
        calls = {"n": 0}

        @fc.cache(max_size=10, ttl=1.0)
        def add(a: int, b: int = 0) -> int:
            calls["n"] += 1
            return a + b

        self.assertEqual(add(1, b=2), 3)
        self.assertEqual(add(1, b=2), 3)
        self.assertEqual(add(1, b=3), 4)  # different kwargs → miss
        self.assertEqual(calls["n"], 2)

    def test_cache_clear(self):
        @fc.cache(max_size=10, ttl=1.0)
        def f(x: int) -> int:
            return x

        f(1)
        f(2)
        self.assertEqual(len(f.cache), 2)
        f.cache_clear()
        self.assertEqual(len(f.cache), 0)


class TestAsyncDecorator(unittest.TestCase):
    def test_async(self):
        calls = {"n": 0}

        @fc.acache(max_size=10, ttl=1.0)
        async def afn(x: int) -> int:
            calls["n"] += 1
            return x * 3

        async def runner():
            r1 = await afn(7)
            r2 = await afn(7)
            return r1, r2

        r1, r2 = asyncio.run(runner())
        self.assertEqual(r1, 21)
        self.assertEqual(r2, 21)
        self.assertEqual(calls["n"], 1)


class TestSpecialized(unittest.TestCase):
    def test_lru(self):
        c: fc.LRUCache = fc.LRUCache(max_size=2)
        c.set("a", 1)
        c.set("b", 2)
        c.set("c", 3)  # evicts a
        self.assertIsNone(c.get("a"))

    def test_ttl_cache(self):
        c: fc.TTLCache = fc.TTLCache(max_size=10, ttl=0.05)
        c.set("a", 1)
        time.sleep(0.1)
        self.assertEqual(c.get("a", "MISS"), "MISS")


class TestThreadSafety(unittest.TestCase):
    def test_concurrent_set(self):
        import threading
        c = fc.Cache(max_size=1000)

        def worker(start: int):
            for i in range(start, start + 1000):
                c.set(f"k{i}", i)

        threads = [threading.Thread(target=worker, args=(i * 1000,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # No crash = thread-safe
        self.assertEqual(len(c), 1000)


class TestAddAndTouch(unittest.TestCase):
    """Webhook / event dedupe primitives (claim + sliding keepalive)."""

    def test_add_returns_true_for_new_key(self):
        c = fc.Cache(default_ttl=60)
        self.assertTrue(c.add("delivery-1", {"id": 1}))
        self.assertEqual(c.get("delivery-1"), {"id": 1})

    def test_add_returns_false_for_live_duplicate(self):
        c = fc.Cache(default_ttl=60)
        self.assertTrue(c.add("delivery-1", {"first": True}))
        # Replay: claim denied, original value preserved.
        self.assertFalse(c.add("delivery-1", {"second": True}))
        self.assertEqual(c.get("delivery-1"), {"first": True})

    def test_add_returns_true_after_expiry(self):
        c = fc.Cache(default_ttl=0.05)
        self.assertTrue(c.add("delivery-1", {"first": True}))
        time.sleep(0.08)
        # Expired slot is treated as absent: a fresh add wins.
        self.assertTrue(c.add("delivery-1", {"second": True}))
        self.assertEqual(c.get("delivery-1"), {"second": True})

    def test_add_no_ttl_never_blocks(self):
        c = fc.Cache()  # no TTL
        self.assertTrue(c.add("k", 1))
        self.assertFalse(c.add("k", 2))
        self.assertEqual(c.get("k"), 1)

    def test_touch_extends_live_entry(self):
        c = fc.Cache(default_ttl=0.05)
        c.add("k", "v")
        self.assertTrue(c.touch("k", ttl=60))
        time.sleep(0.08)
        self.assertEqual(c.get("k"), "v")  # still live after sleep

    def test_touch_returns_false_for_missing(self):
        c = fc.Cache(default_ttl=60)
        self.assertFalse(c.touch("missing"))

    def test_touch_returns_false_for_expired(self):
        c = fc.Cache(default_ttl=0.05)
        c.add("k", "v")
        time.sleep(0.08)
        self.assertFalse(c.touch("k"))

    def test_webhook_dedupe_recipe(self):
        """End-to-end webhook dedupe with the add() primitive."""
        c = fc.Cache(default_ttl=300)  # 5-minute replay window

        def receive(delivery_id: str, payload: dict) -> str:
            if c.add(delivery_id, payload):
                return "accepted"
            return "duplicate"

        self.assertEqual(receive("d-1", {"event": "a"}), "accepted")
        self.assertEqual(receive("d-1", {"event": "a-replay"}), "duplicate")
        self.assertEqual(receive("d-2", {"event": "b"}), "accepted")
        # Stats reflect the dedupe: 2 hits on the cache layer (both first
        # inserts), no second insert on replay (which is what we want).
        stats = c.stats()
        self.assertGreaterEqual(stats["size"], 2)


if __name__ == "__main__":
    unittest.main()
