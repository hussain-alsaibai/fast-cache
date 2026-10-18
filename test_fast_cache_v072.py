"""
fast-cache v0.7.2 — SingleFlightCache tests

Covers:
  * Single thread: hit/miss flow
  * Multiple threads on the same key share one factory call (the
    "cache stampede" protection).
  * Multiple keys proceed in parallel.
  * Factory exception propagates and unlocks waiters.
  * Factory returning None is treated as a real miss next time.
  * Stats counters.
  * clear() wakes up waiters and resets in-flight state.
"""
from __future__ import annotations

import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed

from fast_cache import Cache
from fast_cache_v072 import SingleFlightCache, SingleFlightStats


class TestSingleThread(unittest.TestCase):
    """Single-threaded semantics — must behave like cache.get_or_set."""

    def test_first_call_runs_factory(self):
        cache = Cache()
        sf = SingleFlightCache(cache)
        result = sf.get_or_set("k", lambda: "value")
        self.assertEqual(result, "value")
        self.assertEqual(sf.get("k"), "value")
        self.assertEqual(sf.stats.loads, 1)

    def test_second_call_hits_cache(self):
        cache = Cache()
        sf = SingleFlightCache(cache)
        sf.get_or_set("k", lambda: "value")
        # Second call: factory must not run.
        calls = []
        result = sf.get_or_set("k", lambda: calls.append("ran") or "new")
        self.assertEqual(result, "value")
        self.assertEqual(calls, [])
        self.assertEqual(sf.stats.loads, 1)

    def test_set_then_get_or_set(self):
        cache = Cache()
        sf = SingleFlightCache(cache)
        sf.set("k", "preset")
        # Factory should not be called because the value is cached.
        result = sf.get_or_set("k", lambda: "computed")
        self.assertEqual(result, "preset")
        self.assertEqual(sf.stats.loads, 0)

    def test_passthrough_methods(self):
        cache = Cache()
        sf = SingleFlightCache(cache)
        self.assertIsNone(sf.get("missing"))
        sf.set("k", "v")
        self.assertEqual(sf.get("k"), "v")
        self.assertTrue(sf.delete("k"))
        self.assertIsNone(sf.get("k"))


class TestStampedeProtection(unittest.TestCase):
    """Concurrent callers must share one factory invocation."""

    def test_ten_threads_same_key_one_load(self):
        cache = Cache()
        sf = SingleFlightCache(cache)
        load_count = []
        load_lock = threading.Lock()

        def slow_factory():
            with load_lock:
                load_count.append(1)
            time.sleep(0.05)  # ensure all threads queue before factory returns
            return "result"

        N = 10
        with ThreadPoolExecutor(max_workers=N) as ex:
            futures = [ex.submit(sf.get_or_set, "k", slow_factory) for _ in range(N)]
            results = [f.result() for f in as_completed(futures)]

        # All threads should see the same value.
        self.assertEqual(set(results), {"result"})
        # But only one factory call.
        self.assertEqual(sum(load_count), 1)
        self.assertEqual(sf.stats.loads, 1)
        # All others are waiters (shared).
        self.assertEqual(sf.stats.shared, N - 1)

    def test_different_keys_run_in_parallel(self):
        cache = Cache()
        sf = SingleFlightCache(cache)
        load_count = []
        load_lock = threading.Lock()

        def slow_factory(k):
            with load_lock:
                load_count.append(k)
            time.sleep(0.05)
            return f"value:{k}"

        keys = ["a", "b", "c", "d"]
        with ThreadPoolExecutor(max_workers=len(keys)) as ex:
            futures = {k: ex.submit(sf.get_or_set, k, lambda k=k: slow_factory(k)) for k in keys}
            results = {k: f.result() for k, f in futures.items()}

        # Each key loaded exactly once.
        self.assertEqual(sorted(load_count), keys)
        self.assertEqual({k: results[k] for k in keys}, {k: f"value:{k}" for k in keys})
        self.assertEqual(sf.stats.loads, len(keys))
        self.assertEqual(sf.stats.shared, 0)

    def test_factory_exception_propagates_and_unlocks(self):
        cache = Cache()
        sf = SingleFlightCache(cache)

        def fail():
            raise RuntimeError("boom")

        N = 5
        with ThreadPoolExecutor(max_workers=N) as ex:
            futures = [ex.submit(sf.get_or_set, "k", fail) for _ in range(N)]
            results = []
            for f in as_completed(futures):
                try:
                    f.result()
                    results.append("ok")
                except RuntimeError as e:
                    results.append(str(e))

        # All callers see the same exception.
        self.assertEqual(len(results), N)
        self.assertTrue(all(r == "boom" for r in results))

        # The in-flight slot must be cleared so a retry can re-run the factory.
        # (Otherwise the next caller would block forever on the dead Event.)
        def retry():
            return "recovered"
        result = sf.get_or_set("k", retry)
        self.assertEqual(result, "recovered")


class TestFactoryReturningNone(unittest.TestCase):
    """Factory returning None is treated as a real miss next time."""

    def test_none_factory_result_is_not_cached(self):
        cache = Cache()
        sf = SingleFlightCache(cache)
        result = sf.get_or_set("k", lambda: None)
        self.assertIsNone(result)
        # Next call must run factory again.
        counter = []
        result = sf.get_or_set("k", lambda: counter.append(1) or "real")
        self.assertEqual(result, "real")
        self.assertEqual(counter, [1])


class TestStats(unittest.TestCase):
    """SingleFlightStats counters and serialization."""

    def test_serialized_shape(self):
        stats = SingleFlightStats()
        self.assertEqual(
            stats.serialized(),
            {"loads": 0, "shared": 0, "waiters": 0, "peak_waiters": 0},
        )

    def test_counters_update(self):
        stats = SingleFlightStats()
        stats.record_load()
        stats.record_load()
        stats.record_shared()
        self.assertEqual(stats.loads, 2)
        self.assertEqual(stats.shared, 1)

    def test_peak_waiters(self):
        stats = SingleFlightStats()
        stats.inc_waiters()
        stats.inc_waiters()
        stats.inc_waiters()
        self.assertEqual(stats.waiters, 3)
        self.assertEqual(stats.peak_waiters, 3)
        stats.dec_waiters()
        stats.dec_waiters()
        # peak stays at 3, current drops to 1
        self.assertEqual(stats.waiters, 1)
        self.assertEqual(stats.peak_waiters, 3)

    def test_reset(self):
        stats = SingleFlightStats()
        stats.record_load()
        stats.inc_waiters()
        stats.reset()
        self.assertEqual(stats.serialized(), {"loads": 0, "shared": 0, "waiters": 0, "peak_waiters": 0})

    def test_repr(self):
        stats = SingleFlightStats()
        stats.record_load()
        s = repr(stats)
        self.assertIn("loads=1", s)
        self.assertIn("shared=0", s)


class TestClearAndEviction(unittest.TestCase):
    """clear() must wake any in-flight waiters."""

    def test_clear_during_load_releases_waiters(self):
        cache = Cache()
        sf = SingleFlightCache(cache)
        started = threading.Event()

        def slow():
            started.set()
            time.sleep(0.2)
            return "slow"

        # Start a loader thread.
        t = threading.Thread(target=lambda: sf.get_or_set("k", slow))
        t.start()
        started.wait()

        # Another thread waiting on the same key.
        waiter_result = []
        def waiter():
            waiter_result.append(sf.get_or_set("k", lambda: "should_not_run"))

        w = threading.Thread(target=waiter)
        w.start()
        time.sleep(0.02)  # let waiter park

        # Clearing should wake the waiter.
        sf.clear()
        t.join(timeout=2.0)
        w.join(timeout=2.0)

        # The waiter must NOT be hanging.
        self.assertFalse(w.is_alive())


class TestExpiry(unittest.TestCase):
    """After TTL expiry, the next miss must run factory again."""

    def test_expired_value_reruns_factory(self):
        cache = Cache()
        sf = SingleFlightCache(cache)
        counter = []
        def factory():
            counter.append(1)
            return f"v{len(counter)}"
        v1 = sf.get_or_set("k", factory, ttl=0.05)
        self.assertEqual(v1, "v1")
        time.sleep(0.1)
        v2 = sf.get_or_set("k", factory)
        self.assertEqual(v2, "v2")
        self.assertEqual(sf.stats.loads, 2)


if __name__ == "__main__":
    unittest.main()
