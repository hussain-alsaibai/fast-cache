"""Tests for eviction policies (LRU, LFU, FIFO, TTL_AWARE)."""

from __future__ import annotations

import asyncio
import sys
import time
from collections import deque
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fast_cache as fc


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_cache(policy, max_size=3, ttl=None):
    return fc.Cache(max_size=max_size, default_ttl=ttl, policy=policy)


# ---------------------------------------------------------------------------
# LRU
# ---------------------------------------------------------------------------


class TestLRUPolicy:
    def test_basic_lru(self):
        c = make_cache(fc.CachePolicy.LRU, max_size=2)
        c.set("a", 1)
        c.set("b", 2)
        c.get("a")  # a → MRU; b → LRU
        c.set("c", 3)  # evicts b
        assert c.get("a") == 1
        assert c.get("b") is None
        assert c.get("c") == 3
        assert c.stats()["evictions"] == 1

    def test_lru_overwrite_does_not_reset_position(self):
        """Re-setting an existing key still moves it to MRU position."""
        c = make_cache(fc.CachePolicy.LRU, max_size=2)
        c.set("a", 1)
        c.set("b", 2)
        c.set("a", 11)  # a is now MRU
        c.set("c", 3)  # evicts b
        assert c.get("b") is None
        assert c.get("a") == 11
        assert c.get("c") == 3


# ---------------------------------------------------------------------------
# LFU
# ---------------------------------------------------------------------------


class TestLFUPolicy:
    def test_evicts_least_frequent(self):
        c = make_cache(fc.CachePolicy.LFU, max_size=2)
        c.set("a", 1)
        c.set("b", 2)
        # bump a's hit count without touching b
        c.get("a")
        c.get("a")
        c.get("a")
        # a=3, b=0. Inserting "d" forces an eviction; both b and d have 0 hits,
        # so the older one (b) is the victim.
        c.set("d", 4)
        assert c.get("a") == 1
        assert c.get("b") is None
        assert c.get("d") == 4

    def test_lfu_tie_breaks_to_oldest(self):
        """When all entries have equal hit counts, oldest is evicted."""
        c = make_cache(fc.CachePolicy.LFU, max_size=2)
        c.set("a", 1)
        time.sleep(0.005)
        c.set("b", 2)
        c.set("c", 3)  # evicts a (oldest at hit-count = 0)
        assert c.get("a") is None
        assert c.get("b") == 2
        assert c.get("c") == 3

    def test_lfu_zero_hits_eviction(self):
        c = make_cache(fc.CachePolicy.LFU, max_size=3)
        c.set("a", 1)
        c.set("b", 2)
        c.set("c", 3)
        c.get("a")  # now a=1, b=0, c=0
        c.get("a")  # now a=2, b=0, c=0
        c.get("c")  # now a=2, b=0, c=1
        c.set("d", 4)  # evicts b (0 hits; ties to oldest)
        assert c.get("b") is None


# ---------------------------------------------------------------------------
# FIFO
# ---------------------------------------------------------------------------


class TestFIFOPolicy:
    def test_evicts_oldest(self):
        c = make_cache(fc.CachePolicy.FIFO, max_size=2)
        time.sleep(0.005)
        c.set("a", 1)
        time.sleep(0.005)
        c.set("b", 2)
        # Touching or re-setting does not change created_at
        c.get("a")
        c.set("c", 3)  # evicts a (oldest)
        assert c.get("a") is None
        assert c.get("b") == 2
        assert c.get("c") == 3

    def test_fifo_independent_of_access(self):
        c = make_cache(fc.CachePolicy.FIFO, max_size=2)
        c.set("a", 1)
        c.set("b", 2)
        c.get("a")
        c.get("a")
        c.get("a")
        # FIFO should still evict a because the user has not re-set it.
        c.set("c", 3)
        assert c.get("a") is None
        assert c.get("b") == 2


# ---------------------------------------------------------------------------
# TTL_AWARE
# ---------------------------------------------------------------------------


class TestTTLAwarePolicy:
    def test_prefers_most_recently_expired(self):
        c = make_cache(fc.CachePolicy.TTL_AWARE, max_size=2)
        # a expires first
        c.set("a", 1, ttl=0.05)
        c.set("b", 2, ttl=10.0)
        # wait for a to expire
        time.sleep(0.08)
        c.set("c", 3, ttl=10.0)
        # 'a' should have been evicted (most recently expired < now)
        assert c.get("a") is None
        assert c.get("b") == 2
        assert c.get("c") == 3

    def test_falls_back_to_lru_when_none_expired(self):
        c = make_cache(fc.CachePolicy.TTL_AWARE, max_size=2)
        c.set("a", 1, ttl=10.0)
        c.set("b", 2, ttl=10.0)
        c.get("a")  # a → MRU
        c.set("c", 3, ttl=10.0)
        # Nothing has expired; falls back to LRU. b is evicted.
        assert c.get("a") == 1
        assert c.get("b") is None
        assert c.get("c") == 3


# ---------------------------------------------------------------------------
# Eviction triggers
# ---------------------------------------------------------------------------


class TestEvictionTriggers:
    def test_set_overflow_evicts(self):
        c = make_cache(fc.CachePolicy.LRU, max_size=2)
        c.set("a", 1)
        c.set("b", 2)
        c.set("c", 3)
        assert len(c) == 2

    def test_eviction_counted_in_stats(self):
        c = make_cache(fc.CachePolicy.LRU, max_size=1)
        c.set("a", 1)
        c.set("b", 2)
        c.set("c", 3)
        assert c.stats()["evictions"] == 2

    def test_unbounded_cache_does_not_evict(self):
        c = fc.Cache(max_size=None, policy=fc.CachePolicy.LRU)
        for i in range(100):
            c.set(f"k{i}", i)
        assert len(c) == 100
        assert c.stats()["evictions"] == 0

    def test_evicted_event_fires_for_each_victim(self):
        c = fc.Cache(max_size=1, policy=fc.CachePolicy.LRU)
        seen = deque(maxlen=10)
        c.on(fc.CacheEvent.EVICTED, lambda **kw: seen.append(kw["key"]))
        c.set("a", 1)
        c.set("b", 2)
        c.set("c", 3)
        assert list(seen) == ["a", "b"]


# ---------------------------------------------------------------------------
# Static / class-level attributes
# ---------------------------------------------------------------------------


class TestCachePolicyClass:
    def test_constants(self):
        assert fc.CachePolicy.LRU == "lru"
        assert fc.CachePolicy.LFU == "lfu"
        assert fc.CachePolicy.FIFO == "fifo"
        assert fc.CachePolicy.TTL_AWARE == "ttl_aware"

    def test_cache_event_constants(self):
        assert fc.CacheEvent.HIT == "hit"
        assert fc.CacheEvent.MISS == "miss"
        assert fc.CacheEvent.EXPIRED == "expired"
        assert fc.CacheEvent.EVICTED == "evicted"
        assert fc.CacheEvent.SET == "set"
        assert fc.CacheEvent.CLEARED == "cleared"

    def test_unknown_policy_safe_fallback(self):
        """An unrecognised policy must not crash eviction."""
        c = fc.Cache(max_size=1, policy="weird-new-policy")
        c.set("a", 1)
        c.set("b", 2)
        # Falls back to LRU-style "next(iter)" which evicts the oldest.
        assert c.get("b") == 2


# ---------------------------------------------------------------------------
# Decorator integration with policies
# ---------------------------------------------------------------------------


class TestDecoratorPolicies:
    def test_decorator_accepts_policy(self):
        calls = {"n": 0}

        @fc.cache(max_size=2, ttl=60, policy=fc.CachePolicy.LFU)
        def slow(x):
            calls["n"] += 1
            return x

        assert slow(1) == 1
        assert slow(1) == 1
        assert slow(2) == 2
        assert slow.cache_info()["policy"] == "lfu"

    def test_async_decorator_accepts_policy(self):

        @fc.acache(max_size=2, ttl=60, policy=fc.CachePolicy.FIFO)
        async def slow(x):
            return x

        async def runner():
            v = await slow(42)
            return v

        assert asyncio.run(runner()) == 42
        # Call again to make sure stats get recorded.
        assert asyncio.run(runner()) == 42


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
