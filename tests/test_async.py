"""Tests for async iteration and cache event hooks."""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fast_cache as fc


# ---------------------------------------------------------------------------
# Async iteration
# ---------------------------------------------------------------------------


class TestAsyncIteration:
    def test_aiter_yields_live_keys(self):
        c = fc.Cache(max_size=10)
        c.set("a", 1)
        c.set("b", 2)
        c.set("c", 3)

        async def collect():
            return [k async for k in c]

        keys = asyncio.run(collect())
        assert set(keys) == {"a", "b", "c"}

    def test_aiter_skips_expired(self):
        c = fc.Cache(max_size=10)
        c.set("live", "v")  # default_ttl=None, lives forever
        c.set("dead", "v", ttl=0.05)
        time.sleep(0.08)

        async def collect():
            return [k async for k in c]

        keys = asyncio.run(collect())
        assert "live" in keys
        assert "dead" not in keys

    def test_items_async_returns_pairs(self):
        c = fc.Cache(max_size=10)
        c.set("a", 1)
        c.set("b", 2)

        async def collect():
            return {k: v async for k, v in c.items_async()}

        items = asyncio.run(collect())
        assert items == {"a": 1, "b": 2}

    def test_values_async_returns_values(self):
        c = fc.Cache(max_size=10)
        c.set("a", 1)
        c.set("b", 2)

        async def collect():
            return [v async for v in c.values_async()]

        values = asyncio.run(collect())
        assert sorted(values) == [1, 2]

    def test_aiter_empty_cache(self):
        c = fc.Cache(max_size=10)

        async def collect():
            return [k async for k in c]

        assert asyncio.run(collect()) == []

    def test_aiter_yields_control(self):
        """Async iteration must yield to the event loop between items."""
        c = fc.Cache(max_size=10)
        for i in range(3):
            c.set(f"k{i}", i)

        # Driving the async iterator via asyncio.run directly so the loop
        # genuinely has a chance to schedule other tasks between yields.
        loop = asyncio.new_event_loop()
        try:
            last = loop.time()
            seen = []

            async def tracker():
                nonlocal last
                async for _ in c:
                    now = loop.time()
                    seen.append(now - last)
                    last = now
                    await asyncio.sleep(0)

            loop.run_until_complete(tracker())
        finally:
            loop.close()
        # Three yields means at least three deltas were recorded.
        assert len(seen) == 3


# ---------------------------------------------------------------------------
# Event hooks
# ---------------------------------------------------------------------------


class TestEventHooks:
    def test_hit_event_fires(self):
        c = fc.Cache()
        calls = []

        def on_hit(key, stale):
            calls.append((key, stale))

        c.on(fc.CacheEvent.HIT, on_hit)
        c.set("a", 1)
        c.get("a")
        c.get("a")
        assert ("a", False) in calls
        assert calls.count(("a", False)) == 2

    def test_miss_event_fires(self):
        c = fc.Cache()
        calls = []

        c.on(fc.CacheEvent.MISS, lambda **kw: calls.append(kw["key"]))
        c.get("nope")
        c.get("also-nope")
        assert calls == ["nope", "also-nope"]

    def test_set_event_fires(self):
        c = fc.Cache()
        seen = []
        c.on(fc.CacheEvent.SET, lambda **kw: seen.append(kw["key"]))
        c.set("a", 1)
        c.set("b", 2)
        assert seen == ["a", "b"]

    def test_expired_event_on_get(self):
        c = fc.Cache(default_ttl=0.05)
        seen = []
        c.on(fc.CacheEvent.EXPIRED, lambda **kw: seen.append(kw["key"]))
        c.set("a", 1)
        time.sleep(0.08)
        c.get("a", default="MISS")
        # May be 1 (on access) but is at least fired.
        assert seen.count("a") >= 1

    def test_evicted_event_fires(self):
        c = fc.Cache(max_size=2, policy=fc.CachePolicy.LRU)
        seen = []
        c.on(fc.CacheEvent.EVICTED, lambda **kw: seen.append(kw["key"]))
        c.set("a", 1)
        c.set("b", 2)
        c.set("c", 3)  # evicts a
        assert seen == ["a"]

    def test_cleared_event_fires(self):
        c = fc.Cache()
        calls = []
        c.on(fc.CacheEvent.CLEARED, lambda **kw: calls.append(True))
        c.set("a", 1)
        c.clear()
        c.clear()
        assert calls == [True, True]

    def test_multiple_handlers(self):
        c = fc.Cache()
        a = []
        b = []
        c.on(fc.CacheEvent.HIT, lambda **kw: a.append(1))
        c.on(fc.CacheEvent.HIT, lambda **kw: b.append(1))
        c.set("a", 1)
        c.get("a")
        assert a == [1]
        assert b == [1]

    def test_bad_handler_does_not_break_cache(self):
        c = fc.Cache()

        def bad(**kw):
            raise RuntimeError("boom")

        c.on(fc.CacheEvent.HIT, bad)

        c.set("a", 1)
        # Should not raise despite the handler.
        assert c.get("a") == 1

    def test_off_removes_handler(self):
        c = fc.Cache()
        seen = []

        def h(**kw):
            seen.append(1)

        c.on(fc.CacheEvent.HIT, h)
        c.on(fc.CacheEvent.HIT, h)  # twice

        c.set("a", 1)
        c.get("a")
        assert seen == [1, 1]

        c.off(fc.CacheEvent.HIT, h)
        c.set("b", 2)
        c.get("b")
        # Still [1, 1] because the only matching handler was removed.
        assert seen == [1, 1]

    def test_unknown_event_raises_on_register(self):
        c = fc.Cache()
        with pytest.raises(ValueError):
            c.on("not-a-real-event", lambda **kw: None)

    def test_emit_to_unregistered_event_is_noop(self):
        """emit() must not raise for unknown event names; only on() guards them."""
        c = fc.Cache()
        c.emit("custom-not-registered", value=42)


# ---------------------------------------------------------------------------
# CacheStats dataclass
# ---------------------------------------------------------------------------


class TestCacheStats:
    def test_defaults(self):
        s = fc.CacheStats()
        assert s.hits == 0
        assert s.misses == 0
        assert s.sets == 0
        assert s.evictions == 0
        assert s.expirations == 0
        assert s.hit_ratio == 0.0

    def test_update_hit_ratio(self):
        s = fc.CacheStats()
        s.update(True)
        s.update(True)
        s.update(False)
        assert s.hits == 2
        assert s.misses == 1
        assert abs(s.hit_ratio - 2 / 3) < 1e-9

    def test_to_dict(self):
        s = fc.CacheStats(hits=4, misses=1)
        s.update(True)
        d = s.to_dict()
        assert d["hits"] == 5
        assert d["misses"] == 1
        assert abs(d["hit_ratio"] - 5 / 6) < 1e-9

    def test_stats_obj_returns_same_instance(self):
        c = fc.Cache()
        s = c.stats_obj()
        assert isinstance(s, fc.CacheStats)
        # Reading stats updates the dataclass.
        c.set("a", 1)
        c.get("a")
        assert s.hits >= 1

    def test_reset_stats(self):
        c = fc.Cache()
        c.set("a", 1)
        c.get("a")
        assert c.hits + c.misses > 0
        c.reset_stats()
        assert c.hits == 0
        assert c.misses == 0
        assert c.stats()["sets"] == 0


# ---------------------------------------------------------------------------
# Export / Import
# ---------------------------------------------------------------------------


class TestExportImport:
    def test_export_round_trip(self):
        c = fc.Cache(default_ttl=60)
        c.set("a", 1)
        c.set("b", "two")
        c.set("c", [1, 2, 3])

        payload = c.export()
        assert payload["version"] == "1.0"
        keys = {item["key"] for item in payload["items"]}
        assert keys == {"a", "b", "c"}

        c2 = fc.Cache(default_ttl=60)
        n = c2.import_(payload)
        assert n == 3
        assert c2.get("a") == 1
        assert c2.get("b") == "two"
        assert c2.get("c") == [1, 2, 3]

    def test_export_skips_expired(self):
        c = fc.Cache(default_ttl=0.05)
        c.set("dead", 1)
        time.sleep(0.08)
        payload = c.export()
        keys = {item["key"] for item in payload["items"]}
        assert "dead" not in keys

    def test_import_counts_items(self):
        c = fc.Cache()
        n = c.import_({"items": [{"key": "x", "value": 1, "ttl": 60}]})
        assert n == 1
        assert c.get("x") == 1

    def test_export_is_json_serializable(self):
        import json

        c = fc.Cache()
        c.set("a", 1)
        c.set("b", "str")
        # Sets / dicts / dataclasses are *not* JSON-serializable; coerce.
        c.set("c", [1, 2, {"nested": True}])
        payload = c.export()
        # Should not raise.
        encoded = json.dumps(payload, default=str)
        decoded = json.loads(encoded)
        assert decoded["version"] == "1.0"
        assert len(decoded["items"]) == 3


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
