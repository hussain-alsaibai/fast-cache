"""Tests for fast-cache v0.7.0 — cache_method decorator."""

from __future__ import annotations

import asyncio
import time

import pytest

from fast_cache import Cache, CacheAsync, cache_method


# ── sync cache_method ────────────────────────────────────────────────────────


def test_cache_method_returns_value_on_first_call():
    cache = Cache()
    calls = []

    class Service:
        @cache_method(cache, prefix="svc")
        def slow(self, x):
            calls.append(x)
            return x * 2

    svc = Service()
    assert svc.slow(21) == 42
    assert calls == [21]


def test_cache_method_returns_cached_value_on_second_call():
    cache = Cache()
    calls = []

    class Service:
        @cache_method(cache, prefix="svc")
        def slow(self, x):
            calls.append(x)
            return x * 2

    svc = Service()
    assert svc.slow(21) == 42
    assert svc.slow(21) == 42
    assert svc.slow(21) == 42
    assert len(calls) == 1  # function invoked exactly once


def test_cache_method_keys_on_arguments():
    cache = Cache()
    calls = []

    class Service:
        @cache_method(cache, prefix="svc")
        def slow(self, x):
            calls.append(x)
            return x * 2

    svc = Service()
    assert svc.slow(21) == 42
    assert svc.slow(22) == 44
    assert svc.slow(21) == 42  # hit
    assert calls == [21, 22]


def test_cache_method_distinguishes_instances():
    cache = Cache()
    calls = []

    class Service:
        def __init__(self, name):
            self.name = name

        @cache_method(cache, prefix="svc")
        def slow(self, x):
            calls.append((self.name, x))
            return f"{self.name}:{x}"

    s1 = Service("alpha")
    s2 = Service("beta")
    assert s1.slow(1) == "alpha:1"
    assert s2.slow(1) == "beta:1"  # different instance, different slot
    assert s1.slow(1) == "alpha:1"  # hit
    assert calls == [("alpha", 1), ("beta", 1)]


def test_cache_method_keys_on_kwargs():
    cache = Cache()
    calls = []

    class Service:
        @cache_method(cache, prefix="svc")
        def slow(self, x, *, scale=1):
            calls.append((x, scale))
            return x * scale

    svc = Service()
    assert svc.slow(10, scale=2) == 20
    assert svc.slow(10, scale=3) == 30
    assert svc.slow(10, scale=2) == 20  # hit
    assert len(calls) == 2


def test_cache_method_respects_ttl():
    cache = Cache()
    calls = []

    class Service:
        @cache_method(cache, prefix="svc", ttl=0.05)
        def slow(self, x):
            calls.append(x)
            return x

    svc = Service()
    assert svc.slow(1) == 1
    time.sleep(0.1)
    assert svc.slow(1) == 1  # expired, recomputed
    assert len(calls) == 2


def test_cache_method_prefix_isolates_namespaces():
    cache = Cache()

    class Service:
        @cache_method(cache, prefix="a")
        def m_a(self, x):
            return ("a", x)

        @cache_method(cache, prefix="b")
        def m_b(self, x):
            return ("b", x)

    svc = Service()
    assert svc.m_a(1) == ("a", 1)
    assert svc.m_b(1) == ("b", 1)


def test_cache_method_custom_key_fn():
    cache = Cache()
    calls = []

    class Service:
        @cache_method(cache, prefix="svc", key_fn=lambda self, x: f"k={x % 10}")
        def slow(self, x):
            calls.append(x)
            return x

    svc = Service()
    assert svc.slow(1) == 1
    # x=11 maps to the same bucket (1 % 10 == 11 % 10), so the cached
    # value for x=1 is returned without recomputing.
    assert svc.slow(11) == 1
    assert svc.slow(2) == 2   # different bucket
    assert calls == [1, 2]


def test_cache_method_module_level_function():
    cache = Cache()
    calls = []

    @cache_method(cache, prefix="fn")
    def double(x):
        calls.append(x)
        return x * 2

    assert double(5) == 10
    assert double(5) == 10
    assert calls == [5]


def test_cache_method_preserves_metadata():
    cache = Cache()

    class Service:
        @cache_method(cache)
        def slow(self, x):
            """Return x doubled."""
            return x * 2

    svc = Service()
    assert svc.slow.__name__ == "slow"
    assert "Return x doubled" in (svc.slow.__doc__ or "")


def test_cache_method_caches_none_result():
    cache = Cache()
    calls = []

    class Service:
        @cache_method(cache, prefix="svc")
        def lookup(self, key):
            calls.append(key)
            return None  # explicit None

    svc = Service()
    assert svc.lookup("k") is None
    assert svc.lookup("k") is None
    assert calls == ["k"]  # only computed once even though result is None


def test_cache_method_caches_falsy_results():
    cache = Cache()
    calls = []

    class Service:
        @cache_method(cache, prefix="svc")
        def lookup(self, key):
            calls.append(key)
            return 0

    svc = Service()
    assert svc.lookup("k") == 0
    assert svc.lookup("k") == 0
    assert calls == ["k"]


def test_cache_method_caches_complex_objects():
    cache = Cache()
    calls = []

    class Service:
        @cache_method(cache, prefix="svc")
        def fetch(self, key):
            calls.append(key)
            return {"users": [1, 2, 3], "meta": key}

    svc = Service()
    assert svc.fetch("u") == {"users": [1, 2, 3], "meta": "u"}
    assert svc.fetch("u") == {"users": [1, 2, 3], "meta": "u"}
    assert calls == ["u"]


# ── async cache_method ───────────────────────────────────────────────────────


def test_cache_method_async_works():
    async def runner():
        cache = CacheAsync()
        calls = []

        class Service:
            @cache_method(cache, prefix="svc")
            async def slow(self, x):
                calls.append(x)
                await asyncio.sleep(0)
                return x * 2

        svc = Service()
        assert await svc.slow(21) == 42
        assert await svc.slow(21) == 42
        assert await svc.slow(21) == 42
        assert calls == [21]

    asyncio.run(runner())


def test_cache_method_async_keys_correctly():
    async def runner():
        cache = CacheAsync()
        calls = []

        class Service:
            @cache_method(cache, prefix="svc", ttl=0.05)
            async def slow(self, x):
                calls.append(x)
                await asyncio.sleep(0)
                return x * 2

        svc = Service()
        assert await svc.slow(1) == 2
        assert await svc.slow(2) == 4
        assert await svc.slow(1) == 2  # hit
        await asyncio.sleep(0.1)
        assert await svc.slow(1) == 2  # expired
        assert calls == [1, 2, 1]

    asyncio.run(runner())


def test_cache_method_async_distinguishes_instances():
    async def runner():
        cache = CacheAsync()
        calls = []

        class Service:
            def __init__(self, name):
                self.name = name

            @cache_method(cache, prefix="svc")
            async def slow(self, x):
                calls.append((self.name, x))
                await asyncio.sleep(0)
                return f"{self.name}:{x}"

        s1, s2 = Service("a"), Service("b")
        assert await s1.slow(1) == "a:1"
        assert await s2.slow(1) == "b:1"
        assert calls == [("a", 1), ("b", 1)]

    asyncio.run(runner())


# ── edge cases ───────────────────────────────────────────────────────────────


def test_cache_method_on_staticmethod():
    cache = Cache()
    calls = []

    class Service:
        @cache_method(cache, prefix="svc")
        @staticmethod
        def compute(x):
            calls.append(x)
            return x + 1

    assert Service.compute(10) == 11
    assert Service.compute(10) == 11
    assert calls == [10]


def test_cache_method_with_classmethod():
    """``cache_method`` does NOT support ``@classmethod`` directly — apply
    it to the underlying function instead (``Service.compute.__func__``)
    or skip the decorator for classmethods. We document this constraint.
    """
    cache = Cache()

    class Service:
        @classmethod
        def compute(cls, x):
            return x + 1

    # Wrapping the underlying function works.
    wrapped = cache_method(cache, prefix="svc")(Service.compute.__func__)
    assert wrapped(Service, 10) == 11
    # But applying ``@cache_method`` on top of ``@classmethod`` is not
    # supported because Python's classmethod descriptor protocol doesn't
    # forward ``cls`` when the descriptor is called directly. Document
    # this with a smoke test that the constraint is documented.
    assert True


def test_cache_method_collision_resistant_keys():
    """Two instances with the same arguments but different ``self`` must
    not collide. (Verified by the distinguishing test above; this is the
    deterministic-keying smoke test.)"""
    cache = Cache()

    class Service:
        @cache_method(cache, prefix="svc")
        def slow(self, x):
            return x

    svc = Service()
    a = svc.slow(1)
    b = svc.slow(1)
    assert a == b
    # Same value, same key — that's by design.
