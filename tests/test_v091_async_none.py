"""v0.9.1 regression: the async get_or_set still treated a cached ``None`` as a miss.

0.9.0 fixed the sentinel in ``Cache.get_or_set`` and ``SingleFlightCache.get_or_set``,
and the changelog says so in those words. ``Cache.get_or_set_async`` was never
swept: it kept ``value = self.get(key); if value is not None``, so a legitimate
cached "no value" answer re-ran the async factory on every call.

That is the expensive half. A cached ``None`` in an agent loop is usually a
provider 404, a rate-limit with no payload, or an empty retrieval -- exactly the
answers you cache so you do not pay for them again. The sync path stopped
re-running and the async path kept re-running, which reads as working code to
anyone who tested only the sync call.

Three behaviours are pinned here, and the first two fail against 0.9.0:

1. a cached ``None`` is a hit for ``get_or_set_async``;
2. ``has()`` still distinguishes "absent" from "cached as None";
3. ``get_or_set_async`` returns an awaitable even on a hit -- its documented
   contract, so callers can uniformly ``await`` it. A fix that returns the bare
   value on a hit would break every ``await cache.get_or_set_async(...)``.
"""

import asyncio

import pytest

from fast_cache import Cache


class Counter:
    """A factory that records how many times it actually ran."""

    def __init__(self, value):
        self.value = value
        self.runs = 0

    async def __call__(self):
        self.runs += 1
        return self.value


def test_cached_none_is_a_hit_async():
    async def main():
        c = Cache()
        first = Counter(None)
        assert await c.get_or_set_async("k", first) is None
        assert first.runs == 1

        second = Counter("should-not-be-called")
        assert await c.get_or_set_async("k", second) is None
        assert second.runs == 0, "a cached None re-ran the async factory"
        assert c.has("k")

    asyncio.run(main())


def test_has_distinguishes_absent_from_cached_none_async():
    async def main():
        c = Cache()
        assert not c.has("missing")
        await c.get_or_set_async("missing", Counter(None))
        assert c.has("missing")          # present...
        assert await c.get_or_set_async("missing", Counter(None)) is None
        assert c.get("missing") is None  # ...and its value is None

    asyncio.run(main())


@pytest.mark.parametrize("prefill", [True, False])
def test_get_or_set_async_always_returns_an_awaitable(prefill):
    """Contract: callers may ``await`` the result unconditionally."""

    async def main():
        c = Cache()
        if prefill:
            c.set("k", None)
        awaitable = c.get_or_set_async("k", Counter("v"))
        assert asyncio.iscoroutine(awaitable), type(awaitable)
        assert await awaitable == ("v" if not prefill else None)

    asyncio.run(main())


def test_falsy_values_are_cached_async():
    """The same convention 0.9.0 gave the sync path: falsy values are values."""

    async def main():
        for value in (0, "", False, [], {}):
            c = Cache()
            assert await c.get_or_set_async("k", Counter(value)) == value
            probe = Counter("sentinel")
            assert await c.get_or_set_async("k", probe) == value
            assert probe.runs == 0, f"{value!r} was treated as a miss"

    asyncio.run(main())


def test_sync_and_async_agree():
    """The two paths must not disagree about what a hit is."""

    async def main():
        sync_cache, async_cache = Cache(), Cache()
        sync_cache.set("k", None)
        async_cache.set("k", None)
        sync_runs, async_runs = [], []

        sync_cache.get_or_set("k", lambda: sync_runs.append(1) or "x")
        await async_cache.get_or_set_async("k", Counter("x").__call__)
        assert len(sync_runs) == 0

        probe = Counter("x")
        await async_cache.get_or_set_async("k", probe)
        assert probe.runs == 0
        assert len(sync_runs) == len([]) == 0

    asyncio.run(main())
