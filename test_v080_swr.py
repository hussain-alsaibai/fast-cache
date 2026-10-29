"""Tests for the v0.8.0 SWR / namespacing sidecar.

Run: python -m pytest test_v080_swr.py -v
"""
from __future__ import annotations

import threading
import time

import pytest

from fast_cache import Cache
from _v080 import NamespaceView, SwrCache, SwrStats, namespaced


@pytest.fixture
def inner():
    return Cache()


@pytest.fixture
def swr(inner):
    c = SwrCache(inner, swr_window=10.0)
    yield c
    c.close()


# --------------------------------------------------------------------------
# freshness lifecycle
# --------------------------------------------------------------------------

def test_first_read_is_a_miss_and_calls_factory(swr):
    calls = []

    def factory():
        calls.append(1)
        return "v1"

    assert swr.get_or_refresh("k", factory, ttl=0.2) == "v1"
    assert len(calls) == 1
    assert swr.stats.expired_miss == 1


def test_fresh_value_skips_factory(swr):
    swr.get_or_refresh("k", lambda: "v1", ttl=5)
    swr.stats.reset()
    assert swr.get_or_refresh("k", lambda: "boom", ttl=5) == "v1"
    assert swr.stats.fresh_hit == 1
    assert swr.stats.refreshes == 0


def test_stale_value_is_served_and_refreshes_in_background(swr):
    swr.get_or_refresh("k", lambda: "v1", ttl=0.05)
    time.sleep(0.08)

    assert swr.get_or_refresh("k", lambda: "v2", ttl=5) == "v1"  # stale served
    assert swr.wait_for_refreshes(timeout=5)
    assert swr.peek("k") == "v2"
    assert swr.stats.stale_hit == 1
    assert swr.stats.refreshes == 1


def test_value_past_window_blocks_on_factory(swr):
    swr.get_or_refresh("k", lambda: "v1", ttl=0.05)
    time.sleep(0.08)
    # window is 10s, so still stale-servable; use a tiny-window cache to test expiry
    tight = SwrCache(Cache(), swr_window=0.0)
    try:
        tight.get_or_refresh("k", lambda: "v1", ttl=0.05)
        time.sleep(0.08)
        assert tight.get_or_refresh("k", lambda: "v2", ttl=5) == "v2"
        assert tight.stats.expired_miss == 2
    finally:
        tight.close()


def test_is_fresh_and_is_stale(swr):
    swr.get_or_refresh("k", lambda: "v1", ttl=5)
    assert swr.is_fresh("k") is True
    assert swr.is_stale("k") is False
    assert swr.is_fresh("missing") is False


def test_zero_window_disables_swr(inner):
    c = SwrCache(inner, swr_window=0.0)
    try:
        c.get_or_refresh("k", lambda: "v1", ttl=0.05)
        time.sleep(0.08)
        c.get_or_refresh("k", lambda: "v2", ttl=5)
        assert c.stats.stale_hit == 0
    finally:
        c.close()


def test_negative_window_rejected(inner):
    with pytest.raises(ValueError):
        SwrCache(inner, swr_window=-1)


# --------------------------------------------------------------------------
# coalescing
# --------------------------------------------------------------------------

def test_concurrent_stale_reads_coalesce_into_one_refresh(inner):
    gate = threading.Event()
    started = threading.Event()
    calls = []

    def slow_factory():
        calls.append(1)
        started.set()
        gate.wait(2)
        return "v2"

    c = SwrCache(inner, swr_window=10.0)
    try:
        c.get_or_refresh("k", lambda: "v1", ttl=0.05)
        time.sleep(0.08)

        results = []

        def reader():
            results.append(c.get_or_refresh("k", slow_factory, ttl=5))

        threads = [threading.Thread(target=reader) for _ in range(6)]
        for t in threads:
            t.start()
        started.wait(2)
        time.sleep(0.1)
        gate.set()
        for t in threads:
            t.join(5)

        assert results == ["v1"] * 6          # everyone got the stale value
        assert len(calls) == 1                  # factory ran exactly once
        assert c.stats.coalesced >= 4           # extra readers suppressed
        c.wait_for_refreshes(timeout=5)
        assert c.peek("k") == "v2"
    finally:
        c.close()


# --------------------------------------------------------------------------
# failure handling
# --------------------------------------------------------------------------

def test_failed_refresh_keeps_previous_value(inner):
    def boom():
        raise RuntimeError("upstream down")

    c = SwrCache(inner, swr_window=10.0, refresh_error_handler=lambda k, e: None)
    try:
        c.get_or_refresh("k", lambda: "v1", ttl=0.05)
        time.sleep(0.08)
        assert c.get_or_refresh("k", boom, ttl=5) == "v1"   # stale degraded, not raised
        c.wait_for_refreshes(timeout=5)
        assert c.peek("k") == "v1"                          # value survives failure
        assert c.stats.refresh_errors == 1
    finally:
        c.close()


def test_blocking_refresh_failure_raises_on_first_load(inner):
    c = SwrCache(inner, swr_window=10.0)
    try:
        with pytest.raises(ValueError):
            c.get_or_refresh("k", lambda: (_ for _ in ()).throw(ValueError("no cache")), ttl=5)
    finally:
        c.close()


def test_error_handler_is_invoked(inner):
    seen = []
    c = SwrCache(inner, swr_window=10.0, refresh_error_handler=lambda k, e: seen.append((k, type(e).__name__)))
    try:
        c.get_or_refresh("k", lambda: "v1", ttl=0.05)
        time.sleep(0.08)
        c.get_or_refresh("k", lambda: (_ for _ in ()).throw(RuntimeError("x")), ttl=5)
        c.wait_for_refreshes(timeout=5)
        assert seen == [("k", "RuntimeError")]
    finally:
        c.close()


# --------------------------------------------------------------------------
# invalidation / lifecycle
# --------------------------------------------------------------------------

def test_invalidate_drops_freshness(swr):
    swr.get_or_refresh("k", lambda: "v1", ttl=5)
    assert swr.is_fresh("k")
    assert swr.invalidate("k") is True
    assert swr.is_fresh("k") is False
    assert swr.get_or_refresh("k", lambda: "v2", ttl=5) == "v2"


def test_context_manager_closes_executor(inner):
    with SwrCache(inner, swr_window=1.0) as c:
        c.get_or_refresh("k", lambda: "v1", ttl=1)
    assert c.pending_refreshes() == 0


# --------------------------------------------------------------------------
# namespacing
# --------------------------------------------------------------------------

def test_namespace_isolates_keys(inner):
    users = namespaced(inner, "users")
    orders = namespaced(inner, "orders")
    users.set("42", {"name": "ali"})
    orders.set("42", {"total": 10})

    assert users.get("42") == {"name": "ali"}
    assert orders.get("42") == {"total": 10}
    assert "users:42" in list(inner.keys())
    assert len(users) == 1


def test_namespace_clear_only_drops_own_keys(inner):
    users = namespaced(inner, "users")
    orders = namespaced(inner, "orders")
    users.set("a", 1); users.set("b", 2)
    orders.set("a", 3)

    assert users.clear() == 2
    assert len(users) == 0
    assert orders.get("a") == 3


def test_namespace_add_is_insert_if_absent(inner):
    ns = namespaced(inner, "n")
    assert ns.add("k", 1) is True
    assert ns.add("k", 2) is False
    assert ns.get("k") == 1


def test_namespace_incr(inner):
    ns = namespaced(inner, "c")
    assert ns.incr("hits") == 1
    assert ns.incr("hits", 4) == 5


def test_namespace_keys_are_stripped(inner):
    ns = namespaced(inner, "x")
    ns.set("one", 1); ns.set("two", 2)
    assert sorted(ns.keys()) == ["one", "two"]
    assert dict(ns.items()) == {"one": 1, "two": 2}


def test_empty_prefix_rejected(inner):
    with pytest.raises(ValueError):
        namespaced(inner, "")


# --------------------------------------------------------------------------
# stats
# --------------------------------------------------------------------------

def test_stats_snapshot_and_reset(swr):
    swr.get_or_refresh("k", lambda: "v1", ttl=5)
    snap = swr.stats.snapshot()
    assert snap["fresh_hit"] == 0 and snap["expired_miss"] == 1
    swr.stats.reset()
    assert swr.stats.snapshot()["expired_miss"] == 0


def test_stats_repr(swr):
    assert "SwrStats" in repr(swr.stats)


def test_swr_works_over_sw072_sidecar():
    from _v072 import SingleFlightCache

    base = Cache()
    sf = SingleFlightCache(base)
    c = SwrCache(sf, swr_window=5.0)
    try:
        assert c.get_or_refresh("k", lambda: "v1", ttl=5) == "v1"
        assert c.peek("k") == "v1"
    finally:
        c.close()


# --------------------------------------------------------------------------
# regression: Cache.info() used to deadlock forever
# --------------------------------------------------------------------------

def test_cache_info_does_not_deadlock():
    """CacheStats.to_dict() re-entered a non-reentrant Lock via hit_rate."""
    c = Cache()
    c.set("a", 1)
    c.get("a")
    c.get("missing")

    info = c.info()
    assert info["entries"] == 1
    assert info["hits"] == 1
    assert info["misses"] == 1
    assert info["hit_rate"] == 0.5


def test_cache_stats_to_dict_standalone():
    from fast_cache import CacheStats

    st = CacheStats()
    st.hit()
    st.miss()
    st.miss()
    d = st.to_dict()
    assert d["hits"] == 1 and d["misses"] == 2
    assert d["hit_rate"] == round(1 / 3, 4)


# --------------------------------------------------------------------------
# scalar size fast path (Cache.set was capped at ~78K ops/sec by json.dumps)
# --------------------------------------------------------------------------

def test_scalar_size_estimation_is_sane():
    """Scalar sizes must match len(json.dumps(v)) so memory caps stay
    consistent between scalars and containers."""
    import json

    from fast_cache import _SCALAR_SIZE

    for v in [True, False, 0, 42, -7, 1 << 100, 3.14, "ab", "", None]:
        assert _SCALAR_SIZE(v) == len(json.dumps(v)), v
    # bytes are not JSON-serializable; the legacy path stringified them.
    assert _SCALAR_SIZE(b"abc") == len(json.dumps("b'abc'")) 
    assert _SCALAR_SIZE([1, 2, 3]) is None       # containers defer to json
    assert _SCALAR_SIZE({"a": 1}) is None


def test_memory_usage_accounting_still_works():
    import json

    c = Cache()
    c.set("s", "hello")
    c.set("i", 42)
    assert c.memory_usage == len(json.dumps("hello")) + len(json.dumps(42))
    assert c.info()["entries"] == 2
    assert c.info()["memory_usage"] == c.memory_usage
