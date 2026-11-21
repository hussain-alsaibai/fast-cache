"""v0.9.0 regressions: expired entries must not inflate size or memory.

Every accessor that reports *live* state (``size``, ``memory_usage``, ``info``,
``len``) used to read ``len(self._data)`` / ``self._total_size`` directly.
Those counts include entries whose TTL has passed but which nothing has
touched yet -- lazy TTL means nothing deletes them until a ``get`` hits that
exact key. So a cache that expired entirely reported full size and full
memory forever, and the README's own advice (``cache.memory_usage`` to watch
growth) was reading a number that only ever went up.

``keys()``/``values()``/``items()`` skipped expired entries when yielding but
never reclaimed them, so iterating a fully-expired cache printed ``[]`` while
``len()`` still said 3.

The fix reclaims expired entries before reporting. Nothing observable is lost:
an expired entry can never be returned by any accessor.
"""

import time

import pytest

from fast_cache import Cache


def _wait(seconds: float = 0.08) -> None:
    time.sleep(seconds)


def test_size_excludes_expired_entries():
    c = Cache(ttl=0.05)
    for i in range(3):
        c.set(f"k{i}", "v" * 10)
    assert c.size == 3
    _wait()
    assert c.size == 0


def test_memory_usage_excludes_expired_entries():
    c = Cache(ttl=0.05)
    for i in range(3):
        c.set(f"k{i}", "v" * 100)
    assert c.memory_usage > 0
    _wait()
    assert c.memory_usage == 0


def test_len_excludes_expired_entries():
    c = Cache(ttl=0.05)
    c.set("a", 1)
    c.set("b", 2)
    assert len(c) == 2
    _wait()
    assert len(c) == 0


def test_info_reports_live_entries_only():
    c = Cache(ttl=0.05)
    c.set("a", "x" * 50)
    _wait()
    info = c.info()
    assert info["entries"] == 0
    assert info["memory_usage"] == 0


def test_iteration_then_size_agree():
    """The old shape: keys() said [] while size still said 3."""
    c = Cache(ttl=0.05)
    for i in range(3):
        c.set(f"k{i}", i)
    _wait()
    assert list(c.keys()) == []
    assert c.size == 0
    assert list(c.values()) == []
    assert list(c.items()) == []


def test_live_entries_survive_the_purge():
    """Mixed TTLs: only the expired ones are reclaimed."""
    c = Cache()
    c.set("short", "gone", ttl=0.05)
    c.set("long", "kept", ttl=60)
    _wait()
    assert c.size == 1
    assert c.get("long") == "kept"
    assert c.has("short") is False


def test_purge_does_not_double_count_memory():
    """Re-setting a key after a purge keeps accounting exact."""
    c = Cache(ttl=0.05)
    c.set("k", "v" * 100)
    _wait()
    assert c.memory_usage == 0
    c.set("k", "w" * 100)
    assert c.size == 1
    assert c.memory_usage > 0
    # A second read must not drift the counter.
    first = c.memory_usage
    assert c.memory_usage == first


def test_unlimited_cache_still_reports_correctly():
    c = Cache()
    for i in range(5):
        c.set(f"k{i}", i)
    assert c.size == 5
    assert len(c) == 5
    assert c.info()["entries"] == 5


@pytest.mark.parametrize("reader", ["size", "memory_usage", "info", "len"])
def test_eviction_stats_not_inflated_by_purge(reader):
    """Reclaiming expired entries is not an eviction."""
    c = Cache(ttl=0.05)
    c.set("a", 1)
    _wait()
    if reader == "info":
        c.info()
    elif reader == "len":
        len(c)
    else:
        getattr(c, reader)
    assert c.stats.evictions == 0
