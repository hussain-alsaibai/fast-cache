"""Tests for v0.6.0 observability features:

  * CacheStats.to_prometheus() — Prometheus text exposition format
  * CacheMetricsListener — push-style sink for OpenTelemetry / custom meters
"""
import unittest

from fast_cache import Cache, CacheStats, CacheMetricsListener


class TestPrometheusExport(unittest.TestCase):
    def test_basic_format(self):
        stats = CacheStats()
        stats.hit()
        stats.hit()
        stats.miss()
        stats.eviction()
        stats.expiration()
        stats.insert()
        stats.delete()
        out = stats.to_prometheus()
        # Required HELP/TYPE lines for every metric
        self.assertIn("# HELP fast_cache_hits_total", out)
        self.assertIn("# TYPE fast_cache_hits_total counter", out)
        self.assertIn('fast_cache_hits_total{cache="default"} 2', out)
        self.assertIn('fast_cache_misses_total{cache="default"} 1', out)
        self.assertIn('fast_cache_evictions_total{cache="default"} 1', out)
        self.assertIn('fast_cache_expirations_total{cache="default"} 1', out)
        self.assertIn('fast_cache_inserts_total{cache="default"} 1', out)
        self.assertIn('fast_cache_deletes_total{cache="default"} 1', out)
        # Hit ratio: 2 / (2 + 1) = 0.666666...
        self.assertIn("fast_cache_hit_ratio", out)
        self.assertIn('cache="default"} 0.666667', out)

    def test_namespace_and_label(self):
        stats = CacheStats()
        stats.hit()
        out = stats.to_prometheus(namespace="myapp", cache_label="embeddings")
        self.assertIn("myapp_hits_total", out)
        self.assertIn('cache="embeddings"', out)

    def test_zero_state(self):
        stats = CacheStats()
        out = stats.to_prometheus()
        # No traffic yet — counters at 0, ratio at 0.0
        self.assertIn('fast_cache_hits_total{cache="default"} 0', out)
        self.assertIn('fast_cache_hit_ratio{cache="default"} 0.000000', out)

    def test_trailing_newline(self):
        # Prometheus exposition spec requires a final newline.
        self.assertTrue(CacheStats().to_prometheus().endswith("\n"))


class TestCacheMetricsListener(unittest.TestCase):
    def test_initial_emit_has_no_delta(self):
        stats = CacheStats()
        captured = []

        def sink(delta):
            captured.append(delta)

        listener = CacheMetricsListener(stats, sink)
        delta = listener.emit()
        self.assertEqual(delta, {})

    def test_emit_reports_delta(self):
        cache = Cache(maxsize=10)
        captured = []

        def sink(delta):
            captured.append(delta)

        listener = CacheMetricsListener(cache.stats, sink)
        cache.set("a", 1)
        cache.set("b", 2)
        cache.get("a")
        cache.get("missing")
        delta = listener.emit()
        self.assertEqual(delta.get("insert"), 2)
        self.assertEqual(delta.get("hit"), 1)
        self.assertEqual(delta.get("miss"), 1)
        # Listener was called once with the delta dict.
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0].get("insert"), 2)

    def test_emit_only_deltas_since_last(self):
        cache = Cache(maxsize=10)
        captured = []

        def sink(delta):
            captured.append(dict(delta))

        listener = CacheMetricsListener(cache.stats, sink)
        cache.set("a", 1)
        listener.emit()  # first emit reports insert=1
        cache.set("b", 2)
        cache.set("c", 3)
        delta = listener.emit()  # only the new inserts
        self.assertEqual(delta.get("insert"), 2)
        self.assertNotIn("hit", delta)

    def test_sink_errors_dont_break(self):
        cache = Cache(maxsize=10)
        # Sink that always blows up
        CacheMetricsListener(cache.stats, lambda d: 1 / 0)
        # Cache operations should still work
        cache.set("a", 1)
        self.assertEqual(cache.get("a"), 1)

    def test_listener_attached_to_real_cache(self):
        cache = Cache(maxsize=2)
        sink_calls = []
        listener = CacheMetricsListener(cache.stats, sink_calls.append)
        # Fill to capacity then trigger an eviction.
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("c", 3)  # evicts "a"
        delta = listener.emit()
        self.assertEqual(delta.get("insert"), 3)
        self.assertEqual(delta.get("eviction"), 1)


if __name__ == "__main__":
    unittest.main()
