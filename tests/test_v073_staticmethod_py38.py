"""Regression tests for fast-cache v0.7.3 -- Python 3.8/3.9 staticmethod support.

``staticmethod`` objects only became directly callable in Python 3.10
(bpo-43682). Before that, calling one raises::

    TypeError: 'staticmethod' object is not callable

``cache_method`` stacked on top of ``@staticmethod`` therefore broke on
Python 3.8 and 3.9: the decorator held the descriptor and called it
directly. The fix unwraps ``fn.__func__`` and re-wraps the result as a
``staticmethod`` so class and instance access keep working without
binding ``self``/``cls``.

These tests simulate the pre-3.10 behaviour with a ``staticmethod``
subclass whose ``__call__`` raises, exactly like the 3.8/3.9 builtin.
"""

from __future__ import annotations

import pytest

from fast_cache import Cache, cache_method


class Py38StaticMethod(staticmethod):
    """A ``staticmethod`` that cannot be called directly.

    On Python 3.8/3.9 the builtin has no ``__call__`` at all, so any code
    that calls the descriptor object directly raises ``TypeError``.
    Subclassing and adding a raising ``__call__`` reproduces that.
    """

    def __call__(self, *args, **kwargs):
        raise TypeError("'staticmethod' object is not callable")


def test_cache_method_on_staticmethod_works_on_py38():
    """The decorator must unwrap the descriptor, not call it."""
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


def test_cache_method_on_py38_staticmethod_object():
    """A non-callable staticmethod object must still be handled."""
    cache = Cache()
    calls = []

    def compute(x):
        calls.append(x)
        return x + 1

    wrapped = cache_method(cache, prefix="svc")(Py38StaticMethod(compute))

    assert wrapped(10) == 11
    assert wrapped(10) == 11
    assert calls == [10]


def test_cache_method_staticmethod_does_not_bind_self():
    """Instance access must not pass ``self`` as the first argument."""
    cache = Cache()
    calls = []

    class Service:
        @cache_method(cache, prefix="svc")
        @staticmethod
        def compute(x):
            calls.append(x)
            return x + 1

    assert Service().compute(10) == 11
    assert calls == [10]


def test_cache_method_rejects_classmethod():
    """Applying the decorator to a ``classmethod`` must fail loudly."""
    cache = Cache()

    class Service:
        @classmethod
        def compute(cls, x):
            return x + 1

    with pytest.raises(TypeError, match="classmethod"):
        cache_method(cache, prefix="svc")(Service.__dict__["compute"])
