"""The README must document APIs that actually exist.

This pins the documented surface to the shipped code. The README shipped
three APIs that raised TypeError on use (`swr_ttl=`, `@fc.cache`,
`namespace=`) for months, and a Redis adapter section referencing a package
that was never published. A doc test is the cheapest guard against that
coming back: every kwarg the README shows is checked against the real
signature, and every import path is checked against a real module.
"""

from __future__ import annotations

import importlib
import inspect
import re
import sys
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
README = REPO / "README.md"


def _readme() -> str:
    return README.read_text(encoding="utf-8")


class TestDocumentedCacheKwargs(unittest.TestCase):
    """Every kwarg shown in a Cache(...) snippet must be a real parameter."""

    def test_cache_kwargs_in_readme_are_real(self) -> None:
        from fast_cache import Cache

        params = set(inspect.signature(Cache.__init__).parameters) - {"self"}
        text = _readme()

        # (?<![A-Za-z_]) so that SwrCache(...) and CacheAsync(...) do not match.
        calls = re.findall(r"(?<![A-Za-z_])Cache\(([^()]*)\)", text)
        self.assertTrue(calls, "expected at least one Cache(...) call in the README")

        for call in calls:
            for kwarg in re.findall(r"(\w+)\s*=", call):
                self.assertIn(
                    kwarg,
                    params,
                    "README documents Cache(%s=...) but Cache has no such parameter; "
                    "real parameters are %s" % (kwarg, sorted(params)),
                )


class TestDocumentedImportsExist(unittest.TestCase):
    """Every from X import Y in the README must resolve."""

    def test_documented_imports_resolve(self) -> None:
        sys.path.insert(0, str(REPO))
        try:
            text = _readme()
            pairs = set(re.findall(r"^from (\w+) import ([\w, ]+)$", text, re.M))
            self.assertTrue(pairs, "expected at least one from X import Y in the README")

            for module_name, names in pairs:
                if module_name == "__future__":
                    continue
                try:
                    mod = importlib.import_module(module_name)
                except ImportError:
                    self.fail(
                        "README imports from %r, which is not importable" % module_name
                    )
                for name in [n.strip() for n in names.split(",") if n.strip()]:
                    self.assertTrue(
                        hasattr(mod, name),
                        "README does `from %s import %s` but that name does not exist"
                        % (module_name, name),
                    )
        finally:
            if sys.path and sys.path[0] == str(REPO):
                sys.path.pop(0)


class TestRemovedSectionsStayRemoved(unittest.TestCase):
    """Sections for APIs that were never real must not come back."""

    def test_no_redis_adapter_section(self) -> None:
        self.assertNotIn(
            "tiny_redis_adapter",
            _readme(),
            "the Redis adapter section documented a package that was never published",
        )

    def test_no_unimplemented_swr_kwargs(self) -> None:
        self.assertNotIn(
            "swr_ttl=",
            _readme(),
            "swr_ttl= is not a Cache parameter; use SwrCache(inner, swr_window=...)",
        )


if __name__ == "__main__":
    unittest.main()
