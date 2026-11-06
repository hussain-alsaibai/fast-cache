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


def test_wheel_manifest_doc_matches_both_modules():
    """The walkthrough names a file list; the wheel must produce exactly it.

    The doc is a code block of bare names. Reading it is cheap; building the
    wheel is not, so this asserts the two things a stale document gets wrong:
    the count it advertises, and that every module it names exists here.
    """
    import re
    from pathlib import Path

    root = Path(__file__).parent.parent
    doc = (root / "docs" / "dist-manifest-walkthrough.md").read_text(encoding="utf-8")

    block = re.search(r"```\n(_v072\.py.*?)```", doc, re.S)
    assert block, "walkthrough no longer contains the file listing block"
    listed = [line.strip() for line in block.group(1).splitlines() if line.strip()]

    claim = re.search(r"\(wheel, (\d+) files\)", doc)
    assert claim, "walkthrough no longer states a file count"
    assert len(listed) == int(claim.group(1)), (
        f"walkthrough says {claim.group(1)} files but lists {len(listed)}"
    )

    modules = [n for n in listed if n.endswith(".py")]
    assert len(modules) == 5, f"expected 5 shipped modules, doc lists {modules}"
    for name in modules:
        assert (root / name).exists(), f"walkthrough lists missing module {name}"
    assert len([n for n in listed if ".dist-info/" in n]) == 5
