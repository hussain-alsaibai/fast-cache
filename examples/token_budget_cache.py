"""TokenBudgetCache — tie `fast-cache` to a per-task spend ceiling.

This example shows how to combine `fast_cache.Cache` with a budget
ledger so an LLM-backed cache (key = prompt hash, value = completion)
refuses to cache or serve a value once the per-task token cap is hit.

It is intentionally framework-agnostic: the budget ledger lives in
*this* file as a minimal in-memory simulation so the example is
self-contained. To swap in a real ledger (e.g. `tiny-budget-ledger`)
just replace the `BudgetLedger` class — the cache decorator keeps the
same shape.

Why this matters: an unbounded LLM cache can quietly burn through
budget when an agent retries the same prompt 50 times. Pairing it with
a token-budget gate makes the cache cost-aware.

Usage::

    from fast_cache import cache as fc_cache
    from examples.token_budget_cache import TokenBudgetCache, SimulatedLedger

    ledger = SimulatedLedger(per_task_tokens=100_000)
    tbc = TokenBudgetCache(
        inner=fc_cache.Cache(max_size=10_000, ttl=3600),
        ledger=ledger,
        tokens_per_completion=lambda v: v.get("tokens", 0),
    )

    @tbc.wrap
    def complete(prompt: str) -> dict:
        # Real call: response["tokens"] would be the model count.
        return {"text": "...", "tokens": 142}

    # First call: 142 tokens recorded.
    # Cache hit within TTL: reuses; no extra budget drawn.
    # Call 700x with the same prompt: 1 served, 699 cache hits, 0 extra tokens.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class BudgetDecision:
    """Result of asking the ledger whether a write is allowed."""

    allowed: bool
    remaining_tokens: int
    reason: str = ""


@dataclass
class BudgetLedger:
    """Minimal interface — replace with tiny-budget-ledger.BudgetLedger in prod."""

    per_task_tokens: int
    spent_tokens: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def can_spend(self, tokens: int) -> BudgetDecision:
        with self._lock:
            if self.spent_tokens + tokens > self.per_task_tokens:
                return BudgetDecision(
                    allowed=False,
                    remaining_tokens=max(0, self.per_task_tokens - self.spent_tokens),
                    reason="per_task_tokens exceeded",
                )
            return BudgetDecision(
                allowed=True,
                remaining_tokens=self.per_task_tokens - self.spent_tokens - tokens,
            )

    def charge(self, tokens: int) -> None:
        with self._lock:
            self.spent_tokens += tokens

    def reset(self) -> None:
        with self._lock:
            self.spent_tokens = 0


# Backwards-compat alias — the report references it.
SimulatedLedger = BudgetLedger


class TokenBudgetCache:
    """Wrap a `fast_cache.Cache` so writes are gated by a token budget.

    Args:
        inner: the underlying fast-cache Cache.
        ledger: anything with ``can_spend(tokens) -> BudgetDecision``
                and ``charge(tokens) -> None``.
        tokens_per_completion: callable taking the cached value, returning
                               the token cost. Default: ``v.get("tokens", 0)``.
        on_block: optional callable invoked when a write is blocked.
    """

    def __init__(
        self,
        inner: Any,
        ledger: BudgetLedger,
        tokens_per_completion: Optional[Callable[[Any], int]] = None,
        on_block: Optional[Callable[[str, BudgetDecision], None]] = None,
    ) -> None:
        self.inner = inner
        self.ledger = ledger
        self.tokens_per_completion = tokens_per_completion or (
            lambda v: int(v.get("tokens", 0)) if isinstance(v, dict) else 0
        )
        self.on_block = on_block
        # stats
        self.hits = 0
        self.misses = 0
        self.budget_blocks = 0
        self.tokens_charged = 0

    def get(self, key: str) -> Any:
        v = self.inner.get(key)
        if v is None:
            self.misses += 1
            return None
        self.hits += 1
        return v

    def set(self, key: str, value: Any) -> bool:
        """Cache the value if budget allows. Returns True on store."""
        tokens = self.tokens_per_completion(value)
        decision = self.ledger.can_spend(tokens)
        if not decision.allowed:
            self.budget_blocks += 1
            if self.on_block is not None:
                self.on_block(key, decision)
            return False
        self.ledger.charge(tokens)
        self.tokens_charged += tokens
        self.inner.set(key, value)
        return True

    def wrap(self, fn: Callable[..., Any]) -> Callable[..., Any]:
        """Decorator. Key = (fn.__name__, args-frozen, kwargs-frozen).

        Cache hits don't touch the budget. Misses: the loader runs, and
        *only the resulting write* charges the budget. If the write is
        blocked, the value is still returned to the caller (we don't
        punish the caller for the budget refusing to memorize) but the
        next call will recompute.
        """
        def cached(*args: Any, **kwargs: Any) -> Any:
            key = f"{fn.__name__}:{_freeze_args(args)}:{_freeze_kwargs(kwargs)}"
            hit = self.get(key)
            if hit is not None:
                return hit
            value = fn(*args, **kwargs)
            self.set(key, value)
            return value
        cached.__wrapped__ = fn  # type: ignore[attr-defined]
        cached.__name__ = getattr(fn, "__name__", "cached")
        return cached


def _freeze_args(args: tuple) -> str:
    return ",".join(repr(a) for a in args)


def _freeze_kwargs(kwargs: dict) -> str:
    return ",".join(f"{k}={v!r}" for k, v in sorted(kwargs.items()))


__all__ = ["BudgetLedger", "BudgetDecision", "SimulatedLedger", "TokenBudgetCache"]


# ----------------- Self-test (run with `python examples/token_budget_cache.py`) -----------------

if __name__ == "__main__":
    # Tiny in-process demo.  NOT shipped as a test — see test_fast_cache.py.
    from fast_cache import Cache  # noqa: E402

    class FakeModel:
        def __init__(self) -> None:
            self.calls = 0

        def complete(self, prompt: str) -> dict:
            self.calls += 1
            return {"text": f"answer for {prompt}", "tokens": 142}

    m = FakeModel()
    ledger = BudgetLedger(per_task_tokens=400)
    tbc = TokenBudgetCache(inner=Cache(maxsize=10, ttl=3600), ledger=ledger)

    @tbc.wrap
    def complete(prompt: str) -> dict:
        return m.complete(prompt)

    # Call same prompt 5x — first call charges 142 tokens + caches, rest hit.
    for _ in range(5):
        print(complete("hi"))

    print(f"hits={tbc.hits} misses={tbc.misses} blocks={tbc.budget_blocks} "
          f"tokens_charged={tbc.tokens_charged} model_calls={m.calls}")
    # -> hits=4 misses=1 blocks=0 tokens_charged=142 model_calls=1

    # Now bust the ledger and try one more distinct prompt — should block.
    ledger.spent_tokens = 400
    print(complete("new prompt"))
    print(f"budget_blocks={tbc.budget_blocks} model_calls={m.calls}")
    # -> budget_blocks=1 model_calls=2  (the value still returns; cache just won't memorize)
