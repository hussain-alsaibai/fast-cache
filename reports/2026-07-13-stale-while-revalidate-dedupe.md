# Stale-While-Revalidate Dedupe with fast-cache

Date: 2026-07-13

## Trend

Webhook receivers, scan caches, and price-data fetchers all hit the same
shape of problem: a request comes in, you want to either return a recent
result or trigger a fresh lookup, and you never want to do *both* on the
same logical event. The two clean primitives for this are
**stale-while-revalidate** (return the last known value while a background
refresh is in flight) and **atomic add** (claim a slot exactly once per
delivery window).

The OpenClaw webhook stack now uses both. `tiny-router`'s
`agent_callback_receiver.py` recipe relies on `Cache.add()` for delivery-ID
dedupe and `Cache.get()` + `prune()` for the lifetime of the in-process
dedupe slot.

## What Shipped

- **`Cache.add(key, value, ttl=None)`** — atomic claim semantics. Returns
  `True` if the key was absent (and inserts it), `False` if a live entry
  already existed. Perfect for the first writer pattern: if you add, you
  own the side effect.
- **`Cache.touch(key, ttl=None)`** — sliding-window keepalive. Returns
  `True` if the entry was live and its TTL was extended; `False` if the
  slot was missing or already expired. Lets an idempotency sweep keep a
  delivery's dedupe window alive while work is still in flight.
- **8 new tests** in `test_fast_cache.py::TestAddAndTouch` covering claim
  semantics, expired-slot reuse, no-TTL behaviour, and a full webhook
  dedupe recipe. Full suite is now 26 tests, all green.

## Recommended Pattern

```python
import fast_cache as fc

# Webhook delivery dedupe: first writer wins, replays no-op for 5 minutes.
seen = fc.Cache(default_ttl=300)

def on_callback(delivery_id: str, payload: dict) -> str:
    if seen.add(delivery_id, payload):
        do_side_effect(payload)  # only the first writer runs this
        return "accepted"
    return "duplicate"

# Stale-while-revalidate price cache: always serve something, refresh in
# the background when stale.
prices = fc.Cache(default_ttl=60, stale_ttl=600)

def price(symbol: str) -> float:
    v = prices.get(symbol)
    if v is not None:
        return v
    v = fetch_live(symbol)         # the canonical miss path
    prices.set(symbol, v)
    return v
```

## Why It Matters for Agents

- **Webhook + cron dedupe** is the smallest meaningful guard against
  duplicate side effects. Without it, retries become re-payments, re-emails,
  and re-PR-creations.
- **SWR is the cheapest reliability upgrade** for any external data feed
  with a quiet-but-costly refresh window. The dashboard never goes empty,
  and the caller never blocks on the upstream provider.
- **Sliding keepalive via `touch()`** keeps idempotency windows honest when
  work spans multiple background steps. If a job is still running at the
  four-minute mark, you do not want a concurrent retry to look like a new
  request.

## Engagement Hooks

- "Claim the slot once, do the work once, return the same response forever."
- "Webhook glue should be a 5-line dedupe, not a Redis cluster."
- "If your cache can do `add`, your retry loop can stop being careful."