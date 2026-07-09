# Agent Memoization and Dedupe With fast-cache

Date: 2026-07-09

## Trend

Agentic workflows repeat themselves: the same GitHub issue metadata, web page,
package manifest, model pricing data, or CI status is fetched multiple times in
one run. Developers need cheap, bounded memoization before they reach for Redis
or a database. They also need short dedupe windows for webhooks and scheduled
checks.

## Why fast-cache Fits

`fast-cache` is a strong fit for local, single-process automation:

- Memoize API and web fetches inside one agent run.
- Keep delivery IDs or event hashes for a short TTL dedupe window.
- Cache expensive repo scans while a cron job builds a report.
- Use stale-while-revalidate for dashboards where old status is better than no
  status during a transient provider failure.

The library stays easy to vendor into bounties, scripts, and internal tools
because it is one file and has no service dependency.

## Recommended Pattern

```python
import time
import fast_cache as fc

dedupe = fc.Cache(max_size=10_000, default_ttl=900)

def should_process_event(delivery_id):
    if dedupe.get(delivery_id):
        return False
    dedupe.set(delivery_id, {"seen_at": time.time()})
    return True

@fc.cache(max_size=256, ttl=300, stale_ttl=900)
def fetch_issue(repo, number):
    # Replace with a real GitHub/API fetch.
    return {"repo": repo, "number": number}
```

## Product Opportunities

- Add an `examples/webhook_dedupe.py` example.
- Add an `examples/agent_fetch_cache.py` example that memoizes repo metadata.
- Document `stats()` as a natural companion to cron and workflow reports.

## Engagement Hooks

- "Bounded memory for repetitive agent runs."
- "Redis is overkill for a 15-minute webhook dedupe window."
- "Cache tool results inside the process that is already doing the work."
