# Negative Caching for Agent Scanners With fast-cache

Date: 2026-07-12

## Trend

Autonomous developer workflows do a lot of exploratory reads: check whether a
GitHub issue still has no competing PR, whether a package has a security policy,
whether docs expose a machine-readable endpoint, whether a provider status page
is healthy, or whether a bounty target still accepts submissions.

Most cache examples focus on successful fetches. Agent scanners also need to
cache misses and refusals for short periods. Without negative caching, the same
run repeatedly asks for data that is absent, rate-limited, closed, or not worth
acting on.

## Why fast-cache Fits

`fast-cache` is useful for negative caching because it is local, bounded, and
easy to inspect in a final report:

- Store "not found", "closed", "saturated", or "rate limited" decisions with
  shorter TTLs than successful results.
- Avoid hammering GitHub, package registries, and docs sites during a single
  scan.
- Keep skip reasons visible through `stats()` and explicit cached values.
- Use `prune()` before writing report output for long-running jobs.

This is a developer-productivity win because the scanner spends more time
ranking useful work and less time re-discovering the same dead ends.

## Recommended Pattern

```python
import fast_cache as fc

scan_cache = fc.Cache(max_size=4096, default_ttl=900)

NEGATIVE_TTL = 300
POSITIVE_TTL = 1800

def cache_issue_state(key, state):
    ttl = NEGATIVE_TTL if state["status"] in {"closed", "saturated", "missing"} else POSITIVE_TTL
    scan_cache.set(key, state, ttl=ttl)

def get_issue_state(repo, number, fetch):
    key = f"issue:{repo}#{number}"
    cached = scan_cache.get(key)
    if cached is not None:
        return cached

    state = fetch(repo, number)
    cache_issue_state(key, state)
    return state
```

## Product Opportunities

- Add an `examples/negative_cache.py` scanner snippet.
- Document TTL guidance: short TTL for misses/refusals, longer TTL for stable
  metadata.
- Extend README positioning around "cache decisions, not just values."

## Engagement Hooks

- "Do not re-check the same closed bounty issue ten times in one run."
- "Negative caching is retry control for scanners."
- "Cache the skip reason so the final report can explain itself."
