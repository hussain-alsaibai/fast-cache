# Agent Context Cache Boundaries With fast-cache

Date: 2026-07-17

## Trend

Agent developer tools are adding more context, more MCP servers, more
observability, and more governance. That makes caching more important, but also
more dangerous: an agent cache can accidentally reuse stale permissions, stale
repository state, or stale cost assumptions.

The useful pattern is not "cache everything." It is explicit cache boundaries:
small TTL scopes, explainable keys, and stats that tell an operator whether the
cache is saving work or hiding drift.

## Why fast-cache fits

`fast-cache` gives agent code a local, bounded cache without adding Redis,
cachetools, or another service. That is useful for:

- Short-lived cron runs that repeat GitHub, web, or package metadata lookups.
- Bounty scanners that score many issues against the same repository facts.
- MCP/tool registries where capability discovery is expensive but changes
  should not be trusted forever.
- Operator dashboards that can reuse recent health probes for a few seconds.

## Boundary-first cache design

Use different caches for different trust windows.

```python
from fast_cache import Cache

repo_facts = Cache(max_size=512, default_ttl=300)
tool_schemas = Cache(max_size=128, default_ttl=60)
negative_results = Cache(max_size=256, default_ttl=30)

def cache_key(repo, topic):
    return f"{repo}:{topic}"

def remember_repo_fact(repo, topic, value):
    repo_facts.set(cache_key(repo, topic), value)

def get_repo_fact(repo, topic):
    return repo_facts.get(cache_key(repo, topic))
```

Recommended TTL posture:

| Cache type | TTL | Reason |
|---|---:|---|
| Repo metadata | 5-15 min | Avoid repeated API reads while respecting active work |
| Tool schemas | 30-120 sec | MCP/tool surfaces can change during development |
| Negative lookups | 15-60 sec | Avoid sticky false negatives |
| Cost/provider pricing | 1-6 hours | Useful for planning, but should be refreshed daily |
| Auth/permission checks | 0 sec or very short | Recheck near side effects |

## What to measure

Expose `stats()` at the end of a run:

- Hit ratio by cache type, not just globally.
- Evictions, which indicate max size is too low or keys are too granular.
- Expirations, which indicate whether TTL is doing real work.
- Avoided tool calls, when the caller can estimate the replacement cost.

## Anti-patterns

- Caching raw prompts or user data when a normalized key would do.
- Sharing one cache across auth, repo state, and cost data.
- Long negative-cache TTLs for "repo not found," "no bounty," or "no issues."
- Reusing cached permission checks right before a public action.
- Hiding stale-while-revalidate behavior in paths that require fresh safety
  decisions.

## OpenClaw fit

OpenClaw's daily developer-tool and bounty crons benefit from local memoization:
the same repos, issue trackers, package metadata, and tool descriptions are read
many times. `fast-cache` is a good default for those repeated reads as long as
each cache declares what it is allowed to remember and how long the answer is
trusted.

## Source signals

- Agent observability comparisons now emphasize trace depth, MCP integration,
  and cost.
- 2026 agent-framework commentary highlights production readiness,
  human-in-the-loop controls, observability, and security boundaries.
- MCP ecosystem discussion is pushing state and capability surfaces into more
  explicit handles.
