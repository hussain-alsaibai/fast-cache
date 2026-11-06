# fast-cache × tiny-dist-audit — a worked adoption guide

`fast-cache` is our zero-dependency LRU + TTL + stale-while-revalidate cache.
Its docs are the ones developers actually read before trusting a package.

This is a **worked example** of using
[`tiny-dist-audit`](https://github.com/hussain-alsaibai/tiny-dist-audit) to
answer a question the README cannot: *what does the published `fast-cache`
wheel actually contain, and did it change?*

Every number below was produced by running the commands shown, against
`fast-cache` 0.8.0 on 2026-11-06. Nothing here is illustrative-only.

## 1. Build the artifact, then manifest it

```bash
python -m pip wheel --no-deps --no-build-isolation -w /tmp/fc-dist .
ls /tmp/fc-dist
# fast_cache-0.8.0-py3-none-any.whl
```

```bash
python -m tiny_dist_audit manifest /tmp/fc-dist/fast_cache-0.8.0-py3-none-any.whl \
  -o fast_cache-0.8.0.manifest.json
```

## 2. What the manifest says about this wheel

```
artifact : fast_cache-0.8.0-py3-none-any.whl (wheel, 10 files)
name/ver : fast-cache 0.8.0
```

The ten entries are:

```
_v072.py
_v080.py
fast_cache.py
fast_cache_v071.py
fast_cache_v072.py
fast_cache-0.8.0.dist-info/METADATA
fast_cache-0.8.0.dist-info/RECORD
fast_cache-0.8.0.dist-info/WHEEL
fast_cache-0.8.0.dist-info/licenses/LICENSE
fast_cache-0.8.0.dist-info/top_level.txt
```

Tree digest: `05f8905adafcf94746dad7de1df2d6017c258fcdb79dcbbb955f66d186fdd886`.

Two facts fall out of that, and both matter to anyone deciding whether to
depend on this package:

1. **The historical sidecars ship.** `fast_cache_v071.py` and
   `fast_cache_v072.py` are installed alongside the current module. They are
   not dead weight exactly — `test_fast_cache_v071.py` and
   `test_fast_cache_v072.py` still import them — but they are three
   `__version__` strings in one environment, and `import fast_cache` is not the
   only name that resolves. The manifest makes this visible in one line.
2. **`llms.txt` does not ship.** It exists in the repository, so an agent that
   reads the repo gets it, and an agent that reads the installed package does
   not. That is a packaging decision, not a bug — but it is the kind of thing
   nobody notices until they look at the file list.

## 3. Pin it, then diff every release

Commit `fast_cache-0.8.0.manifest.json`. On the next release, before publishing:

```bash
python -m tiny_dist_audit check /tmp/fc-dist/fast_cache-0.8.1-py3-none-any.whl \
  --baseline fast_cache-0.8.0.manifest.json --fail-on high
```

A file that appears or disappears, a byte that changed, and any change to
`Requires-Python` or `Requires-Dist` all show up by name, with exit code 5.
For a package that promises "zero dependencies", the `Requires-Dist` line is
the one worth watching: a stray dependency entering a zero-dependency package
is invisible in a changelog and obvious in a manifest diff.

## 4. Where this does *not* help

- It does not tell you whether `fast_cache.py` is correct or fast. It hashes
  it; it does not understand it.
- It does not prove the wheel you downloaded from PyPI matches this build.
  Compare digests yourself, or wire the manifest into the publishing job.
- It does not scan for secrets inside files — only for credential-shaped
  **names** like `.env` and `*.pem`.

## Reproduce it

```bash
git clone https://github.com/hussain-alsaibai/fast-cache
cd fast-cache
python -m pip wheel --no-deps --no-build-isolation -w /tmp/fc-dist .
pip install tiny-dist-audit
python -m tiny_dist_audit check /tmp/fc-dist/*.whl \
  --baseline docs/fast_cache-0.8.0.manifest.json --fail-on high
```

Exit code 0 means the artifact you just built is byte-identical, by file, to
the one recorded at 0.8.0.
