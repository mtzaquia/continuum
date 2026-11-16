# Diagnose bucket operations

Enable Apple unified logging during development:

```swift
import Continuum

#if DEBUG
Continuum.debug = .normal
#endif
```

The process-wide setting is safe to access from any concurrency domain.

| Level | Output |
| --- | --- |
| `.off` | No optional diagnostics; the default. |
| `.normal` | Requests, cache decisions, shared work, supersession, failures, and results. |
| `.trace` | Normal output plus bucket configuration, source attempts, mutation steps, and persistence activity. |

## Follow an operation

Filter Console by subsystem `eu.lelfe.continuum`. Narrow by category
(`load`, `mutation`, `persistence`, `pagination`, `invalidation`, `operation`, or
`lifecycle`) or an operation identifier:

```text
[load][op:12AB34CD] ⇢ requested bucket="posts" version=1 instance=91EAF870 policy=cached established=true
[load][op:12AB34CD] ↻ requires remote validation bucket="posts" version=1 instance=91EAF870 freshness=stale age=304.0s stale-after=300.0s
[load][op:12AB34CD] ✓ loaded bucket="posts" version=1 instance=91EAF870 origin=remote count=14 next-page=true
```

Shared-work messages identify the operation joined; supersession messages name
the replacement. Each partition has its own instance identifier. Nested
Continuum operations preserve the surrounding operation identifier.

Cached loads log why they reuse memory or require remote validation. An
unvalidated bucket has yet to complete a full remote load. Local-only buckets
with `staleAfter` set report `freshness=ignored-no-remote` when they reuse memory.
Trace-level bucket setup logs the configured `staleAfter` in seconds, or
`unlimited` when omitted. [Snapshot freshness](loading.md#refresh-stale-snapshots)

## Know what is logged

Logs include namespaces, schema versions, generated identifiers, source and
snapshot counts, policies, and error type names. They exclude model values,
partition keys, cursors, payloads, and free-form error descriptions. Use stable
schema names rather than user-entered text for namespaces.

Optional diagnostics are compiled out of Release builds. Loading a bucket with
no sources always emits a warning; its normal loading and error behavior is
unchanged by the diagnostic level.

[Loading](loading.md) · [Operation ordering](operation-ordering.md)
