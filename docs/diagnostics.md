# Diagnose bucket operations

Continuum diagnostics turn one bucket operation into a short, correlated story:
what was requested, which source or snapshot resolved it, why work waited or
stopped, and what state resulted. Diagnostics use Apple unified logging and are
off by default.

## Choose a level at startup

Set the process-wide level before creating or using buckets:

```swift
import Continuum

#if DEBUG
Continuum.debug = .normal
#endif
```

`Continuum.debug` is safe to read or write from any concurrency domain. The
setting affects every bucket in the process.

| Level | Events |
| --- | --- |
| `.off` | No optional diagnostic events. This is the default. |
| `.normal` | Requests, cache decisions, shared-work waits, supersession, failures, and final state. |
| `.trace` | Everything in normal, plus source attempts, mutation pipeline details, and local-persistence lifecycle. |

Normal is intended for routine development. Trace helps explain source
selection, queued mutations, and write-through ordering when a normal story
does not identify the cause.

## Read one operation as a story

Every public load, mutation, reset, invalidation, and page request receives a
compact operation identifier. Events also include the key namespace, schema
version, and a short bucket-partition instance identifier:

```text
[load][op:12AB34CD] ⇢ requested bucket="posts" version=1 instance=91EAF870 policy=cached-then-remote established=false
[load][op:12AB34CD] ✓ local source resolved bucket="posts" version=1 instance=91EAF870 source=1 count=12
[load][op:12AB34CD] ✓ published cached snapshot while refreshing bucket="posts" version=1 instance=91EAF870 count=12
[load][op:12AB34CD] ✓ loaded bucket="posts" version=1 instance=91EAF870 origin=remote count=14 next-page=true
```

When callers share active work, the waiting caller logs the active operation
identifier it joined. Remote loads, mutations, and resets state which earlier
load or page operation they superseded. Nested Continuum operations preserve
an existing operation identifier, and the outer identifier is restored when a
nested diagnostic scope ends.

Partition values are deliberately absent. The per-partition instance identifier
distinguishes overlapping partitions without publishing their domain values.

## Understand the privacy boundary

Normal and trace events include structural facts only:

- key namespace and schema version;
- generated operation and bucket-instance identifiers;
- source positions, destination counts, snapshot counts, and loaded state;
- load policy, source origin, mutation kind, and pagination availability;
- error type names.

Neither level logs model values, lookup inputs, partition values, pagination
cursors, local or remote payloads, or free-form error descriptions. A key
namespace appears as a quoted, escaped public string, so use stable schema names
rather than user-entered text when declaring keys.

## Filter unified logs

Continuum uses the stable subsystem `com.mtzaquia.Continuum`. Events use these
categories:

- `lifecycle`
- `load`
- `operation`
- `mutation`
- `persistence`
- `pagination`
- `invalidation`

Filter by subsystem to follow all bucket activity, then narrow by category or
the `op:` identifier when investigating one operation.

## Account for release builds

Optional diagnostic calls are compiled out of non-debug builds. Changing
`Continuum.debug` in a release build is safe but emits no optional events.
Loading a bucket with no local or remote source emits an unconditional warning
identifying it as an in-memory bucket. The existing
`ContinuumError.missingRemoteSource(namespace:)` error and observable failure
state are otherwise unchanged by the diagnostic level.

Next: [Loading snapshots](loading.md) ·
[Persisting local snapshots](persistence.md) ·
[Paginating buckets](pagination.md)
