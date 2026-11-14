# Manage resource lifetime

Retain buckets in a repository or feature owner, and retain compositions for as
long as their derived values are useful.

## Bound partition identities

A bucket retains every accessed partition until released. Equal keys reuse the
same partition. Reset clears its data, but does not evict it or stop its
invalidation subscriptions. There is no automatic eviction.

Prefer bounded identities. Scope repositories using arbitrary search strings
or other unbounded keys to a session lifetime. A selected partition can outlive
its outer bucket when another owner retains it.

## Own observations deliberately

| Resource | Lifetime |
| --- | --- |
| Bucket or partition iterator | Owns an independent observation and retains its source until cancelled or released. |
| Composition iterator | Retains the composition and subscribes to its shared outcome. Ending one iterator does not stop others. |

A repository query owns any subscriptions or lookup tasks it starts.
A composition retains its inputs for as long as it observes them.

Avoid closures that capture an owner which itself retains the composition.
External operations must cooperate with cancellation to stop promptly;
obsolete results are prevented from publishing.

## Account for costs

Iteration uses unbounded buffering to preserve already emitted results and
reset transitions. Slow consumers can retain old snapshots. Observation may
coalesce changes and is not a mutation log; equal results may repeat.

Indexed snapshots are arrays: individual lookup scans them, and normalization
builds an index-to-position dictionary. Profile representative snapshots in a
Release build before introducing additional caches or indexes.

Next: [Partitioning](partitioning.md) · [Compositions](composition.md)
