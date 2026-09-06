# Manage resource lifetime

Buckets retain cached snapshots and the work needed to observe them. Scope
repository ownership and observation tasks to the period when those resources
are useful.

## Bound partition identities

The first subscript access creates a partition. Equal identities reuse it, and
the outer bucket retains every accessed partition until the bucket is released.
Reset clears data and pagination without removing the partition or its
invalidation subscriptions.

Prefer bounded identities such as a small set of query variants. Arbitrary
search strings, timestamps, or ever-changing cursors can grow a long-lived
bucket's partition storage indefinitely. There is no automatic eviction API;
scope such a repository to a search/session lifetime when appropriate.

An external reference to a selected partition can keep it alive after its
outer bucket is released. An active update observation also retains its
sources until termination.

## Cancel observation tasks

Each `updates()` or `bucketUpdates` call creates independent observation work.
Cancel the task iterating the stream when its consumer is no longer needed.
If the stream is retained, simply breaking out of a loop does not explicitly
cancel that observation.

Emitted updates use an unbounded buffer. A consumer that processes results more
slowly than observation emits them retains older snapshots. This preserves
already emitted results and reset transitions; changing to latest-only
buffering would change that delivery contract.

Observation may coalesce changes made before it resumes. These streams are
current-state observation, not a durable log of every mutation. Starting a
retry clears the old failure and exposes either retained data or unavailable
state; ordinary loading bookkeeping does not duplicate a successful result.

## Measure indexed snapshots

`IndexedKey` stores an ordered array. Lookup scans that array; normalization
builds an index-to-position dictionary and a normalized result. Store/remove
also normalize input, and the bucket normalizes their output to support custom
key spaces. These operations can matter for large snapshots or repeated lookup
inside rendering loops.

Run the standalone Release benchmark described in
[Validation](../Validation/README.md) before choosing a different storage
representation. It measures lookup and the complete key-level mutation plus
normalization pipeline at several collection sizes. The benchmark does not
include network latency, persistence, or UI rendering.

A local Apple Silicon baseline on 6 September 2026 using Swift 6.3.3 produced
these median totals (milliseconds):

| Rows | 2,000 lookups | 100 stores plus normalization |
| --- | ---: | ---: |
| 100 | 14.23 | 7.74 |
| 1,000 | 129.74 | 68.72 |
| 10,000 | 1,306.83 | 654.98 |

These are measurements of this fixture, not latency promises for applications.
They support measuring repeated indexed access in large collections before
choosing an auxiliary index. No auxiliary index or changed normalization
contract is introduced by these correctness fixes. Use measurements from representative workloads to justify
that additional state and its invalidation rules.

Next: [Partitioning buckets](partitioning.md) ·
[Repository composition](repository-composition.md)
