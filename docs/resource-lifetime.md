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

Direct bucket and partition iterators create independent observations. Cancelling
or releasing an iterator ends its observation and releases the retained source.
Composition iterators subscribe independently to one shared observable outcome;
the composition keeps observing for its own lifetime.

Sequence inputs belong to their composition, not to any one consumer. Releasing
one consumer does not stop the other consumers or upstream subscriptions.
Releasing the composition cancels upstream iteration; cancellation remains
cooperative for external sequences. Restarting a sequence input cancels its
previous iterator and prevents its late results from replacing current state.

Each composition materializes its own sequence inputs. Reusing one `Input`
declaration in two compositions creates two subscriptions. The factory must
provide a usable sequence each time; returning the same single-consumer stream
does not turn it into a broadcast source.

Emitted updates use an unbounded buffer. A consumer that processes results more
slowly than observation emits them retains older snapshots. This preserves
already emitted results and reset transitions; changing to latest-only
buffering would change that delivery contract.

Observation may coalesce changes made before it resumes. These streams are
current-state observation, not a durable log of every mutation. Starting a
retry clears the old failure and exposes either retained data or unavailable
state; ordinary bucket loading bookkeeping does not duplicate a successful
result. Compositions may emit repeated equal results after explicit loads.

## Measure indexed snapshots

`IndexedKey` stores an ordered array. Lookup scans that array; normalization
builds an index-to-position dictionary and a normalized result. Store/remove
also normalize input, and the bucket normalizes their output to support custom
key spaces. These operations can matter for large snapshots or repeated lookup
inside rendering loops.

Profile representative snapshots in a Release build before adding auxiliary
indexes or caches. Include mutation and normalization costs as well as lookup
time, and account for snapshots retained by slow asynchronous consumers.

Next: [Partitioning buckets](partitioning.md) ·
[Live compositions](composition.md)
