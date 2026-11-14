# Migrate to Continuum 2.0.0

## Rename singleton bucket aliases

Replace `DataBucket<Value>` with `ValueBucket<Value>` and
`PartitionedDataBucket<Partition, Value>` with
`PartitionedValueBucket<Partition, Value>` in type annotations. Both names
still describe a bucket built with `Key<Value>`.

## Replace deprecated entry points

| Before | Use in 2.0.0 |
| --- | --- |
| `BucketUpdate`, `BucketUpdateSource` | `Update`, `UpdateSource` |
| `bucket.updates()`, `partition.updates()` | Iterate the bucket or selected partition directly. |
| `bucketUpdates(observing:transform:)` | Build and iterate a `Composition` with one `Input` per source. |
| Synchronous `reset()`, `reset(including: .localSources)`, `ResetScope` | `try await reset()` clears memory and writable local snapshots. |

## Select partitions from a container

Create partitioned buckets with `PartitionedBucket(key, partitionedBy: …)`.
Subscripting the container returns the same `Bucket<Space, Operation>` type
used for a standalone bucket. Existing aliases such as `ValueBucket`,
`IndexedBucket`, and `PartitionedValueBucket` still work. Replace a direct
`BucketPartition<Space, Operation>` annotation with `Bucket<Space, Operation>`.
If you spell the container type directly, use
`PartitionedBucket<Partition, Space, Operation>`. For a closure-based mutation,
the operation type is `MutationAction<Space>`.

## Migrate mutations and entry loaders

Replace `Store` and `Remove` with one `Mutate` declaration in the bucket
builder. The handler's first parameter determines the operation type accepted
by `mutate(_:)`. Use `mutation.update` for an established snapshot and
`mutation.update(id:)` for an indexed entry. Pair an edit with its endpoint
using the `remote:` trailing closure. Return `.acknowledged` to keep the
optimistic edit, or `.updated(value)` to publish and persist a server entry
or snapshot. This closure replaces the beta.5 `remotely:` overloads.

A bucket without a typed handler accepts a closure through `mutate(_:)`.
Establish the initial snapshot with `load()`. Use `LocalSource` for a snapshot
that starts in memory. An edit can return `nil` to clear its snapshot, or an
empty array to keep an established empty collection. [Mutation guide](remote-mutations.md)

When spelling `Bucket<Space, Operation>` directly, supply its operation type as
the last argument. The existing bucket
aliases use `MutationAction<Space>` for closure-based mutations. For a typed
handler, use the corresponding `Mutable` alias, such as
`MutableIdentifiedBucket<Card, CardOperation>` or
`PartitionedMutableIdentifiedBucket<ProfileID, Card, CardOperation>`.
[Typed mutation aliases](remote-mutations.md#annotate-a-typed-bucket)

## Name remote loading capabilities

Use `RemoteSource { … }` for a snapshot loader. For indexed entry loading,
use `RemoteSource({ … }, loadEntry: …)`. For pagination, use
`RemoteSource({ … }, nextPage: …, loadEntry: …)` and omit `loadEntry`
when there is no indexed lookup. The `Load` wrapper and nested builder are
removed. Calls to `load(id:using:)` and `loadNext()` keep their spelling.

Replace the generic `MutableBucket` and `PartitionedMutableBucket` aliases
with `Bucket` and `PartitionedBucket`. The value, indexed, and identified
aliases still accept the operation type where their names include `Mutable`.

## Observe refresh errors separately

In 1.1.0 a failed collection refresh published `.result(.failure(error))` even
when `value` or `values` still held a snapshot. In 2.0.0, `latest` and iteration
report `.result(.success(snapshot))` while that snapshot remains available. This
includes empty collections and snapshots restored after a failed mutation or
reset. A failed collection load without a snapshot still publishes a failure.
Successful reset still clears the data and publishes `.reset`.

Keep rendering from `latest` or iteration. Move refresh banners, error logging,
and similar effects that previously relied on a failure update to `error` or
`state.error`. When you start a throwing bucket operation yourself, you can also
handle its error in `catch`. A `.cached` memory hit preserves the error. Entry
loads now report their errors through `error` while keeping the collection's
availability. Pagination continues to report errors through `nextPageError`.

## Pass buckets directly into compositions

Pass a bucket, selected partition, or nested composition directly to
`Input(source)`. The requested load policy is forwarded once to each distinct
source. Use `Input(source, loads: false)` for observation alone. Read
`composition.error` for input and transform errors, including errors while
a snapshot remains available. Call reset on the bucket or query that owns the
data. Compositions observe those resets.

By default, failures in `latest` now carry a `CompositionError`, even for a
single error. Inspect its `errors` array to recover the underlying types. A
custom `mapFailures:` now receives the full list of current errors, including
optional input errors and any error thrown by the transform. Its return value
sets `composition.error` and is shared by `latest` whenever the outcome fails.
[Error mapping →](composition.md#observe-composition-errors)

## Move custom input lifecycle into sources

Replace `Input(update:load:)`, `Input(updates:)`, and per-input load closures
with an observable source. Implement `LoadableUpdateSource` when the composition
should forward loads. If you used `.resolving`, keep the lookup and its state in
the repository query and call the selected bucket's `load(id:using:)` there.
Pass the query to `Input(query)`. Its published snapshot and error are visible
to every consumer. [Repository queries](composition.md#compose-a-repository-query)

## Update Ensemble error presentation

Keep forwarding `.result` to the sink and `.reset` to `sink.reset()`. A failed
refresh that retains data now reaches `ViewData` as success, so failure-phase
handlers will only see actual failure outcomes. Observe `composition.error`
for a composed screen or `bucket.error` for a direct bucket binding. See the
[Ensemble binding example](composition.md#bind-to-ensemble).
