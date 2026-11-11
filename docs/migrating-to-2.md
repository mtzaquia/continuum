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

Configured bucket initializers default `staleAfter` to `nil`. `Input(source)`
and `Input(source, load:)` use one initializer with an optional load action.
Direct calls keep their existing spelling. If you pass an initializer as a
function value, use the full signature or wrap the call in a closure to use
the defaults.

## Migrate mutations and entry loaders

Replace `Store` and `Remove` with one `Mutate` declaration in the bucket
builder. The handler's first parameter determines the operation type accepted
by `mutate(_:)`. Use `mutation.update` for an established snapshot and
`mutation.update(id:)` for an indexed entry. Pair an edit with its endpoint
using `remotely:`. The endpoint can return the authoritative entry or snapshot
for Continuum to publish and persist.

A bucket without a typed handler accepts a closure through `mutate(_:)`.
Use `mutation.replace(with:)` to deliberately establish a complete snapshot
or clear it with `nil`. Indexed edits can return an empty array to keep an
established empty collection. [Mutation guide](remote-mutations.md)

When spelling the generic types directly, `Bucket` and `BucketPartition`
now take the operation type as their last argument. The existing bucket
aliases use `MutationAction<Space>` for closure-based mutations. For a typed
handler, use the corresponding `Mutable` alias, such as
`MutableIdentifiedBucket<Card, CardOperation>` or
`PartitionedMutableIdentifiedBucket<ProfileID, Card, CardOperation>`.
[Typed mutation aliases](remote-mutations.md#annotate-a-typed-bucket)

Replace `LoadEntry { id in … }` with `Load { id in … }`. The call site
`load(id:using:)` keeps its spelling.
If you explicitly spell `Load`'s generic arguments, use `Load<Void, Snapshot>`
for a complete snapshot and `Load<ID, Value>` for an entry.

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

Remove any custom `UpdateSource` adapter whose purpose was to turn a failure
with retained data back into success. Pass the bucket or selected partition
directly to `Input`, including when you configure a load action. The transform
can still run as other inputs change. Read `composition.error` for the current
errors, including failures from optional and nested inputs.

`composition.load(using:)` now loads bucket and selected partition inputs with
the requested policy. Remove closures that only forward the policy to the
bucket. Keep custom actions, and pass `Input(bucket) { _ in }` for a bucket
that should only be observed.

Nested composition inputs also load with the parent's policy. Remove closures
that only forward the policy to a child. Use `Input(child) { _ in }` to keep
the child observation-only through that input. Each top-level call loads shared
children once across the inputs that forward automatically.

By default, failures in `latest` now carry a `CompositionError`, even for a
single error. Inspect its `errors` array to recover the underlying types. A
custom `mapFailures:` now receives the full list of current errors, including
optional input errors and any error thrown by the transform. Its return value
sets `composition.error` and is shared by `latest` whenever the outcome fails.
[Error mapping →](composition.md#observe-composition-errors)

## Start sequence inputs on first use

`Input(updates:)` starts its subscription when the composition is first read
through `latest` or `error`, iterated, or loaded. If you relied on construction
to start a subscription, read `latest` at that point. Calling `load()` also
runs configured loading actions. [Sequence inputs →](composition.md#bridge-result-sequences)

## Update Ensemble error presentation

Keep forwarding `.result` to the sink and `.reset` to `sink.reset()`. A failed
refresh that retains data now reaches `ViewData` as success, so failure-phase
handlers will only see actual failure outcomes. Observe `composition.error`
for a composed screen or `bucket.error` for a direct bucket binding. See the
[Ensemble binding example](composition.md#bind-to-ensemble).
