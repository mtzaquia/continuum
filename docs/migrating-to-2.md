# Migrate to Continuum 2.0

This guide covers the 2.0 API for apps upgrading from 1.1 or an earlier 2.0 prerelease. See [Install](../README.md#install) for the current version pin.

## Update bucket declarations

| Earlier spelling | 2.0 |
| --- | --- |
| `DataBucket<Value>` | `ValueBucket<Value>` |
| `PartitionedDataBucket<ID, Value>` | `PartitionedValueBucket<ID, Value>` |
| `Bucket(key, partitionedBy: ID.self)` | `PartitionedBucket(key, partitionedBy: ID.self)` |
| `Bucket<Space, Scope>` in 1.1 or `Bucket<Space, Scope, Operation>` in betas | `Bucket<Space, Operation>` |
| `BucketPartition` | The selected partition is a `Bucket<Space, Operation>`. |
| `MutableBucket<Space, Operation>` | `Bucket<Space, Operation>` |
| `PartitionedMutableBucket<ID, Space, Operation>` or earlier `PartitionedBucket<ID, Space>` | `PartitionedBucket<ID, Space, Operation>` |

Use `MutationAction<Space>` as the operation type for a closure-based bucket. The [bucket aliases](partitioning.md#choose-a-bucket-type) and [typed mutation aliases](remote-mutations.md#annotate-a-typed-bucket) cover common cases.

## Update remote loading and mutation configuration

- Keep `RemoteSource { … }` for a snapshot. Replace the nested builder and `Load` wrappers with `RemoteSource({ … }, nextPage: NextPage { … }, loadEntry: { id in … })`, supplying the capabilities you use. Earlier `LoadEntry` declarations become `loadEntry:`. [Loading](loading.md) · [Pagination](pagination.md)
- Replace `Store`, `Remove`, `store(_:)`, and `remove` with `Mutate` and `mutate(_:)`. Establish the initial snapshot with `load()`. Edit it with `mutation.update` or `mutation.update(id:)`. The `remote:` closure returns `.acknowledged` to keep the edit or `.updated(value)` to apply the server value. This also replaces prerelease `remotely:` and `mutation.replace(with:)`. [Mutations](remote-mutations.md)

## Update observation and reset

| Earlier spelling | 2.0 |
| --- | --- |
| `BucketUpdate`, `BucketUpdateSource` | `Update`, `UpdateSource` |
| `bucket.updates()`, `partition.updates()` | Iterate the bucket directly. |
| `bucketUpdates(observing:transform:)` | Declare one `Input` per source in a `Composition`. |
| Synchronous `reset()`, `reset(including:)`, `ResetScope` | `try await bucket.reset()` clears memory and writable local snapshots. |

A failed refresh keeps a usable snapshot as `.result(.success(snapshot))`. Use `bucket.error` or `composition.error` for refresh feedback. A collection load with no usable snapshot can still publish a failure. Composed failures default to `CompositionError`, even for a single error. `mapFailures:` receives current input errors, including optional and nested inputs, followed by any transform error. [Composition errors](composition.md#observe-composition-errors)

For Ensemble, keep forwarding `.result` to `sink.receive` and `.reset` to `sink.reset`. A failure banner for retained data can read the source's `error`. [Ensemble binding](composition.md#bind-to-ensemble)

## Update composition inputs

- Pass buckets, resolved buckets, and nested compositions to `Input(source)`. Remove closures that only forwarded the load policy. Use `Input(source, loads: false)` in place of a no-op load closure.
- Replace `Input(update:load:)`, `Input(updates:)`, `InputSubscriptionBehavior`, and custom per-input load closures with an observable `UpdateSource`. The external source owns its requests, subscriptions, and reset. Call its methods directly. [External source example](composition.md#observe-an-external-source)
- Replace `Input(posts).resolving(\.authorID, from: authors)` with `Input(posts.resolving(\.authorID, from: authors))`. Retain the resolved source to share or retry its work. The transform receives one `ResolvedSnapshot` with `root` and `related`. Configure former one-shot lookups as `loadEntry:` on the related bucket, and call `loadNext()` on the root bucket. [Resolve related entries](composition.md#resolve-related-entries)
