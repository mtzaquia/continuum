# Changelog

## Unreleased (target 2.0.0)

- Removed `mutation.replace(with:)`. Establish a snapshot with `load()` and
  edit it with `mutation.update`.
- `mutation.update` runs the endpoint in its `remote:` trailing closure.
  Return `.acknowledged` to keep the optimistic edit or `.updated(value)`
  to apply the server value.
- Forced remote loads wait for pending mutations and resets before replacing
  reads. Reset clears the snapshot and revokes ownership before suspending.
  Running mutations can finish their requests while their old snapshot edits
  are discarded.
- One `Mutate` handler and `mutate(_:)` replace `Store`, `Remove`, and their
  bucket methods. Snapshot edits publish and persist optimistically. Returned
  server entries and snapshots take precedence.
- Configure remote sources with an unlabeled snapshot or initial-page closure
  and named `loadEntry` and `nextPage` arguments.

- Buckets and selected partitions keep established snapshots available after a
  failed refresh. Existing subscribers and new subscribers receive the retained
  value as a success. A cached memory hit preserves the operation error, which
  remains observable through `error` and `state.error`.
- Mutation and reset rollback restore the previous availability. Entry load and
  invalidation stream errors are visible through operation state. A failed
  collection load without a snapshot still publishes failure.
- Required bucket inputs continue to contribute their retained snapshots to
  compositions after a refresh fails. Ensemble bindings receive successful data
  and can observe the composition's `error` for refresh feedback.
- `Composition.error` reports current input and transform errors, including
  errors from optional and nested inputs. `mapFailures:` maps the full list
  once, and a failed `latest` shares the same mapped error. The default returns
  an ordered `CompositionError`.
- `PartitionedBucket` returns the same `Bucket<Space, Operation>` type used
  for standalone snapshots. Pagination, entry loads, and mutations stay on
  the selected bucket.
- `Input(source)` observes buckets, nested compositions, and repository
  queries. `LoadableUpdateSource` opts a source into load forwarding. Shared
  sources receive the policy once per call. Use `loads: false` for observation
  alone. Queries own any sequence subscriptions and relationship fetches.
- Reset buckets and queries directly. Required-input invalidation propagates
  through nested compositions.
- Retired the deprecated update aliases, `bucketUpdates`, `updates()`, and the
  legacy reset overloads and `ResetScope`. Iterate sources directly, use
  `Composition` to combine inputs, and call `try await reset()` to reset.
- Configured buckets default `staleAfter` to `nil`.

For bucket and composition API migrations, see the [2.0.0 migration guide](docs/migrating-to-2.md).

### Bucket type aliases

- Renamed `DataBucket` to `ValueBucket` and `PartitionedDataBucket` to
  `PartitionedValueBucket`. Update type annotations that use the old names.
- Added `IdentifiedBucket<Value>` and `PartitionedIdentifiedBucket<Partition, Value>`
  for collections indexed by the value's `Identifiable.ID`. Use `IndexedBucket`
  and `PartitionedIndexedBucket` to specify the index type explicitly.
- Added `Mutable` variants of the value, indexed, and identified bucket
  aliases for typed `Mutate` handlers. These aliases take the operation type
  as their last argument.
