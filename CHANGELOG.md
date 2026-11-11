# Changelog

## Unreleased (target 2.0.0)

- One `Mutate` handler and `mutate(_:)` replace `Store`, `Remove`, and their
  bucket methods. Snapshot edits publish and persist optimistically. Returned
  server entries and snapshots take precedence.
- Indexed entry sources use `Load { id in … }` alongside the snapshot loader.

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
- `Input(update:load:)` bridges observable repository state that reports its
  own availability and failures. Observed resets propagate through nested
  compositions.
- `Input(updates:)` starts its sequence subscription on first use of the
  composition. A first use that loads with `.restart` creates one subscription.
- `Composition.load(using:)` loads bucket and selected partition inputs with
  the supplied policy, including optional inputs and relationship roots. Pass
  `Input(bucket) { _ in }` to observe a bucket without loading it.
- Nested composition inputs forward the parent's load policy by default. Each
  shared child loads once per top-level call. Use `Input(child) { _ in }` to
  observe the child without loading it through that input.
- Retired the deprecated update aliases, `bucketUpdates`, `updates()`, and the
  legacy reset overloads and `ResetScope`. Iterate sources directly, use
  `Composition` to combine inputs, and call `try await reset()` to reset.
- Configured buckets default `staleAfter` to `nil`. Source inputs accept an
  optional `load` action through one initializer.

This changes observable behavior even though existing `Update` switches still
compile. See the [2.0.0 migration guide](docs/migrating-to-2.md).

### Bucket type aliases

- Renamed `DataBucket` to `ValueBucket` and `PartitionedDataBucket` to
  `PartitionedValueBucket`. Update type annotations that use the old names.
- Added `IdentifiedBucket<Value>` and `PartitionedIdentifiedBucket<Partition, Value>`
  for collections indexed by the value's `Identifiable.ID`. Use `IndexedBucket`
  and `PartitionedIndexedBucket` to specify the index type explicitly.
- Added `MutableBucket` and the `Mutable` variants of the existing bucket
  aliases for typed `Mutate` handlers. These aliases take the operation type
  as their last argument.
