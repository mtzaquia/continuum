# Changelog

## Unreleased (target 2.0.0)

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

This changes observable behavior even though existing `Update` switches still
compile. See the [2.0.0 migration guide](docs/migrating-to-2.md).

### Bucket type aliases

- Renamed `DataBucket` to `ValueBucket` and `PartitionedDataBucket` to
  `PartitionedValueBucket`. Update type annotations that use the old names.
- Added `IdentifiedBucket<Value>` and `PartitionedIdentifiedBucket<Partition, Value>`
  for collections indexed by the value's `Identifiable.ID`. Use `IndexedBucket`
  and `PartitionedIndexedBucket` to specify the index type explicitly.
