# Migrate to Continuum 2.0.0

## Rename singleton bucket aliases

Replace `DataBucket<Value>` with `ValueBucket<Value>` and
`PartitionedDataBucket<Partition, Value>` with
`PartitionedValueBucket<Partition, Value>` in type annotations. Both names
still describe a bucket built with `Key<Value>`.

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
can still run as other inputs change. Observe the original bucket for refresh
errors. `mapFailures:` still handles required input failures that reach the
composition, including failed relationship lookups. A throwing transform still
fails the output.

## Update Ensemble error presentation

Keep forwarding `.result` to the sink and `.reset` to `sink.reset()`. A failed
refresh that retains data now reaches `ViewData` as success, so failure-phase
handlers will only see actual failure outcomes. Observe the source bucket's
`error` separately for a banner or other refresh feedback. See the
[Ensemble binding example](composition.md#bind-to-ensemble).
