# Changelog

## 2.0.0-beta.11

- `Bucket<Space, Operation>` owns one snapshot. `PartitionedBucket` selects the same bucket type by key. `ValueBucket` replaces `DataBucket`, and indexed collections have `IdentifiedBucket` aliases for `Identifiable` models.
- Configure `RemoteSource` with a trailing snapshot or first-page closure followed by `loadEntry:` and `nextPage:` closures as needed. For nested collection pagination, specify `accumulating:` and `indexedBy:` on `RemoteSource`.
- `Mutate` and `mutate(_:)` replace `Store` and `Remove`. Edits can publish and persist optimistically, then apply an endpoint's acknowledgment or authoritative value.
- Failed collection refreshes retain usable snapshots. Collection retries keep the previous error visible until they settle. Reset clears the snapshot and error and invalidates older work.
- `Composition` combines observable inputs, forwards load policies once per source, and exposes aggregate failures through `error` and `mapFailures:`. External `UpdateSource` conformers can implement `load(using:)` to participate in composition loads. The default is a no-op.
- Declare relationships with `Input(root).resolving(_:from:)`. The transform receives one `ResolvedSnapshot`. The input reuses matching entries and fetches distinct missing IDs concurrently. Buckets own entry loading and pagination, and the root bucket applies its own freshness policy.
- Iterate sources directly and call `try await reset()` on buckets. Deprecated iteration helpers, type aliases, and reset overloads were removed.

[Migrate to 2.0](docs/migrating-to-2.md) · [Published releases](https://github.com/mtzaquia/continuum/releases)
