# Changelog

## 2.0.0-beta.9

- `Bucket<Space, Operation>` owns one snapshot. `PartitionedBucket` selects the same bucket type by key. `ValueBucket` replaces `DataBucket`, and indexed collections have `IdentifiedBucket` aliases for `Identifiable` models.
- Configure `RemoteSource` with an unlabeled snapshot or first-page closure and optional `nextPage:` and `loadEntry:` capabilities.
- `Mutate` and `mutate(_:)` replace `Store` and `Remove`. Edits can publish and persist optimistically, then apply an endpoint's acknowledgment or authoritative value.
- Failed collection refreshes retain usable snapshots. Collection retries keep the previous error visible until they settle. Reset clears the snapshot and error and invalidates older work.
- `Composition` combines observable inputs, forwards load policies to distinct Continuum sources, and exposes aggregate failures through `error` and `mapFailures:`. Apps and libraries supply observation-only inputs through the public `UpdateSource` protocol, whose `error` defaults to `nil`.
- `bucket.resolving(_:from:)` produces a reusable source. It reuses matching entries and fetches distinct missing IDs concurrently. The root bucket owns pagination.
- Iterate sources directly and call `try await reset()` on buckets. Deprecated iteration helpers, type aliases, and reset overloads were removed.

[Migrate to 2.0](docs/migrating-to-2.md) · [Published releases](https://github.com/mtzaquia/continuum/releases)
