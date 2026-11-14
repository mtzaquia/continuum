# Load snapshots

Call `try await bucket.load(using:)` on a bucket or selected partition. The call returns the complete snapshot. Read `latest` to observe intermediate values as they become available.

| Policy | Snapshot loading |
| --- | --- |
| `.cached` (default) | Return fresh memory, otherwise try local sources in order and fall through to remote on a miss. Configured freshness can require remote validation. |
| `.cachedThenRemote` | Expose memory or the first local hit while fetching remotely. A successful call returns the remote snapshot. |
| `.remote` | Wait for pending mutations and resets, then skip cache and replace active reads. |

A local source returns `nil` on a miss. An empty collection is a hit. A local read error throws and stops the load. If a load needs remote data and no `RemoteSource` is configured, it throws `ContinuumError.missingRemoteSource`.

## Refresh stale snapshots

Pass `staleAfter: .seconds(300)` to `Bucket` or `PartitionedBucket`. Freshness starts when a successful full remote snapshot enters memory, after persistence. The first cached load can publish a local hit before validating it remotely. Once stale, the next cached load shares a refresh while keeping existing data readable.

- `nil` (default) allows indefinite cache reuse. `.zero` requires validation on every cached load. Durations must be nonnegative. Local-only buckets ignore this setting.
- Each partition tracks freshness in memory. Only a successful full remote load renews it. Reset clears it, and a failed reset restores the previous interval.
- A failed refresh leaves stale data eligible for the next retry. Expiration takes effect on the next load.

## Load one indexed entry

Configure `loadEntry:` beside the complete snapshot loader.

```swift
let authors: IdentifiedBucket<Author> = Bucket(IndexedKey("authors")) {
  RemoteSource(
    { try await client.authors() },
    loadEntry: { id in try await client.author(id: id) }
  )
}

let author = try await authors.load(id: authorID)
```

`.cached` accepts an existing entry. On a miss, it uses `loadEntry` directly. `.cachedThenRemote` and `.remote` fetch remotely. Different IDs can load concurrently. Fetches for the same ID share work until a `.remote` request replaces them.

A fetched entry must match its requested ID. If it is already in the collection, Continuum updates it in place and persists the complete list. An absent entry is returned to the caller only. An entry failure throws and sets `error` while preserving `latest`.

## Read loading state

| Property | Use |
| --- | --- |
| `latest` | Render a success, handle a failure when no snapshot is available, or clear on `.reset`. |
| `isLoaded` | Tell an established snapshot, including an empty array, from an unavailable one. |
| `isLoading` | Track full snapshot loading, including refreshes. |
| `error` or `state.error` | Observe collection, entry, mutation, persistence, or invalidation errors. |
| `pagination` | Track continuation progress and page errors. |

A failed refresh throws and keeps any established snapshot as a success in `latest`. During a collection retry, the prior error stays visible until the load settles, even if an intermediate cache hit publishes a snapshot. A cached memory hit that starts no load preserves the error. Use `.remote` to explicitly refresh established data. [Pagination](pagination.md) · [Operation ordering](operation-ordering.md)

To load several sources together, use [`Composition.load(using:)`](composition.md#load-the-composition).
