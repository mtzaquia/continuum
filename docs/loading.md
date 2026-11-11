# Load snapshots

Choose a policy on one bucket or selected partition. A bucket load returns its
complete snapshot or throws; loading does not discard established values.

```swift
let posts = try await repository.posts.load(using: .cachedThenRemote)
```

| Policy | Behavior |
| --- | --- |
| `.cached` (default) | Return established memory unless `staleAfter` requires remote validation. Otherwise try local sources in order, then remote. Callers requiring source work share it. |
| `.cachedThenRemote` | Expose memory or the first local hit, then always reach remote. Return the remote snapshot. Concurrent callers share work. |
| `.remote` | Skip cache, supersede active work, and start a new remote flight. Repeated calls are latest-wins. |

A local `nil` is a miss; an empty collection is a hit. A local error stops the
load. Both remote-reaching policies require a `RemoteSource`.

## Refresh stale snapshots

Set `staleAfter` to make cached snapshot loads refresh data after a duration:

```swift
let posts: IdentifiedBucket<Post> = Bucket(
  IndexedKey("posts"),
  staleAfter: .seconds(300)
) {
  LocalSource { try await database.posts() }
  RemoteSource { try await client.posts() }
}

try await posts.load()
```

Omitting the parameter, or passing `nil`, preserves unlimited cache reuse.
Durations must be nonnegative; `.zero` requires remote validation on every
cached snapshot load. Local-only and in-memory configurations ignore freshness.
Partitioned buckets accept `staleAfter` after `partitionedBy:`; each selected
partition tracks its own interval.

Freshness starts when a successful full remote snapshot finishes persistence
and enters memory. At or after the duration, the next `.cached` load awaits a
shared remote refresh while existing values remain readable and `isLoaded`
stays true. Failed refreshes preserve those values, report the error, and leave
the snapshot eligible for retry on the next cached load.

Local snapshots have no age metadata. With freshness enabled and a remote
source configured, the first cached load exposes the first local hit, then
validates it remotely before returning. Freshness is tracked only for the
current partition lifetime and is not persisted across launches.

Expiration itself starts no work, clears no data, and evicts no partitions.
Only successful full remote loads restart the interval. Mutations, entry loads,
and subsequent pages do not renew collection freshness. Reset clears freshness;
a failed reset restores the original interval with the previous snapshot.
`.cachedThenRemote` and `.remote` always retain their existing behavior.

## Load one indexed entry

Add `LoadEntry` after `Load` (or `NextPage` for a paginated source):

```swift
let authors: IdentifiedBucket<Author> = Bucket(IndexedKey("authors")) {
  RemoteSource {
    Load { try await client.authors() }
    LoadEntry { id in try await client.author(id: id) }
  }
}

let author = try await authors.load(id: authorID, using: .remote)
```

`.cached` returns an existing entry or fetches it. `.cachedThenRemote` and
`.remote` fetch while keeping existing values readable. `.remote` supersedes
earlier fetches for the same ID; other policies share work. Different IDs fetch
concurrently. Entry loads do not scan local sources.
`staleAfter` applies to complete snapshot loads; entry loads keep these policies
and have no independent freshness interval.

An entry already in the collection is replaced in place and persisted before
publication. An absent entry is returned without insertion or persistence.
Entry loading preserves ordering, pagination, and whole-list `isLoading` and
`isLoaded`. The returned index must match the request. Entry failures throw and
set `error` while preserving the collection outcome. Starting another entry
fetch clears the error. A cached entry hit preserves it.
Reset and superseding collection operations cancel pending entry work.

## Read loading state

- `isLoaded` indicates an established complete snapshot, including an empty one.
- `isLoading` tracks active collection loads, including refreshes.
- `error` and `state.error` expose the latest collection load, entry load,
  mutation, persistence, or invalidation error. Read `nextPageError` for a
  failed continuation.
- `latest` and iteration report snapshot availability. An established snapshot
  is `.result(.success(snapshot))`. A collection load failure without a
  snapshot is `.result(.failure(error))`. Reset clears availability.

A failed refresh throws to its caller and leaves the established snapshot
available to existing and new observers. If a mutation or reset rolls back,
observers receive the restored snapshot when one existed. A cached memory hit
preserves the current error. Starting source work or a mutation clears it.
Successful remote loads finish local persistence before publishing.
[Persistence details →](persistence.md)

A bucket with no sources is for in-memory mutations. Calling `load()` warns
and throws `missingRemoteSource` if no snapshot exists.

## Coordinate concurrent work

Cached loading without fresh memory waits for pending mutations and resets
before checking again. `.cachedThenRemote` also waits; `.remote` supersedes
them. A cached call with fresh memory returns immediately, even during a refresh.

If `.cachedThenRemote` joins a cached flight that resolves locally, the shared
flight continues to remote. Callers still awaiting it receive the remote result.
An awaiting caller's cancellation does not cancel a shared bucket flight.
[Complete ordering and cancellation rules →](operation-ordering.md)

## Load through a composition

```swift
let feed = Composition {
  Input(repository.posts) { policy in
    try await repository.posts.load(using: policy)
  }
} transform: { $0 }

await feed.load(using: .remote)
```

Composition forwards policies unchanged to every configured action. Independent
inputs load concurrently; a resolved input loads its root before its related
keys. `load` returns `Void`. An available bucket snapshot continues to contribute
its value after a failed refresh. Read the bucket's `error` to present that
failure. [Composition loading →](composition.md#load-the-composition)

Next: [Persistence](persistence.md) · [Compositions](composition.md)
