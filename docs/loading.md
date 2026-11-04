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
let posts = Bucket(
  IndexedKey<Post.ID, Post>("posts"),
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
let authors = Bucket(IndexedKey<Author.ID, Author>("authors")) {
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
Ordering, pagination, and whole-list loading state remain unchanged. The returned
index must match the request; errors throw without failing or changing the list.
Reset and superseding collection operations cancel pending entry work.

## Read loading state

- `isLoaded`: a complete snapshot has been established, including an empty one.
- `isLoading`: source work is active, possibly alongside established data.
- `error`: the latest load, mutation, persistence, or invalidation failure.
- `latest`: the observable `Update<Snapshot>` result or reset.

A failed refresh preserves the established snapshot in the value accessors,
but `latest` reports the failure. A cached memory hit preserves that error;
starting source work clears it. Successful remote loads finish local
persistence before publishing. [Persistence details →](persistence.md)

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
keys. `load` returns `Void`, with required failures delivered through `latest`
and iteration. [Composition loading →](composition.md#load-the-composition)

Next: [Persistence](persistence.md) · [Compositions](composition.md)
