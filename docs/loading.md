# Load snapshots

Choose a policy on one bucket or selected partition. A bucket load returns its
complete snapshot or throws; loading does not discard established values.

```swift
let posts = try await repository.posts.load(using: .cachedThenRemote)
```

| Policy | Behavior |
| --- | --- |
| `.cached` (default) | Return established memory. Otherwise try local sources in order, then remote. Concurrent callers without memory share work. |
| `.cachedThenRemote` | Expose memory or the first local hit, then always reach remote. Return the remote snapshot. Concurrent callers share work. |
| `.remote` | Skip cache, supersede active work, and start a new remote flight. Repeated calls are latest-wins. |

A local `nil` is a miss; an empty collection is a hit. A local error stops the
load. Both remote-reaching policies require a `RemoteSource`.

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

Cached loading without memory waits for pending mutations and resets before
checking again. `.cachedThenRemote` also waits; `.remote` supersedes them.
A cached call with memory returns immediately, even during a refresh.

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
