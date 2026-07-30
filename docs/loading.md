# Load snapshots

Continuum offers three loading policies for choosing between established
memory, local sources, and the remote source. The selected policy also defines
whether active source work is shared or superseded.

## Use cached loading by default

`load()` uses `LoadPolicy.cached`:

```swift
let posts = try await repository.posts.load()
```

An established memory snapshot returns immediately, even when a refresh is
active. This keeps already published values inexpensive to request while
`isLoading` continues to describe the refresh.

Without memory, a cached load joins active source work. When no work exists, it
tries `LocalSource` expressions in declaration order and returns the first
snapshot. If every local source returns `nil`, it falls through to
`RemoteSource`.

Concurrent cached callers without memory receive the same result from one
source flight.

An in-memory bucket created with `Bucket(keySpace)` has no loading sources.
Calling `load()` on it emits a runtime warning and, when no snapshot has been
established by an in-memory mutation, throws
`ContinuumError.missingRemoteSource(namespace:)`. Use `store`, `remove`, and
`reset` for in-memory state instead.

## Publish cache before remote

Use `.cachedThenRemote` when the repository should expose cached data as soon as
possible but the operation should not complete until remote data arrives:

```swift
let remotePosts = try await repository.posts.load(
  using: .cachedThenRemote
)
```

The policy proceeds in two observable phases:

1. It keeps established memory, or publishes the first successful local
   snapshot when memory is absent.
2. It always loads and publishes the remote snapshot.

`isLoading` remains `true` across both phases. Publishing a local snapshot makes
`isLoaded` true immediately, including when that snapshot is empty. The return
value is always the final remote snapshot.

This policy requires a `RemoteSource`. Without one, it throws
`ContinuumError.missingRemoteSource(namespace:)` before reading local sources or
changing established memory.

If the remote phase fails, the cached snapshot remains established,
`isLoading` becomes false, `error` exposes the remote failure, and `load` throws.
When a local source declares `persist:`, the remote phase also waits for that
write before publishing the remote snapshot. Persistence failures follow the
same state behavior.

Concurrent `.cachedThenRemote` callers share the active remote-reaching flight.
If the policy encounters an ordinary cached flight, it first follows that work
and then upgrades the shared load to remote when the result came from a local
source. Callers still awaiting that shared load resolve with the remote
snapshot.

## Supersede with remote loading

Use `.remote` when the current source request is obsolete:

```swift
let refreshedPosts = try await repository.posts.load(using: .remote)
```

A remote load requires `RemoteSource`, cancels active source work, skips memory
and local sources, and immediately starts a new remote flight. Repeated remote
calls are latest-wins rather than coalesced.

For a paginated remote source, the new initial page replaces all previously
merged pages and establishes a new continuation checkpoint.

Cancellation is cooperative, so Continuum also assigns every flight a
generation. Results and errors from an obsolete generation cannot replace the
current snapshot or loading state. Callers awaiting superseded work follow the
replacement flight when it remains current.

Previously established values stay readable while the remote load runs. An
ordinary cached call may return those memory values immediately; it does not
cancel or duplicate the refresh.

## Apply policies per partition

Loading behavior belongs to one unpartitioned bucket or selected partition:

```swift
async let buying = repository.accounts[.buy].load(
  using: .cachedThenRemote
)
async let selling = repository.accounts[.sell].load(
  using: .cachedThenRemote
)
```

Forcing `.buy` supersedes only `.buy`. It does not cancel or change `.sell`.
Different partitions can continue loading concurrently.

Next: [Persisting local snapshots](persistence.md) ·
[Paginating buckets](pagination.md) ·
[Partitioning buckets](partitioning.md) ·
[Repository composition](repository-composition.md)
