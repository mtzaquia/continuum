# Persist local snapshots

A `LocalSource` can describe both sides of one local backing store: loading its
complete optional snapshot and replacing that state after remote work or an
explicit bucket mutation. Read-only sources keep the original single-closure
form; persistence is progressively disclosed with a labeled trailing closure.

## Make a local source writable

Attach `persist:` to the source backed by the database or cache:

```swift
let posts = Bucket(PostsData.all) {
  LocalSource {
    try await database.posts()
  } persist: { posts in
    try await database.replacePosts(with: posts)
  }

  RemoteSource {
    try await client.posts()
  }
}
```

The read and persistence operations use the same `Snapshot?` model. For an
indexed key:

- `nil` means that no snapshot is persisted.
- `[]` means that an empty snapshot is persisted successfully.
- A nonempty array is the complete normalized, insertion-ordered snapshot.

For a singleton key, removing its value passes `nil` to `persist:`.

## Persist atomic results

Persistence follows the bucket's atomic snapshot model. It receives the
resulting state rather than the original command:

```swift
try await posts.store(post)
try await posts.remove(post.id)
```

For store and remove, the key first derives and normalizes the resulting
complete snapshot, then immediately publishes it to observable memory and
persists it. A failure restores the previously established snapshot to memory
and attempts to restore writable local sources.

Successful remote loads also write through before publication. A paginated
remote source persists the complete merged snapshot after each page, not the
individual page response. Coalesced callers share that persistence work along
with the source request.

When `RemoteSource` declares `Store` or `Remove`, that remote operation runs
after the optimistic local write-through pipeline. An authoritative value
returned by `Store` reconciles the complete snapshot and writes it through to
every local destination. A remote failure restores the prior snapshot. See
[Mutating remote values](remote-mutations.md) for ordering and failure
behavior.

`reset()` forgets observable memory and pagination state and deletes the
snapshots held by every writable local source:

```swift
try await posts.reset()
```

Memory and pagination reset immediately, then every writable destination
receives `nil` in declaration order. A failure restores the previous observable
state and attempts to restore local persistence. An indexed source returning
`[]` establishes a known empty snapshot; a successful remote result writes it
through, while `nil` remains absent.

`InvalidationSignal` uses this same reset behavior for its events. It is
appropriate when a cached snapshot is known stale and must not be restored by a
later `.cached` load.

## Use several writable destinations

Any number of local sources can declare `persist:`:

```swift
let posts = Bucket(PostsData.all) {
  LocalSource {
    try await fastCache.posts()
  } persist: { posts in
    try await fastCache.replacePosts(with: posts)
  }

  LocalSource {
    try await database.posts()
  } persist: { posts in
    try await database.replacePosts(with: posts)
  }

  RemoteSource {
    try await client.posts()
  }
}
```

Continuum invokes writable destinations sequentially in declaration order. It
stops at the first error and does not publish the candidate snapshot to memory.
Destinations that completed earlier are not rolled back, so several independent
stores do not form a cross-store transaction.

Concurrent explicit mutations are serialized. Each mutation derives its
candidate from the last successfully published snapshot rather than racing
another suspended persistence operation.

## Capture partition context

A partitioned bucket supplies its stable partition value when the local source
is created. Both closures can capture it without repeating a parameter at each
bucket operation:

```swift
let accounts = Bucket(
  AccountsData.all,
  partitionedBy: Purpose.self
) { purpose in
  LocalSource {
    try await database.accounts(purpose: purpose)
  } persist: { accounts in
    try await database.replaceAccounts(
      accounts,
      purpose: purpose
    )
  }
}
```

Each selected partition owns independent observable memory and persistence
coordination.

Next: [Invalidating snapshots](invalidation.md) ·
[Loading snapshots](loading.md) ·
[Mutating remote values](remote-mutations.md) ·
[Paginating buckets](pagination.md) ·
[Partitioning buckets](partitioning.md)
