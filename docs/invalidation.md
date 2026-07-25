# Invalidate snapshots

Invalidation returns a bucket to absent state when its established data is no
longer valid. Continuum supports an explicit local-inclusive reset and
application event streams that trigger the same operation automatically.

## Choose the reset depth

Call `reset()` when only observable memory should be forgotten:

```swift
posts.reset()
```

The next cached load can restore the local snapshot. No reset persistence
operation runs. Active source and mutation tasks are cancelled; generation and
epoch checks prevent their eventual results from entering local persistence or
memory even if an underlying operation ignores cooperative cancellation.

Include writable local sources when their snapshots must also be removed:

```swift
try await posts.reset(including: .localSources)
```

Continuum resets memory and pagination state immediately, then sends `nil` to
each writable `LocalSource` sequentially in declaration order. It stops at the
first error. A failure restores the previous observable snapshot and pagination
checkpoint, attempts to restore local persistence, appears as `error`, and
causes the call to throw.

## Observe application events

Add an `InvalidationSignal` to reset automatically for each element of a
sendable `AsyncSequence`:

```swift
import Foundation

let posts = Bucket(PostsData.all) {
  LocalSource {
    try await database.posts()
  } persist: { posts in
    try await database.replacePosts(with: posts)
  }

  RemoteSource {
    try await client.posts()
  }

  InvalidationSignal {
    NotificationCenter.default.notifications(
      named: .accountDidChange
    )
  }
}
```

The event values are intentionally ignored. Their arrival means the bucket's
snapshot is no longer valid. Any number of invalidation signals can be declared.

An unpartitioned bucket starts each observation when it is created and cancels
it when released. A sequence that finishes stops observing normally. A sequence
failure becomes the bucket's observable `error` and ends only that observation.

If an event-triggered reset fails, Continuum restores the previous observable
state and `error` exposes the persistence failure. The event sequence stays
active, so a later event retries the reset.

## Invalidate partitions independently

A partitioned bucket creates a partition's configuration on the first access
through the outer subscript. Each such partition owns its own invalidation
observations and resets only its captured local destinations:

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

  InvalidationSignal {
    accountChanges.values(for: purpose)
  }
}
```

An application-wide event sequence declared for every partition is observed
once per partition that has been accessed.

Next: [Persisting local snapshots](persistence.md) ·
[Loading snapshots](loading.md) ·
[Partitioning buckets](partitioning.md)
