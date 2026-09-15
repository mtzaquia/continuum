# Invalidate snapshots

Use `reset()` when a snapshot is no longer valid:

```swift
try await posts.reset()
```

Reset supersedes source work and mutations, clears memory and pagination, and
sends `nil` to writable local sources. This prevents a later cached load from
restoring stale data. A persistence failure restores the previous state,
attempts to restore storage, and throws. [Persistence rules →](persistence.md)

## Observe application events

An `InvalidationSignal` performs the same reset for every event:

```swift
import Foundation

let posts = Bucket(IndexedKey<Post.ID, Post>("posts")) {
  LocalSource {
    try await database.posts()
  } persist: { snapshot in
    try await database.replacePosts(with: snapshot)
  }
  RemoteSource { try await client.posts() }
  InvalidationSignal {
    NotificationCenter.default.notifications(named: .accountDidChange)
  }
}
```

The event sequence must be sendable; its element values are ignored. A bucket
owns its subscriptions for its lifetime. A partition starts its subscriptions
on first access, so an application-wide signal is observed once per accessed
partition.

A failed reset leaves the subscription running so a later event can retry.
Sequence completion ends that subscription; a sequence error becomes the
bucket's error and ends it.

## Understand observable resets

A bucket iterator emits reset after a result while the bucket remains
unavailable. If replacement data is already available when observation resumes,
it emits that result directly.

Compositions preserve required-input reset history, including through nesting.
Optional resets contribute nil. A resolved root also discards its relationship
values; resetting a selected required partition invalidates the pair without
reloading it. Unrelated inputs keep observing and are not reset or restarted.
[Composition outcomes →](composition.md#observe-outcomes-and-resets)

Next: [Operation ordering](operation-ordering.md) · [Compositions](composition.md)
