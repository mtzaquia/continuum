# Invalidate a snapshot

Call `try await bucket.reset()` when a snapshot should be discarded. Reset clears the snapshot, error, freshness, and cursor before suspending, then sends `nil` to writable local sources. It supersedes older reads and edits. A failed write throws, restores previous availability, and attempts to restore persistence. [Operation ordering](operation-ordering.md)

## Observe application events

Add an `InvalidationSignal` to a bucket configuration to reset it on events.

```swift
import Continuum
import Foundation

let posts = Bucket(IndexedKey<Post.ID, Post>("posts")) {
  RemoteSource { try await client.posts() }
  InvalidationSignal {
    NotificationCenter.default.notifications(named: Notification.Name("postsDidChange"))
  }
}
```

The async sequence must be sendable. Each element triggers a reset. A failed reset leaves the subscription available for the next event. Sequence completion ends the subscription. If iteration throws, the bucket records `error`, preserves availability, and ends that subscription. Each bucket subscribes for its lifetime, so a partitioned container starts a subscription per accessed partition.

For a session change that reuses a bucket, stop submitting old-session work and await `reset()` before exposing the next session. Event delivery is asynchronous.

## Understand observable resets

A bucket iterator can coalesce a reset and a replacement into the replacement result. Compositions preserve required Continuum input resets across that coalescing. [Composition outcomes](composition.md#observe-outcomes-and-resets)
