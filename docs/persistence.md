# Persist snapshots locally

A `LocalSource` can read an entire snapshot and optionally replace it. Continuum runs the closures you supply, so storage format, location, and schema belong to the app.

```swift
let posts = Bucket(IndexedKey<Post.ID, Post>("posts")) {
  LocalSource {
    try await database.posts()
  } persist: { snapshot in
    try await database.replacePosts(with: snapshot)
  }
  RemoteSource { try await client.posts() }
}
```

The read returns `nil` on a cache miss. The write receives `nil` to delete the stored snapshot. An empty array represents a present collection. Omit `persist:` for a read-only source.

## Know when writes happen

| Operation | Order |
| --- | --- |
| Remote load or next page | Persist the complete normalized snapshot, then publish it. |
| Entry load for an existing member | Persist the complete list with the replaced entry, then publish it. |
| Optimistic edit | Publish, persist, then run the paired endpoint. |
| Authoritative mutation response | Publish the reconciled snapshot, then persist it. |
| `reset()` | Clear memory and pagination, then persist `nil`. |

## Use several writable destinations

Reads try local sources in declaration order until a hit. Writes visit each writable source in declaration order and stop at the first error. If a load or page write fails, the candidate stays out of memory and earlier destinations may already have committed it. If a mutation or reset fails while it still owns the snapshot, Continuum restores memory and attempts to restore each writable source, reporting the original error.

Local reads and writes are serialized per bucket across suspension points. Each backing store must coordinate writes from different buckets or other callers. [Operation ordering](operation-ordering.md)
