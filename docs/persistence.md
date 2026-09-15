# Persist local snapshots

Add `persist:` when a local source can replace its stored snapshot:

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

Both closures work with the complete `Snapshot?`. `nil` means absent or deleted;
`[]` is a successfully loaded empty collection. Continuum owns no storage driver
or schema migration layer.

## Know when writes happen

| Operation | Publication and persistence |
| --- | --- |
| Remote load or next page | Persist the complete normalized snapshot, then publish it. |
| `store` or `remove` | Publish optimistically, persist locally, then run any configured remote mutation. |
| Authoritative `Store` response | Reconcile the snapshot, publish it, then persist it. |
| `reset` | Clear memory and pagination, then persist `nil`. |

Pages persist the full accumulated snapshot. Coalesced loads share persistence
work. Read-only local sources omit `persist:` and receive no writes.

## Use several writable destinations

A bucket accepts multiple `LocalSource` declarations. Reads try them in order
until one returns a snapshot. Writes visit every writable source in declaration
order and stop at the first error.

A failed remote load or page does not publish its candidate; completed writes
are not rolled back. A failed mutation or reset restores its previous memory
snapshot, if it still owns the state, and attempts to restore all writable
sources. Restoration can also fail; the original error is reported.

These destinations do not form a transaction. Shared backing stores must
coordinate writes from other buckets or direct callers themselves.

Local reads and writes are serialized per partition, including across resets.
See [Operation ordering](operation-ordering.md) for cancellation and the rules
for calling back into a bucket from its source closures.

Next: [Remote mutations](remote-mutations.md) · [Invalidation](invalidation.md)
