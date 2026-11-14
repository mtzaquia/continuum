# Mutate a bucket snapshot

Declare a `Mutate` handler beside the bucket's sources. Its first parameter is the operation accepted by `mutate(_:)`. The handler runs at the front of that bucket's mutation queue.

```swift
enum PostEdit: Sendable {
  case rename(Post.ID, title: String)
}

let posts = Bucket(IndexedKey<Post.ID, Post>("posts")) {
  RemoteSource { try await client.posts() }
  Mutate { (edit: PostEdit, mutation) in
    switch edit {
    case let .rename(id, title):
      try await mutation.update(id: id) { post in
        post.renamed(to: title)
      } remote: {
        .updated(try await client.renamePost(id: id, title: title))
      }
    }
  }
}

try await posts.load()
try await posts.mutate(.rename(postID, title: "Draft"))
```

`update(id:)` edits an indexed entry. `update` edits the complete snapshot. The bucket publishes the edit, persists it to writable local sources, then runs `remote:`. Return `.acknowledged` to keep the edit or `.updated(value)` to publish and persist the server's entry or snapshot. Omit `remote:` for a local edit. A server entry can change the edited entry's ID.

An edit requires an established snapshot, including a loaded empty collection. An entry edit also requires that ID to be present. On a miss, the paired endpoint can still run, while publication and persistence for that edit are skipped. A handler can also call a remote endpoint directly without editing the snapshot.

## Annotate a typed bucket

| Snapshot | Standalone | Partitioned |
| --- | --- | --- |
| Single value | `MutableValueBucket<Value, Operation>` | `PartitionedMutableValueBucket<Partition, Value, Operation>` |
| Indexed by `Identifiable.id` | `MutableIdentifiedBucket<Value, Operation>` | `PartitionedMutableIdentifiedBucket<Partition, Value, Operation>` |
| Custom index | `MutableIndexedBucket<ID, Value, Operation>` | `PartitionedMutableIndexedBucket<Partition, ID, Value, Operation>` |

For a custom key space, spell the types `Bucket<Space, Operation>` and `PartitionedBucket<Partition, Space, Operation>`.

## Supply a mutation closure at the call site

A bucket without a typed handler accepts a closure through the same method.

```swift
try await favorites.mutate { mutation in
  try await mutation.update { posts in
    posts.filter { $0.id != postID }
  }
}
```

Return `nil` from the snapshot edit to clear availability. Return `[]` to keep an established empty collection. If the handler throws or is cancelled, Continuum rolls back the snapshot and attempts to restore persistence while the handler still owns it. A server side effect may outlive that rollback. Use the mutation handle only during its invocation and await edits sequentially. [Operation ordering](operation-ordering.md) · [Persistence](persistence.md)
