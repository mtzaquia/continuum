# Mutate remote values

Declare `Load`, followed by optional `Store` and `Remove` capabilities inside
`RemoteSource`. Consumers keep using the bucket's ordinary mutation methods.

```swift
let posts = Bucket(IndexedKey<Post.ID, Post>("posts")) {
  RemoteSource {
    Load { try await client.posts() }
    Store { post in try await client.store(post) }
    Remove { id in try await client.removePost(id: id) }
  }
}

try await posts.store(draft)
try await posts.remove(postID)
```

Each capability appears at most once, in that order. Omitting one keeps the
corresponding mutation local-only. A read-only source can use
`RemoteSource { try await client.posts() }`.

## Reconcile server-assigned values

`Store` can return the authoritative model, including a server-assigned ID:

```swift
Store { draft in
  try await client.createPost(draft)
}
```

Continuum replaces the submitted indexed identity when the returned identity
differs. If the server only acknowledges the write, the `Void`-returning overload
retains the submitted model:

```swift
Store { post in
  try await client.put(post)
}
```

For a singleton bucket, consumers call `remove()` without an argument. Its
remote capability can be written `Remove<SingletonInput> { ... }`.

## Understand failure and ordering

Mutations publish optimistically and persist locally before contacting the
server. Reconciliation publishes and persists the authoritative snapshot.
Failure restores the previous snapshot if the mutation still owns the state;
remote side effects cannot be undone by local rollback.

Concurrent mutations are serialized. Reset and `.remote` loads supersede them;
`.cachedThenRemote` and `loadNext()` wait. Cancellation restores state only while
the mutation still owns it. See [Operation ordering](operation-ordering.md) and
[Persistence](persistence.md) for the complete contracts.

In a paginated source, place `Store` and `Remove` after `NextPage`. Mutations
update the accumulated snapshot while preserving its cursor.

Next: [Pagination](pagination.md) · [Persistence](persistence.md)
