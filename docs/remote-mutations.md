# Mutate remote values

A `RemoteSource` can own the operations that create, update, and delete its
values. Repositories keep their transport dependencies inside the source DSL,
while consumers continue to use the same typed bucket operations.

## Add mutation capabilities

Use `Load` when a remote source also declares `Store` or `Remove`:

```swift
let posts = Bucket(PostsData.all) {
  LocalSource {
    try await database.posts()
  } persist: { posts in
    try await database.replacePosts(with: posts)
  }

  RemoteSource {
    Load {
      try await client.posts()
    }

    Store { post in
      try await client.store(post)
    }

    Remove { id in
      try await client.removePost(id: id)
    }
  }
}
```

The builder requires one `Load`, followed by at most one `Store` and at most
one `Remove` in that order. Omitting either mutation keeps the corresponding
bucket operation local-only. A read-only source can retain the shorter
`RemoteSource { snapshot }` form.

Repository consumers do not call source capabilities directly:

```swift
try await posts.store(draft)
try await posts.remove(postID)
```

The same shape works in a partition configuration because each operation can
capture the stable partition value supplied by `Bucket`:

```swift
let accounts = Bucket(
  AccountsData.all,
  partitionedBy: Purpose.self
) { purpose in
  RemoteSource {
    Load {
      try await client.accounts(purpose: purpose)
    }
    Store { account in
      try await client.store(account, purpose: purpose)
    }
    Remove { id in
      try await client.removeAccount(id: id, purpose: purpose)
    }
  }
}
```

## Publish the authoritative value

`Store` returns the value established by the remote system. This lets the
server assign an identifier, normalize fields, or attach version metadata
after the submitted value has entered optimistic local persistence and
observable memory:

```swift
Store { draft in
  let response = try await client.createPost(draft)
  return response.post
}
```

When the authoritative indexed value has a different identifier, Continuum
removes the submitted identifier before inserting the returned value. The
snapshot therefore does not retain both the draft and its server-created form.

If the remote system only acknowledges the request, use the `Void`-returning
overload. Continuum then retains the submitted value:

```swift
Store { post in
  try await client.put(post)
}
```

## Understand ordering and failure

An explicit remote mutation runs in this order:

1. Derive and normalize the submitted complete snapshot.
2. Publish it to observable memory and persist it through writable local
   sources in declaration order.
3. Invoke `Store` or `Remove`.
4. For `Store`, reconcile any server-authoritative value, then publish and
   persist the reconciled snapshot.

A failure restores the prior snapshot in observable memory and attempts to
write that snapshot back through writable local sources in declaration order.
Continuum exposes the original failure through both the throwing bucket call
and `error`. A remote system can still have applied a request that fails
ambiguously, so this is UI and local-state rollback rather than a distributed
transaction.

Concurrent explicit mutations are serialized across the complete pipeline.
`reset()` does not invoke remote mutation capabilities.

## Combine mutations with pagination

For a paginated source, declare mutation capabilities after `NextPage`:

```swift
RemoteSource {
  Load {
    let response = try await client.posts(after: nil)
    return Page(
      values: response.posts,
      next: response.nextCursor
    )
  }
  NextPage { cursor in
    let response = try await client.posts(after: cursor)
    return Page(
      values: response.posts,
      next: response.nextCursor
    )
  }
  Store { post in
    try await client.store(post)
  }
  Remove { id in
    try await client.removePost(id: id)
  }
}
```

Storing or removing a value updates the complete accumulated snapshot and
preserves the current continuation checkpoint. Local persistence receives that
complete snapshot, including all pages loaded so far.

For a singleton key, the zero-argument remote removal can be made explicit with
`Remove<SingletonInput>`:

```swift
RemoteSource {
  Load<Int> {
    try await client.count()
  }
  Store<Int> { count in
    try await client.setCount(count)
  }
  Remove<SingletonInput> {
    try await client.removeCount()
  }
}
```

Consumers still call `try await count.remove()` without supplying an input.

Next: [Persisting local snapshots](persistence.md) ·
[Paginating buckets](pagination.md) · [Partitioning buckets](partitioning.md)
