# Paginate a bucket

Keep the initial load and continuation together inside `RemoteSource`:

```swift
let posts = Bucket(IndexedKey<Post.ID, Post>("posts")) {
  RemoteSource {
    Load {
      let page = try await client.posts(after: nil)
      return Page(values: page.posts, next: page.nextCursor)
    }
    NextPage { cursor in
      let page = try await client.posts(after: cursor)
      return Page(values: page.posts, next: page.nextCursor)
    }
  }
}

try await posts.load(using: .remote)
if posts.hasNextPage {
  try await posts.loadNext()
}
```

The initial page replaces the snapshot. Later pages append entries; duplicate
indices keep their original position and take the newest value. Return
`next: nil` when exhausted. Further `loadNext()` calls then return the current
snapshot without fetching.

A cached local snapshot does not establish a cursor. Use a remote-reaching load
before requesting pages. Each partition owns an independent continuation.

## Accumulate a nested collection

For an aggregate response, name the collection to merge:

```swift
RemoteSource {
  Load {
    let page = try await client.subscription(after: nil)
    return Page(value: page.subscription, next: page.nextCursor)
  }
  NextPage(accumulating: \.runs, indexedBy: \.id) { cursor in
    let page = try await client.subscription(after: cursor)
    return Page(value: page.subscription, next: page.nextCursor)
  }
}
```

The incoming aggregate supplies all sibling properties; only `runs` accumulates.
Both key paths must be sendable, and the collection path must be writable.
Stored path variables must preserve `& Sendable` in their declared type.

## Observe and retry

Continuation work exposes `isLoadingNextPage`, `hasNextPage`, `nextPageError`,
and `pagination`. Initial loading still uses `isLoading`, `isLoaded`, and `error`.
A failed page retains its snapshot and cursor; another `loadNext()` retries it.

Calling without pagination throws `missingPaginatedRemoteSource`; calling before
an initial remote page throws `initialPageNotLoaded`. Both appear in
`nextPageError`.

Concurrent calls for one cursor share work. Page loading waits for mutations and
initial loading before capturing its base. A mutation or forced refresh can
supersede it. [Operation ordering →](operation-ordering.md)

If new entries introduce foreign keys, an ordinary
`Input(posts).resolving(\.authorID, from: authors)` resolves them before publishing
the expanded pair. [Relationship resolution →](composition.md#resolve-relationships)

Next: [Compositions](composition.md) · [Persistence](persistence.md)
