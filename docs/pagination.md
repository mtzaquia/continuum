# Paginate a bucket

Configure the first request and its continuation in `RemoteSource`. Call `loadNext()` on the bucket that owns the cursor.

```swift
let posts = Bucket(IndexedKey<Post.ID, Post>("posts")) {
  RemoteSource(
    {
      let page = try await client.posts(after: nil)
      return Page(values: page.posts, next: page.nextCursor)
    },
    nextPage: NextPage { cursor in
      let page = try await client.posts(after: cursor)
      return Page(values: page.posts, next: page.nextCursor)
    }
  )
}

try await posts.load(using: .remote)
if posts.hasNextPage {
  try await posts.loadNext()
}
```

A successful remote first page replaces the snapshot and establishes the cursor. Later array pages append entries. In an indexed bucket, duplicate IDs keep their original position and take the latest value. Return `next: nil` to mark exhaustion. Calls after exhaustion return the current snapshot.

A cached local snapshot has no remote cursor. If pagination is configured but no remote first page is available, `loadNext()` throws `initialPageNotLoaded`. A bucket without pagination throws `missingPaginatedRemoteSource`.

## Accumulate a nested collection

For a snapshot that holds a collection, return `Page(value:next:)` and specify the collection to merge. The incoming page supplies the other fields. Both key paths must be sendable, and the collection path must be writable.

```swift
let workflow = Bucket(Key<Workflow>("workflow")) {
  RemoteSource(
    {
      let page = try await client.workflow(after: nil)
      return Page(value: page.workflow, next: page.nextCursor)
    },
    nextPage: NextPage(accumulating: \.runs, indexedBy: \.id) { cursor in
      let page = try await client.workflow(after: cursor)
      return Page(value: page.workflow, next: page.nextCursor)
    }
  )
}
```

## Observe and retry

Use `pagination.isLoading`, `pagination.hasNextPage`, and `pagination.error` for continuation state, or their bucket shortcuts `isLoadingNextPage`, `hasNextPage`, and `nextPageError`. A failed page keeps the current snapshot and cursor. Call `loadNext()` again to retry.

A composition observes the merged snapshot. When using a [resolved bucket](composition.md#resolve-related-entries), related lookups can continue after `loadNext()` returns. [Operation ordering](operation-ordering.md)
