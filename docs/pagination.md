# Paginate a bucket

Pagination extends an atomic snapshot without turning its entries into
independently loaded resources. The remote source owns the typed cursor
operations and merge behavior; repository consumers load the first snapshot
normally and request later pages through the bucket.

## Declare both page operations

Keep `Load` and `NextPage` together inside `RemoteSource`:

```swift
let accounts = Bucket(
  AccountsData.all,
  partitionedBy: Purpose.self
) { purpose in
  RemoteSource {
    Load {
      let response = try await client.accounts(
        purpose: purpose,
        after: nil
      )

      return Page(
        values: response.accounts,
        next: response.nextCursor
      )
    }

    NextPage { cursor in
      let response = try await client.accounts(
        purpose: purpose,
        after: cursor
      )

      return Page(
        values: response.accounts,
        next: response.nextCursor
      )
    }
  }
}
```

The builder requires exactly one `Load` followed by one `NextPage`.
`Load` establishes the cursor type from its `Page` result. `NextPage`
receives that cursor without receiving the key or stable partition value again.

Return `next: nil` when the remote collection is exhausted. Cursor values stay
inside these operations; the selected partition stores a continuation
checkpoint without exposing transport pagination types to consumers.

The ordinary `RemoteSource { snapshot }` form remains unchanged for
non-paginated buckets.

Optional `Store` and `Remove` capabilities follow `NextPage`. Explicit
mutations update the complete accumulated snapshot without discarding the
current cursor:

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

## Accumulate a nested collection

When the remote response contains a larger aggregate, return that complete
value from each `Page` and declare the one nested collection that accumulates
on `NextPage`:

```swift
let subscription = Bucket(SubscriptionsData.current) {
  RemoteSource {
    Load {
      let response = try await client.subscription(after: nil)

      return Page(
        value: response.subscription,
        next: response.nextCursor
      )
    }

    NextPage(
      accumulating: \.runs,
      indexedBy: \.id
    ) { cursor in
      let response = try await client.subscription(after: cursor)

      return Page(
        value: response.subscription,
        next: response.nextCursor
      )
    }
  }
}
```

The initial page replaces the complete `Subscription`. For each continuation,
the incoming `Subscription` becomes the new base, so properties beside `runs`
use their latest remote values. Only `runs` accumulates. Existing run indices
retain their position, duplicate indices use the latest value, and new indices
append in remote page order.

The accumulation key path must be writable because Continuum constructs the
merged aggregate before publishing it.

## Load and merge pages

Use `load()` for the initial remote page, then `loadNext()` while another page is
available:

```swift
let buying = repository.accounts[.buy]

try await buying.load(using: .cachedThenRemote)

if buying.hasNextPage {
  try await buying.loadNext()
}
```

The initial remote page is a complete snapshot replacement. For an indexed
snapshot, each continuation page is appended in page order. When a page repeats
an existing index, its new value replaces the old value without moving that
index. Nested accumulation follows the merge behavior declared by `NextPage`.

When the latest page returns no cursor, `hasNextPage` becomes false and another
`loadNext()` returns the current snapshot without starting source work.

A local-only initial load cannot establish a remote cursor. Call
`load(using: .cachedThenRemote)` or `.remote` before `loadNext()` when local
sources may satisfy the default cached load.

## Observe continuation state

Initial loading remains described by `state`, `isLoading`, `isLoaded`, and
`error`. Continuation work has separate observable state:

```swift
buying.isLoadingNextPage
buying.hasNextPage
buying.nextPageError
buying.pagination
```

The established snapshot remains loaded while a continuation request runs or
fails. A failed page leaves both the current values and cursor available, so a
later `loadNext()` retries the same position. A successful retry clears
`nextPageError`.

Calling `loadNext()` without paginated remote operations throws
`ContinuumError.missingPaginatedRemoteSource(namespace:)`. Calling it before
the initial remote page throws
`ContinuumError.initialPageNotLoaded(namespace:)`. Both failures are exposed
through `nextPageError`.

## Coordinate page work

Concurrent `loadNext()` calls for the same cursor share one source operation.
Initial loads, refreshes, and continuation publication are serialized inside
each selected partition.

A remote load supersedes continuation work and replaces the merged snapshot
with a new initial page. Generation checks prevent obsolete page values from
entering the refreshed snapshot, including when the source ignores cooperative
cancellation.

Different partitions keep independent values, cursors, continuation state, and
in-flight work.

Next: [Loading snapshots](loading.md) ·
[Mutating remote values](remote-mutations.md) ·
[Partitioning buckets](partitioning.md) · [Roadmap](roadmap.md)
