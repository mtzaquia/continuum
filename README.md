# 🎞️ Continuum

[![Tests](https://github.com/mtzaquia/continuum/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/mtzaquia/continuum/actions/workflows/tests.yml)
[![Swift 6.3](https://img.shields.io/badge/Swift-6.3-orange.svg)](https://www.swift.org/)
[![iOS 17+](https://img.shields.io/badge/iOS-17%2B-blue.svg)](https://github.com/mtzaquia/continuum/blob/main/Package.swift)

`Continuum` is a typed, observable data-bucket library for Swift that keeps repository-owned snapshots coherent from cache to network.

A key establishes the identity and complete snapshot owned by one narrow
repository. The bucket loads and publishes that snapshot atomically. Use cases
compose models from several repositories when the UI needs broader,
display-ready data.

- Store heterogeneous values behind strongly typed keys.
- Load singleton or insertion-ordered indexed snapshots and merge cursor pages.
- Distinguish untouched state from a successfully loaded empty result.
- Observe values, loading, established-snapshot state, and failures with Swift
  Observation.
- Subscribe to snapshot results and reset transitions as an asynchronous sequence.
- Try local sources before remote, sharing cached work or superseding it when
  forced from remote.
- Write normalized snapshots through to one or more local destinations.
- Partition one key into independently cached and observable lists.

```swift
let posts = Bucket(PostsData.all) {
  RemoteSource(loadPosts)
}

try await posts.load()
print(posts.values)
```

## Install

Continuum 1.0.0 requires Swift 6.3 and supports iOS 17+ and macOS 14+. Add it
as a Swift Package Manager dependency:

```swift
dependencies: [
  .package(url: "https://github.com/mtzaquia/continuum.git", from: "1.0.0"),
]
```

Add the `Continuum` product to the consuming target, then `import Continuum`
where it is used.

## Five-minute start

Declare an indexed key for the complete ordered collection a repository owns.
An identifiable value uses its `id` automatically, giving the bucket
dictionary-like lookup without changing the source result from `[Post]`:

```swift
import Continuum

nonisolated struct Post: Identifiable, Sendable {
  let id: Int
  let title: String
}

enum PostsData {
  static let all = IndexedKey<Post.ID, Post>("posts")
}
```

Pass `indexedBy:` when the domain uses another stable identity.

Create the observable data bucket inside the repository. The key supplied to
`Bucket` gives every source its snapshot type:

```swift
import Observation

@Observable
final class PostsRepository {
  let posts: IndexedBucket<Post.ID, Post>

  init(
    loadCachedPosts: @escaping @Sendable () async throws -> [Post]?,
    loadRemotePosts: @escaping @Sendable () async throws -> [Post]
  ) {
    posts = Bucket(PostsData.all) {
      LocalSource(loadCachedPosts)
      RemoteSource(loadRemotePosts)
    }
  }
}
```

Bucket state is main actor-isolated. Source operations are `@Sendable`, so
capture actors, sendable clients, or immutable values rather than mutable
UI-owned objects.

Load the complete snapshot, read its ordered values, or look up one model by
index. Concurrent default loads without established memory share source work:

```swift
let repository = PostsRepository(
  loadCachedPosts: { nil },
  loadRemotePosts: {
    [Post(id: 1, title: "A remote value")]
  }
)

try await repository.posts.load()

let currentPosts = repository.posts.values
let selectedPost = repository.posts[1]
```

Indexed buckets preserve insertion order. Replacing an existing value retains
its position; removing and reinserting it appends it to the end. Read ordered
snapshots through `keys`, `values`, or `elements`; `count` and `isEmpty`
describe the current contents:

```swift
let postIDs = repository.posts.keys
let currentPosts = repository.posts.values
```

`isLoaded` belongs to the complete bucket, not an individual indexed entry. It
distinguishes an untouched bucket from a successful empty result:

```swift
if repository.posts.isLoaded && repository.posts.isEmpty {
  // The load succeeded and returned no posts.
}
```

Use `updates()` when a consumer needs an asynchronous sequence instead of
property observation:

```swift
for await update in repository.posts.updates() {
  switch update {
  case .result(.success(let posts)):
    render(posts)
  case .result(.failure(let error)):
    report(error)
  case .reset:
    clear()
  }
}
```

An established snapshot or current error is emitted immediately. Untouched and
loading-without-data states remain silent; a successful empty snapshot is still
emitted. A reset is emitted only while the bucket remains unavailable, so a
replacement already available when observation resumes emits its result
directly. Creating the sequence does not start a load.

That is the core idea: each repository owns one narrow atomic snapshot; use
cases compose repository models when a feature needs a broader result.

## Local and remote sources

A data bucket can try zero or more local sources in declaration order before
falling through to its remote source:

```swift
let posts = Bucket(PostsData.all) {
  LocalSource {
    try await memoryCache.posts()
  }
  LocalSource {
    try await database.posts()
  }
  RemoteSource {
    try await client.posts()
  }
}
```

A local source returns an optional complete snapshot. `nil` means that source
missed and the bucket should continue to the next one. For an indexed key, `[]`
is a successful loaded snapshot. A thrown error stops the load.

Add a labeled trailing closure when the same local source can persist complete
snapshots:

```swift
let posts = Bucket(PostsData.all) {
  LocalSource {
    try await database.posts()
  } persist: { posts in
    try await database.replacePosts(with: posts)
  }
  RemoteSource {
    try await client.posts()
  }
}
```

The persistence operation receives `[Post]?`: `nil` removes the persisted
snapshot, while `[]` persists a known empty result. Remote loads and merged
pages write through before changing memory. Store and remove instead publish
optimistically, write through locally, then invoke their configured remote
operation. See
[Persisting local snapshots](docs/persistence.md) for ordering and failure
behavior.

A remote source can progressively disclose mutation capabilities while bucket
consumers keep using `store` and `remove`:

```swift
let posts = Bucket(PostsData.all) {
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

`Store` can return the server-authoritative model to reconcile the optimistic
snapshot in local persistence and memory. See
[Mutating remote values](docs/remote-mutations.md) for ordering, failure
behavior, and pagination.

Each atomic bucket configuration accepts:

- `LocalSource`: zero or more
- `RemoteSource`: zero or one
- `InvalidationSignal`: zero or more

The default `.cached` policy returns established memory immediately, including
while refresh work is active. Without memory, concurrent cached calls share one
flight through local sources and then remote fallback.

Use `.cachedThenRemote` to publish memory or the first local hit while always
continuing to remote:

```swift
let remotePosts = try await posts.load(using: .cachedThenRemote)
```

The call returns the remote snapshot. A missing `RemoteSource` throws before
using cache, and a remote failure preserves any published cached snapshot.

Use `.remote` to cancel and supersede active source work, skip memory and local
sources, and start a new remote flight. Repeated remote calls are latest-wins;
obsolete results and errors cannot publish.

A remote source can progressively disclose pagination without changing the
ordinary form:

```swift
let posts = Bucket(PostsData.all) {
  RemoteSource {
    Load {
      let response = try await client.posts(after: nil)
      return Page(values: response.posts, next: response.nextCursor)
    }
    NextPage { cursor in
      let response = try await client.posts(after: cursor)
      return Page(values: response.posts, next: response.nextCursor)
    }
  }
}

try await posts.load()

if posts.hasNextPage {
  try await posts.loadNext()
}
```

The initial page replaces the complete snapshot. For an indexed bucket, later
pages append in order and replace duplicate indices without moving them.
`NextPage(accumulating:indexedBy:_:)` can instead accumulate one nested
collection while refreshing its sibling values. Page loading has separate
observable state; see [Paginating buckets](docs/pagination.md).

## Partition independent lists

Pass `partitionedBy:` when one key shape has several stable query variants,
such as accounts available for buying and selling. The configuration receives
the partition value once, and its source operations capture that value:

```swift
let accounts = Bucket(
  AccountsData.all,
  partitionedBy: Purpose.self
) { purpose in
  LocalSource {
    try await database.accounts(purpose: purpose)
  }
  RemoteSource {
    try await client.accounts(purpose: purpose)
  }
}
```

Select a partition before reading, loading, or mutating its atomic snapshot:

```swift
try await accounts[.buy].load()

accounts[.buy].isLoaded
accounts[.buy].values
accounts[.buy][accountID]
```

Each partition has independent values, loading state, and coalesced work.
Different partitions can load concurrently. Partitioning is fixed structurally
by the initializer, while the trailing DSL contains only source capabilities.

## Reset and invalidate snapshots

`reset()` forgets memory, cancels active source and mutation tasks, and removes
every persisted snapshot owned by a local source with a `persist:` closure:

```swift
try await posts.reset()
```

Memory and pagination reset immediately, then writable local sources receive
`nil` in declaration order. A failure restores the previous observable state,
attempts to restore local persistence, throws, and becomes visible through
`error`.

Use `InvalidationSignal` when an application event means a snapshot is stale
everywhere. Every event performs the same `reset()`: it clears memory and sends
`nil` to every writable local source so a later cached load cannot restore the
invalid snapshot.

The parameterized `reset(including:)` overload is deprecated and remains only
as a compatibility forwarding overload.

```swift
let posts = Bucket(PostsData.all) {
  LocalSource {
    try await database.posts()
  } persist: { posts in
    try await database.replacePosts(with: posts)
  }

  RemoteSource {
    try await client.posts()
  }

  InvalidationSignal {
    NotificationCenter.default.notifications(
      named: .accountDidChange
    )
  }
}
```

The bucket owns the observation for its lifetime. See
[Invalidating snapshots](docs/invalidation.md) for sequence failures and
partition behavior.

## Documentation

- [Diagnosing bucket operations](docs/diagnostics.md) — follow correlated load,
  mutation, persistence, invalidation, and pagination stories in unified logs.
- [Loading snapshots](docs/loading.md) — choose cached, cache-then-remote, or
  latest-wins remote loading and understand their state transitions.
- [Persisting local snapshots](docs/persistence.md) — make local sources
  writable and understand atomic-state, ordering, and failure behavior.
- [Mutating remote values](docs/remote-mutations.md) — add typed remote store
  and removal capabilities while preserving the atomic snapshot pipeline.
- [Invalidating snapshots](docs/invalidation.md) — reset memory and writable
  local sources together, and invalidate stale snapshots from application
  events.
- [Paginating buckets](docs/pagination.md) — append ordered snapshots or
  accumulate one nested collection while refreshing its siblings.
- [Partitioning buckets](docs/partitioning.md) — cache and observe several
  independently loaded lists behind one typed key.
- [Repository composition](docs/repository-composition.md) — keep repositories
  narrow and assemble display-ready models in use cases.
- [Roadmap](docs/roadmap.md) — review the current decisions and remaining
  storage-driver, lifecycle, extension, and release work.

## Current scope

The current implementation includes typed atomic keys, insertion-ordered indexed
snapshots, atomic load state, typed local and remote source configuration,
partition-scoped loading, coalescing, write-through local persistence, and
event-driven invalidation, pagination, and remote mutations. Storage drivers,
schema migration, persisted pagination checkpoints, and transactions remain
for subsequent API slices.

## License

Copyright (c) 2026 @mtzaquia

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
