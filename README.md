# 🎞️ Continuum

[![Tests](https://github.com/mtzaquia/continuum/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/mtzaquia/continuum/actions/workflows/tests.yml)
[![Swift 6.3](https://img.shields.io/badge/Swift-6.3-orange.svg)](https://www.swift.org/)
[![iOS 17+](https://img.shields.io/badge/iOS-17%2B-blue.svg)](https://github.com/mtzaquia/continuum/blob/main/Package.swift)
![Class C](https://img.shields.io/badge/class-C-orange)

<a href="https://www.buymeacoffee.com/mtzaquia" target="_blank"><img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me a Coffee" style="height: 30px !important;" ></a>

`Continuum` manages typed snapshots in Swift: load from cache or network,
observe changes, and combine related data into values your feature can use.

A **bucket** owns one complete snapshot. A **partition** gives the same bucket
shape independent snapshots for different keys. A **composition** combines
current values and coordinates their loading.

## Install

Requires Swift 6.3, iOS 17+ or macOS 14+. Add the package dependency below
and the `Continuum` product to your target:

```swift
.package(url: "https://github.com/mtzaquia/continuum.git", from: "1.1.0")
```

Version 1.1.0 includes compositions, direct iteration, and snapshot freshness.
When upgrading from 1.0.x, review the [migration notes](https://github.com/mtzaquia/continuum/releases/tag/v1.1.0)
for observation-protocol and sendable key-path changes.

For the upcoming 2.0.0 release, see the [unreleased notes](CHANGELOG.md)
and [2.0.0 migration guide](docs/migrating-to-2.md).

## Five-minute start

Use `Key<Value>` for a single value or `IndexedKey<ID, Value>` for an ordered
collection. Buckets and compositions are main-actor isolated; source closures
are sendable asynchronous operations.

```swift
import Continuum

nonisolated struct Post: Identifiable, Sendable {
  let id: Int
  let title: String
}

@MainActor
func example() async throws {
  let posts = Bucket(IndexedKey<Post.ID, Post>("posts")) {
    LocalSource { nil }
    RemoteSource { [Post(id: 1, title: "Hello, Continuum")] }
  }

  try await posts.load()
  print(posts.values)       // Complete ordered snapshot.
  print(posts[1]?.title)    // One indexed value.

  try await posts.store(Post(id: 2, title: "Another post"))
  try await posts.remove(1)
}
```

Retain buckets in a repository or feature owner. Replace the example closures
with your database and client calls. A local source returns `nil` on a miss;
an empty array is a successful snapshot. `isLoaded` distinguishes that success
from a bucket that has never loaded.

The default `.cached` policy returns memory or tries local sources before
remote. Use `.cachedThenRemote` to show cache while refreshing, or `.remote`
to supersede work and fetch fresh data. Add `staleAfter: .seconds(300)` to a
bucket initializer to have cached snapshot loads refresh stale data while
keeping existing values readable. [Choose a loading policy →](docs/loading.md)

An established snapshot stays available as a successful `latest` after a failed
refresh or rollback. Observe `error` or `state.error` for the operation failure.
A `.cached` memory hit returns the retained value and preserves the error.

### Declare repository properties

Use the upcoming release's bucket aliases in repository properties and
protocol requirements.

```swift
@MainActor
protocol PostRepository {
  var posts: IdentifiedBucket<Post> { get }
  var featured: ValueBucket<Post> { get }
  var postsByTopic: PartitionedIdentifiedBucket<String, Post> { get }
  var topicNames: PartitionedValueBucket<String, [String]> { get }
}
```

`IdentifiedBucket` and `PartitionedIdentifiedBucket` infer the index type from
the model's `Identifiable.ID`. For a custom index, use `IndexedBucket<Index, Value>`
or `PartitionedIndexedBucket<Partition, Index, Value>` with an `indexedBy:` key.

## Observe and combine

Buckets, selected partitions, and compositions expose an observable `latest`
outcome and support direct asynchronous iteration:

```swift
let titles = Composition {
  Input(posts)
} transform: { posts in
  posts.map(\.title)
}

await titles.load()
let current = titles.latest
let error = titles.error

for await update in titles {
  switch update {
  case .result(.success(let titles)): print(titles)
  case .result(.failure(let error)): print(error)
  case .reset: break // Clear previously displayed data.
  }
}
```

Add `Input` declarations to combine sources. Inputs are required by default;
`.optional()` supplies nil when unavailable or failed. `.resolving` derives
relationships from a collection's keys. `titles.load()` loads bucket and nested
composition inputs with `.cached` by default. Use `Input(source) { _ in }` to
observe an input without loading it through the composition.
Observe `error` for current input and transform errors, including failed
refreshes with retained data. The default `CompositionError` lists them in
input order.
[Build a live composition →](docs/composition.md)

## Guides

| I want to… | Read |
| --- | --- |
| Load snapshots or individual indexed entries | [Loading](docs/loading.md) |
| Combine values, streams, and relationships | [Compositions](docs/composition.md) |
| Keep independent snapshots for different keys | [Partitioning](docs/partitioning.md) |
| Save snapshots to local storage | [Persistence](docs/persistence.md) |
| Send mutations to a server | [Remote mutations](docs/remote-mutations.md) |
| Fetch and merge subsequent pages | [Pagination](docs/pagination.md) |
| Clear stale data | [Invalidation](docs/invalidation.md) |
| Understand cancellation and overlapping work | [Operation ordering](docs/operation-ordering.md) |
| Manage retention and observation costs | [Resource lifetime](docs/resource-lifetime.md) |
| Inspect loading and mutation activity | [Diagnostics](docs/diagnostics.md) |

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
