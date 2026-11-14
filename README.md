# 🎞️ Continuum

Continuum is a Swift library for loading, observing, and combining typed snapshots.

- A **bucket** owns one snapshot and its load, mutation, and reset operations. An indexed bucket can also load individual entries.
- A **partitioned bucket** creates independent buckets by key, such as one list per account.
- A **composition** derives one result from observable inputs and coordinates loads across Continuum sources. Apps and other libraries can supply their own inputs through the public `UpdateSource` protocol.

## Install

Continuum 2.0 requires Swift 6.3 and supports iOS 17+ and macOS 14+. Pin the prerelease in Swift Package Manager and link the `Continuum` product to your target.

```swift
.package(url: "https://github.com/mtzaquia/continuum.git", exact: "2.0.0-beta.10")
```

In Xcode, add the same repository URL and choose **Exact Version** with `2.0.0-beta.10`. See the [prerelease notes](https://github.com/mtzaquia/continuum/releases/tag/v2.0.0-beta.10), [1.1 documentation](https://github.com/mtzaquia/continuum/tree/v1.1.0), or [migration guide](docs/migrating-to-2.md).

## Five-minute start

Use `Key<Value>` for one value or `IndexedKey<ID, Value>` for an ordered collection. In this example, a bucket loads posts and a composition turns them into titles. Replace the example `RemoteSource` closure with your client call.

```swift
import Continuum

nonisolated struct Post: Identifiable, Sendable {
  let id: Int
  let title: String
}

@MainActor
func showPostTitles() async {
  let posts = Bucket(IndexedKey<Post.ID, Post>("posts")) {
    RemoteSource { [Post(id: 1, title: "Hello, Continuum")] }
  }
  let titles = Composition { Input(posts) } transform: { posts in
    posts.map(\.title)
  }

  await titles.load()

  switch titles.latest {
  case .result(.success(let value)): print(value)
  case .result(.failure(let error)): print(error)
  case .reset: break
  }
}
```

Call `load()` to start work. Read `latest` under Swift Observation or iterate the bucket or composition with `for await`. A failed refresh can leave a usable snapshot in `latest` and report the failure through `error`. Retain the bucket in your repository and the composition for as long as its derived value is needed.

## Documentation

- [Load snapshots and entries](docs/loading.md) and choose a cache policy.
- [Partition a bucket](docs/partitioning.md) by account, filter, or another identity.
- [Compose live values](docs/composition.md), resolve related entries, or [observe an external source](docs/composition.md#observe-an-external-source).
- [Paginate a bucket](docs/pagination.md) and retry a failed page.
- [Persist snapshots](docs/persistence.md), [apply optimistic mutations](docs/remote-mutations.md), or [reset invalid data](docs/invalidation.md).
- [Understand ordering and cancellation](docs/operation-ordering.md), [manage lifetime](docs/resource-lifetime.md), and [inspect diagnostics](docs/diagnostics.md).

## Sample app

Open [SampleApp.xcodeproj](SampleApp/SampleApp.xcodeproj) and run the `SampleApp` scheme on an iOS simulator. The [sample](SampleApp/README.md) shows cached-then-remote loading, mutation rollback, paging, and independent partitions.

[Support Continuum](https://www.buymeacoffee.com/mtzaquia)

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
