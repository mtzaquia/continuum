# Compose narrow repositories

Continuum buckets belong to repositories with narrow data responsibilities.
Repositories load and mutate their own atomic domain snapshots. Use cases
coordinate several repositories and assemble the display-ready models required
by a feature.

## Keep each repository atomic

A posts repository owns posts and their loading state. It does not reach into
an authors repository to decorate them for a particular screen.

```swift
import Continuum
import Observation

nonisolated struct Post: Identifiable, Sendable {
  let id: Int
  let authorID: Int
  let title: String
}

enum PostsData {
  static let all = IndexedKey<Post.ID, Post>("posts")
}

@Observable
final class PostsRepository {
  let posts: IndexedBucket<Post.ID, Post>

  init(
    cached: @escaping @Sendable () async throws -> [Post]?,
    remote: @escaping @Sendable () async throws -> [Post]
  ) {
    posts = Bucket(PostsData.all) {
      LocalSource(cached)
      RemoteSource(remote)
    }
  }
}
```

`posts.load()` resolves the complete collection. A successful `[]` result sets
`posts.isLoaded` to `true`; before any snapshot is established, `posts.values`
is also empty but `posts.isLoaded` is `false`.

An authors repository follows the same pattern with its own key, sources, and
mutation behavior. Neither repository needs to know which features consume it:

```swift
nonisolated struct Author: Identifiable, Sendable {
  let id: Int
  let name: String
}

enum AuthorsData {
  static let all = IndexedKey<Author.ID, Author>("authors")
}

@Observable
final class AuthorsRepository {
  let authors: IndexedBucket<Author.ID, Author>

  init(
    cached: @escaping @Sendable () async throws -> [Author]?,
    remote: @escaping @Sendable () async throws -> [Author]
  ) {
    authors = Bucket(AuthorsData.all) {
      LocalSource(cached)
      RemoteSource(remote)
    }
  }
}
```

## Assemble display models in a use case

A use case coordinates the repositories needed for one workflow and returns the
model that workflow should display:

```swift
struct FeedRow: Sendable {
  let postID: Post.ID
  let title: String
  let authorName: String
}

struct LoadFeed {
  let postsRepository: PostsRepository
  let authorsRepository: AuthorsRepository

  func callAsFunction() async throws -> [FeedRow] {
    async let posts = postsRepository.posts.load()
    async let authors = authorsRepository.authors.load()
    _ = try await (posts, authors)

    return postsRepository.posts.values.compactMap { post in
      guard let author = authorsRepository.authors[post.authorID] else {
        return nil
      }

      return FeedRow(
        postID: post.id,
        title: post.title,
        authorName: author.name
      )
    }
  }
}
```

Continuum handles each bucket's storage, loading state, source order, and
coalescing. The use case owns cross-repository orchestration and presentation
composition.

## Observe composed display models

Use `bucketUpdates` when the composed model should continue changing after its
initial load:

```swift
struct ObserveFeed {
  let postsRepository: PostsRepository
  let authorsRepository: AuthorsRepository

  func callAsFunction() -> AsyncStream<BucketUpdate<[FeedRow]>> {
    bucketUpdates(
      observing: (
        postsRepository.posts,
        authorsRepository.authors
      )
    ) { posts, authors in
      let authorsByID = Dictionary(
        uniqueKeysWithValues: authors.map { ($0.id, $0) }
      )

      return posts.compactMap { post in
        guard let author = authorsByID[post.authorID] else {
          return nil
        }

        return FeedRow(
          postID: post.id,
          title: post.title,
          authorName: author.name
        )
      }
    }
  }
}
```

The dependency tuple can contain any number of unpartitioned buckets and
selected partitions. The transform runs only when every dependency has a
successful snapshot; a successful empty snapshot is available data. Dependency
failures take precedence over unavailable dependencies. By default, the first
failure in tuple order is emitted; pass `mapFailures` to wrap every current
dependency failure in a domain error. A thrown transform error is emitted
directly.

Initial and repeated unavailable states remain silent. After a result, an
unavailable dependency emits one `.reset`; a replacement available before
observation resumes emits its result directly. Creating the sequence does not
load its dependencies, so the use case still controls when and how they load.

## Treat pagination as additive snapshot work

The initial load establishes the atomic indexed snapshot. Pagination adds pages
to that snapshot without making individual entries independently loadable:

- Refresh replaces the complete snapshot.
- A next page appends new indices in page order.
- Values for existing indices update without moving.
- Initial loading state remains atomic to the unpartitioned bucket or selected
  partition.
- Page loading, cursors, completion, and page errors remain pagination state.

This keeps `posts.isLoaded` unambiguous: it describes whether the initial
snapshot has been established, including a successful empty snapshot.

## Keep dependencies local

Repository initializers receive their local and remote operations, storage
drivers, or clients. Feature modules can declare their keys and repositories
without registering them in an application-wide Continuum container.

When several stable queries share one snapshot shape, the repository can own
one partitioned bucket rather than duplicating repository types or forwarding
one method per query. See [Partitioning buckets](partitioning.md).

The application composes repositories into use cases at its dependency
boundary. Continuum does not require repositories to share a parent store or
produce presentation-specific joined models.

Next: [Partitioning buckets](partitioning.md) · [Roadmap](roadmap.md)
