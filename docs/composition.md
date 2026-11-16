# Compose live values

A `Composition` derives an observable, asynchronously iterable result from current inputs. Create and access it on the main actor. Retain it for as long as the feature needs the result.

## Declare inputs

```swift
let overview = Composition {
  Input(repo.card)
  Input(repo.fundingSources[cardID]).optional()
  Input { preferences.currencyCode }
} transform: { card, sources, currency in
  CardOverview(card: card, sources: sources ?? [], currency: currency)
}
```

| Input | Behavior |
| --- | --- |
| `Input(source)` | Observe an `UpdateSource` and forward composition load requests to it. |
| `Input(source, loads: false)` | Observe a source without loading it through this input. |
| `Input { expression }` | Read an always-available value and track the observable properties it reads. |
| `.optional()` | Supply `nil` when the input is unavailable or failed. Its error still contributes to `Composition.error`. |

A required success supplies its value, including an empty collection or a domain `nil`. A required failure fails the outcome. When there are no required failures, an unavailable required input makes the composition wait. `.optional()` keeps a single optional layer when the value is already optional.

The builder selects conditional branches at construction. Both sides of `if/else` supply the same types in order. An `if` without `else` makes each input in that branch optional.

## Load the composition

```swift
await overview.load(using: .remote)
try await repo.fundingSources[cardID].reset()
```

`load(using:)` forwards the policy unchanged and loads independent sources concurrently. The same source loads once per top-level call, even across nested compositions. `isLoading` stays true while one or more calls are active. Before returning, `load` processes pending observations of the composition's inputs. When only errors change, it can update `error` without rerunning the transform.

Call reset, pagination, entry loading, and mutations on the source that owns the data. [Bucket loading policies](loading.md)

## Observe outcomes and resets

Read `latest` under Swift Observation or iterate a bucket or composition with `for await`.

```swift
for await update in overview {
  switch update {
  case .result(.success(let value)): render(value)
  case .result(.failure(let error)): report(error)
  case .reset: clear()
  }
}
```

Each iterator starts with the current result if one exists. Initial unavailability is silent. Failures are values and iteration continues. Swift Observation can coalesce changes, and equal results can repeat. [Ownership and buffering](resource-lifetime.md)

A required Continuum source's reset clears the prior composed result even when the reset and its replacement coalesce. If a failure accompanies the reset, the composition emits `.reset` first. An optional input treats reset as `nil`. For an external source, the composition reacts when it observes `.reset`.

## Observe composition errors

`Composition.error` can coexist with a successful `latest`. It includes errors from required and optional inputs in declaration order. Nested compositions contribute their underlying errors in place, followed by any error thrown by the current transform.

`mapFailures:` receives the full list when errors are present. It defaults to an ordered `CompositionError`. If `latest` is a failure, it carries the same mapped error. Keep the mapper free of side effects.

## Resolve related entries

Use `Input(root).resolving(_:from:)` when a collection refers to entries in an indexed bucket.

```swift
let rows = Composition {
  Input(posts).resolving(\.authorID, from: authors)
} transform: { snapshot in
  snapshot.root.map { post in
    FeedRow(post: post, author: snapshot.related[post.authorID])
  }
}

await rows.load()
```

The root array supplies row order. The dictionary supplies authors by ID. `FeedRow` in this example accepts an optional author because a Swift dictionary subscript returns an optional.

The input reuses matching entries already loaded in the related bucket, including a selected partition. Its `RemoteSource` needs `loadEntry:` only to fetch missing IDs. Distinct missing IDs load concurrently. When no entry loader can supply a missing ID, the input reports `ContinuumError.missingEntrySource`. The input publishes the root and related entries together.

- Call `posts.loadNext()` to advance the cursor. The complete pair updates as related lookups finish. A failed page is retried on `posts`.
- Call `rows.load()` to load the root and settle or retry missing entries. The root applies its own load policy, including freshness for `.cached`. Related lookups reuse available entries for every policy. Use `Input(root, loads: false)` to observe the root and resolve related entries while managing the root's loads separately. If another page arrives during lookup, the load waits for the current pair to settle.
- A lookup failure leaves the prior complete pair visible and appears in `rows.error`. Without a complete pair, a required input fails the composition. Use `.optional()` if the transform can accept a missing pair. Reset either bucket to invalidate the pair. After a related-bucket reset, call `rows.load()` to resolve the current root again.

## Observe an external source

`UpdateSource<Snapshot>` is the public input contract behind `Input(source)`. Apps and libraries can conform a main-actor observable class with a `Sendable` snapshot. Implement `load(using:)` to respond to composition loads. Its default is a no-op for sources that only publish updates. The source owns its fetch, reset, and subscription behavior.

```swift
import Continuum
import Observation

@MainActor
@Observable
final class UnreadCountSource: UpdateSource {
  private(set) var latest: Update<Int> = .reset
  private let fetch: @MainActor (LoadPolicy) async -> Update<Int>

  init(fetch: @escaping @MainActor (LoadPolicy) async -> Update<Int>) {
    self.fetch = fetch
  }

  func load(using policy: LoadPolicy) async {
    let update = await fetch(policy)
    guard !Task.isCancelled else { return }
    latest = update
  }
}

@MainActor
func makeBadge(from unreadCount: UnreadCountSource) -> Composition<String> {
  Composition { Input(unreadCount) } transform: { String($0) }
}
```

`error` defaults to `nil`. Publish an initial fetch failure as a failure in `latest`. For a failed refresh with usable data, retain the success in `latest` and expose the operation failure through `error`. On reset, clear any error and publish `.reset`. The external source also owns cancellation and discarding obsolete results.
