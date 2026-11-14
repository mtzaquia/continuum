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
| `Input(source)` | Observe an `UpdateSource`. Buckets, resolved buckets, and nested compositions also participate in loading. |
| `Input(source, loads: false)` | Observe a Continuum source without loading it through this input. |
| `Input { expression }` | Read an always-available value and track the observable properties it reads. |
| `.optional()` | Supply `nil` when the input is unavailable or failed. Its error still contributes to `Composition.error`. |

A required success supplies its value, including an empty collection or a domain `nil`. A required failure fails the outcome. When there are no required failures, an unavailable required input makes the composition wait. `.optional()` keeps a single optional layer when the value is already optional.

The builder selects conditional branches at construction. Both sides of `if/else` supply the same types in order. An `if` without `else` makes each input in that branch optional.

## Load the composition

```swift
await overview.load(using: .remote)
try await repo.fundingSources[cardID].reset()
```

`load(using:)` forwards the policy unchanged and loads independent Continuum sources concurrently. The same source loads once per top-level call, even across nested compositions. `isLoading` stays true while one or more calls are active. Before returning, `load` processes pending observations of the composition's inputs. When only errors change, it can update `error` without rerunning the transform.

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

Use `bucket.resolving(_:from:)` when a collection refers to entries in an indexed bucket.

```swift
let feed = posts.resolving(\.authorID, from: authors)
let rows = Composition { Input(feed) } transform: { snapshot in
  snapshot.root.map { post in
    FeedRow(post: post, author: snapshot.related[post.authorID])
  }
}

await rows.load()
```

The root array supplies row order. The dictionary supplies authors by ID. `FeedRow` in this example accepts an optional author because a Swift dictionary subscript returns an optional.

The resolved source reuses matching entries already in the related bucket and fetches distinct missing IDs concurrently with `loadEntry`. Retain the resolved source to reuse its work across compositions. It publishes the root and related entries together.

- Call `posts.loadNext()` to advance the cursor. The complete pair updates as related lookups finish. A failed page is retried on `posts`.
- Call `feed.load()` to settle or retry missing entries for the current root. With an available root, the default `.cached` policy reuses it. `.remote` and `.cachedThenRemote` first load the root and still reuse available related entries. If another page arrives during lookup, the load waits for the current pair to settle.
- A lookup failure leaves the prior complete pair visible and appears in `feed.error`. Without a complete pair, `latest` reports a failure. Reset either underlying bucket to invalidate the pair. After a related-bucket reset, call `feed.load()` to resolve the current root again.

## Observe an external source

`UpdateSource<Snapshot>` is the public observation protocol behind `Input(source)`. Apps and libraries can conform a main-actor observable class with a `Sendable` snapshot. External sources own when they fetch, reset, or subscribe to upstream events. Call those operations directly.

```swift
import Continuum
import Observation

@MainActor
@Observable
final class UnreadCountSource: UpdateSource {
  private(set) var latest: Update<Int> = .reset

  func receive(_ count: Int) {
    latest = .result(.success(count))
  }

  func reset() {
    latest = .reset
  }
}

@MainActor
func makeBadge(from unreadCount: UnreadCountSource) -> Composition<String> {
  Composition { Input(unreadCount) } transform: { String($0) }
}
```

`error` defaults to `nil`. Publish an initial fetch failure as a failure in `latest`. For a failed refresh with usable data, retain the success in `latest` and expose the operation failure through `error`. On reset, clear any error and publish `.reset`. The external source also owns cancellation and discarding obsolete results.

## Bind to Ensemble

In an [Ensemble](https://github.com/mtzaquia/ensemble) 3 `@ViewDataSource` binding, call the appropriate source load in `refresh`. In `receive`, use this mapping.

```swift
switch update {
case .result(let result): sink.receive(result)
case .reset: sink.reset()
}
```

Read `composition.error` separately for refresh feedback when data stays available. `AsyncContent` connects the source on render. Call `viewData.connect()` for a consumer that uses `ViewData` directly. See the [consumer fixture](../IntegrationTests/EnsembleConsumer/README.md) for a runnable binding.
