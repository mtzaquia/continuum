# Compose live values

A `Composition` turns current values from several sources into one observable,
asynchronously iterable outcome. Retain it at the feature or use-case boundary;
repositories can continue owning their individual buckets.

## Declare inputs

```swift
let displayUser = Composition {
  Input(repo.user) { policy in
    try await repo.user.load(using: policy)
  }
  Input(repo.favorites).optional()
  Input { preferences.language }
} transform: { user, favorites, language in
  DisplayUser(user: user, favorites: favorites, language: language)
}
```

The transform receives underlying values: `Key<User>` supplies `User`, and an
indexed favorites bucket supplies `[Favorite]`. No snapshot wrapper is exposed.

| Input | Behavior |
| --- | --- |
| `Input(source)` | Observe a bucket, selected partition, or another composition. |
| `Input(source) { policy in … }` | Also participate in explicit composition loads. |
| `Input { expression }` | Track observable properties read by the closure. Add `load: { policy in … }` for loading. |
| `.optional()` | Supply nil when unavailable or failed, including loading failures. |

Required inputs block the transform until available. Empty collections are
available. An optional-valued bucket remains required unless `.optional()` is
applied; the modifier flattens an existing optional rather than adding a layer.
A successfully loaded nil, a failure, and unavailability then all contribute nil.

Construct and access compositions on the main actor. Observable read closures
should have no side effects; nonobservable mutations cannot trigger updates.
Ordinary input loading is explicit. Sequence subscriptions and relationship
resolution have the startup behavior described below.

## Load the composition

```swift
await displayUser.load(using: .cached)
await displayUser.load(using: .remote)
```

Policies pass unchanged to every configured action. Independent actions run
concurrently, and `load` waits for all of them. A failed action does not cancel
siblings. Actions must update their observed sources; returning does not itself
establish a value or wait for a future sequence element.

Composition loading returns `Void`; required errors appear in its outcome.
Errors reported by a bucket follow that bucket's recovery. Other action errors
clear on the next load. Caller cancellation propagates to actions without
publishing a cancellation error.

Overlapping loads may run concurrently; the newest invocation owns each action's
error state. A relationship declaration replaces its previous dependent work.
Bucket policies still govern shared source flights. `isLoading` describes active
composition load calls, not subscriptions or automatic relationship lookups.

Independent inputs can reflect different refresh moments. A composition is not
a transaction across buckets. [Bucket loading policies →](loading.md)

## Observe outcomes and resets

Buckets, selected partitions, and compositions share the same interface:

```swift
let current = displayUser.latest // Observable Update<DisplayUser>.

for await update in displayUser {
  switch update {
  case .result(.success(let user)): render(user)
  case .result(.failure(let error)): report(error)
  case .reset: clear()
  }
}
```

`latest` begins as `.reset` and holds the current outcome, not a separately
retained last success. Iteration does not load. A new iterator receives the
current result if present; initial and repeated unavailability stay silent.

| Event | Required input | Optional input |
| --- | --- | --- |
| Success | Supply its value | Supply its value |
| Failure | Fail the combined outcome | Supply nil |
| Reset | Clear the previous outcome and wait | Supply nil |

Failures do not end observation. A throwing transform also produces a failure.
The default error is the first required failure in declaration order; use
`mapFailures:` before `transform:` to combine them.

If a required reset accompanies a failure, the composition emits reset first
so consumers can clear invalid retained data. Reset history from buckets and
nested compositions survives observation coalescing. Unrelated inputs continue
observing; resets do not invoke loading actions or restart subscriptions.

Each iterator is independent. Observation may coalesce changes, equal results
may repeat, and buffering is unbounded. [Ownership and buffering →](resource-lifetime.md)

## Resolve relationships

Pair `.resolving` with an array input when its entries identify required data:

```swift
let authors = Bucket(IndexedKey<Author.ID, Author>("authors")) {
  RemoteSource {
    Load { try await client.authors() }
    LoadEntry { id in try await client.author(id: id) }
  }
}

let feed = Composition {
  Input(posts) { policy in
    try await posts.load(using: policy)
  }
  .resolving(\.authorID, from: authors)
} transform: { posts, authorsByID in
  posts.map { post in
    FeedRow(post: post, author: authorsByID[post.authorID])
  }
}
```

This contributes **two parameters**: `[Post]` and `[Author.ID: Author]`.
Continuum deduplicates keys, loads their entries concurrently, and delivers
the root and complete dictionary together. A previous complete pair may remain
visible while its replacement resolves. Empty roots supply an empty dictionary.
Dictionary subscripts still return Swift optionals.

Resolution uses [entry loading](loading.md#load-one-indexed-entry): existing
entries stay observed; missing entries are fetched without changing the list.
The composition retains the latest observed value while its key is needed,
including after removal from the list. Resetting the bucket invalidates it.
Entry failures affect the composition, not the list, and remain until a successful
relationship load or reset. Collection edits alone do not clear load errors.

`from:` also accepts an explicitly selected indexed partition. Partition keys
select a collection (such as a search query); foreign keys select its entries.
A cached entry needs no loader. Fetching without `LoadEntry` produces
`ContinuumError.missingEntrySource`, which fails the required relationship.

### Use a one-shot lookup

For a service that does not expose buckets, replace `from:` with a closure:

```swift
Input(posts) { policy in
  try await posts.load(using: policy)
}
.resolving(\.authorID) { id, policy in
  try await legacyService.author(id: id, using: policy)
}
```

The sendable closure runs away from main-actor isolation and returns a sendable
value. Returned values are retained while their keys remain needed, but are not
observed after return. Use buckets when you need shared caching, persistence,
mutation, invalidation, or live observation. This bridge has no cache
configuration or TTL.

### Understand resolution work

| Trigger | Work |
| --- | --- |
| Explicit composition load | Finish the root action, then resolve all required keys with the same policy. |
| Available root at construction, or a changed root | Resolve newly introduced keys with `.cached`; retain values for continuing keys. Failed one-shot lookups are retryable. |
| Keys leave the root | Discard their derived values and stop observing their sources. |
| Root reset | Clear the pair and discard retained relationship values. |
| Related bucket reset | Invalidate the pair; wait for explicit loading or externally established data. |

For `.cachedThenRemote`, relationships follow the completed root refresh.
`loadNext()` changes an array root through ordinary observation; no special
pagination composition API is needed.

Required lookup failures use the usual failure outcome. Retry with
`await feed.load(using: ...)`. New roots and explicit loads cancel obsolete
relationship work; late returns cannot replace the current pair. Caller
cancellation cancels that load's dependent tasks. Native bucket flights retain
their shared-work cancellation rules and remain independently observable.

The root must be `Input<[Element]>`. Elements and resolved values are sendable;
keys are `Hashable & Sendable`, and key paths must preserve sendability. The
related collection's index must match the foreign-key type. One relationship per declaration
is supported; chaining and `.optional()` on the pair are not. A conditional
builder block can make both parameters optional.

## Bridge result sequences

```swift
let displayUser = Composition {
  Input(
    updates: { service.results() },
    subscriptionOnLoad: .restart,
    load: { policy in try await service.refresh(using: policy) }
  )
} transform: { user in
  DisplayUser(user: user)
}
```

The factory returns a `Sendable` async sequence of `Result<Value, Failure>`;
standard `AsyncStream` and `AsyncThrowingStream` work. It runs on the main actor
once per composition at construction, then iteration runs in its own task.
The iterator itself need not be sendable.

`subscriptionOnLoad` defaults to `.keep`, including after completion. `.restart`
cancels the old subscription and creates a new iterator before loading. It can
omit the action if subscribing itself starts work. Iterator creation does not
guarantee that a hot upstream stream is ready to receive events.

Before the first result, required inputs wait and optional inputs supply nil.
Result failures recover on later successes. A thrown iteration error becomes a
failure and ends the subscription; normal completion retains the last outcome.
Restart retains the outcome and rejects late results from the old subscription.
Neither completion nor restart emits reset.

## Nest and select inputs

Use a composition as an ordinary source. Loading remains explicit:

```swift
let screen = Composition {
  Input(displayUser) { policy in await displayUser.load(using: policy) }
  if includeRecommendations {
    Input(repo.recommendations)
  }
} transform: { user, recommendations in
  ScreenModel(user: user, recommendations: recommendations)
}
```

Branches are selected once at construction. Both sides of `if/else` must supply
the same types in the same order. An `if` without `else` makes each parameter
optional. Avoid adding `.optional()` inside that block: the branch adds its own
optional layer. Loops and dynamically changing input counts are not supported.

## Bind to Ensemble

[Ensemble](https://github.com/mtzaquia/ensemble) is a separate SwiftUI library
for presentation state. Its `ViewData` can retain successful data while showing
loading or failure; Continuum provides the data outcomes.

```swift
import Ensemble

let context = ViewDataContext()
let viewData = ViewData<DisplayUser>()

context.bind(
  { displayUser },
  to: viewData,
  reload: .refresh {
    Task { await displayUser.load(using: .remote) }
  }
) { update, sink in
  switch update {
  case .result(let result): sink.receive(result)
  case .reset: sink.reset()
  }
}

await displayUser.load()
```

Add Ensemble separately and retain a `ViewDataContext`, a `ViewData<DisplayUser>`,
and the composition in the feature owner. Initial silence preserves loading;
a failure can retain data, while reset clears it without ending the binding.
Retain and cancel the refresh task if it should stop with the feature. Loads
started outside Ensemble do not automatically change its presentation phase.

The [consumer fixture](../IntegrationTests/EnsembleConsumer/README.md) verifies
bucket, nested-composition, and relationship bindings.

Next: [Loading](loading.md) · [Resource lifetime](resource-lifetime.md)
