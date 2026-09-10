# Compose live values and loading

`Composition` combines current values from buckets, selected partitions,
observable expressions, other compositions, and external result sequences.
Declare those dependencies with `Input`, transform their concrete values, and
consume the resulting `Update` through Observation or asynchronous iteration.
Loading actions are explicit and use the same `LoadPolicy` as buckets.

## Use one observation and iteration interface

Unpartitioned buckets, selected partitions, and compositions all expose
`latest: Update<Value>` and conform to `AsyncSequence` with the same element:

```swift
let currentUser = repo.user.latest
let currentPage = repo.posts[pageID].latest
let currentDisplay = displayUser.latest

for await update in repo.user { /* Update<User> */ }
for await update in repo.posts[pageID] { /* Update<[Post]> */ }
for await update in displayUser { /* Update<DisplayUser> */ }
```

Select a partition before reading or iterating it; a partitioned bucket has no
single combined outcome. `latest` is an observable result or reset, not a
separately retained last success. Creating an iterator never loads a source.
Each iterator is independent and ends on cancellation or release. Existing
bucket value accessors such as `value`, `values`, and keyed subscripts remain
useful for reading individual values or collections.

## Declare typed inputs

Create and retain the composition on the main actor, for example in a use case:

```swift
let displayUser = Composition {
  Input(repo.user) { policy in
    try await repo.user.load(using: policy)
  }

  Input(repo.favorites) { policy in
    try await repo.favorites.load(using: policy)
  }
  .optional()

  Input { preferences.language } load: { policy in
    try await preferences.loadLanguage(using: policy)
  }
} transform: { user, favorites, language in
  DisplayUser(user: user, favorites: favorites, language: language)
}
```

`Input` is a declaration, not a wrapper delivered to the transform. A
`Key<User>` supplies `User`; an `IndexedKey<Favorite.ID, Favorite>` supplies
`[Favorite]`. The `.optional()` modifier makes that parameter optional at compile
time. If the value is already optional, `.optional()` preserves that type: both
`Input(bucketOfUser).optional()` and `Input(bucketOfOptionalUser).optional()`
supply `User?`. Loaded nil, unavailability, and failure become indistinguishable
to the transform. Without the modifier, an optional-valued bucket remains
required and propagates failures. Repeating `.optional()` adds no further layer.

Selected partitions such as `Input(repo.users[accountID])` work too.

Required inputs block the transform until available. Optional inputs supply
`nil` when missing or failed, including when their loading action fails. An
empty loaded collection is available. `Input { ... }` tracks observable
properties read by its closure; ordinary nonobservable mutations cannot trigger updates.

Omit a loading closure to observe an input without loading it. The composition
starts observing at construction without invoking loading actions. A sequence
input also starts its subscription, which may itself cause upstream work.

## Where compositions belong

Keep repositories focused on domain data: a posts repository owns posts, while
an authors repository owns authors. Each receives its own storage and network
dependencies. Neither needs to know which features consume its data, and no
global Continuum container is required.

Create and retain a composition at the feature or use-case boundary when the
feature needs a combined model. The same object coordinates loading and keeps
that model current:

```swift
@MainActor
func makeFeed(
  postsRepository: PostsRepository,
  authorsRepository: AuthorsRepository
) -> Composition<[FeedRow]> {
  Composition {
    Input(postsRepository.posts) { policy in
      try await postsRepository.posts.load(using: policy)
    }
    Input(authorsRepository.authors) { policy in
      try await authorsRepository.authors.load(using: policy)
    }
  } transform: { posts, authors in
    let authorsByID = Dictionary(
      uniqueKeysWithValues: authors.map { ($0.id, $0) }
    )
    return posts.compactMap { post in
      guard let author = authorsByID[post.authorID] else { return nil }
      return FeedRow(
        postID: post.id,
        title: post.title,
        authorName: author.name
      )
    }
  }
}
```

Here `posts` and `authors` are indexed buckets, and `FeedRow` is the feature's
plain display model. Retain the returned composition in the feature owner,
call `await feed.load(using: .cached)`, and read `feed.latest` or iterate `feed`.
The repositories keep responsibility for persistence, mutations, and pagination;
the composition owns the relationship needed by this feature. This is one
organization pattern, not a required repository protocol or layer.

## Bridge result sequences

Use `updates:` to subscribe to an external `AsyncSequence` of `Result<Value,
Failure>`. The iterator must satisfy `SendableMetatype`, as standard
`AsyncStream` and `AsyncThrowingStream` iterators do. Neither the sequence nor
the iterator itself needs to conform to `Sendable`. This is distinct
from observing the sequence object as a value:

```swift
let displayUser = Composition {
  Input(
    updates: { service.results() },
    subscriptionOnLoad: .restart,
    load: { policy in
      try await service.refresh(using: policy)
    }
  )
} transform: { user in
  DisplayUser(user: user)
}
```

`subscriptionOnLoad` defaults to `.keep`: loading runs the action without
replacing the existing subscription, even if that subscription has completed.
Choose `.restart` to cancel the previous subscription and create a fresh
sequence and iterator before the action runs. The factory must produce a usable
sequence on every invocation. A `.restart` input can omit the loading action
when restarting the sequence itself initiates the required work.

Restarting retains the current outcome, ignores late results and errors from
obsolete subscriptions, and does not wait for the first new element. Iterator
creation does not guarantee a hot upstream source is ready to receive events;
that readiness contract belongs to the source. Loading waits for iterator
creation and the action, never for a stream element. Caller cancellation stops
the loading action cooperatively, without cancelling the independent ongoing
subscription.

Before its first result a required stream blocks the transform, while
`.optional()` contributes nil. Result failures follow the usual required and
optional rules; later successes recover. A thrown iteration error publishes a
failure and ends that subscription. Normal completion preserves the last
outcome, including remaining unavailable if nothing was emitted. Completion
and restart do not produce resets. Only sequences of `Result` are supported.
Loading action errors follow the same rules as other inputs and clear on the
next load.

Construction starts one subscription per sequence input. Its factory runs on
the main actor during subscription setup. Merely declaring an
`Input` does not subscribe. Every composition gets its own subscription, even
when a declaration is reused; all consumers of one composition share that
subscription. Releasing the composition cancels upstream iteration. As with
other observed inputs, changes may coalesce; the composition is not a durable
log of every emitted element.

## Compose compositions

A composition can supply its concrete output to another composition:

```swift
let screen = Composition {
  Input(displayUser) { policy in
    await displayUser.load(using: policy)
  }
  Input(recommendations).optional()
} transform: { user, recommendations in
  ScreenModel(user: user, recommendations: recommendations)
}
```

`user` is the inner composition's output directly. Required inner failures fail
its parent; optional inner failures and resets contribute `nil`. Loading remains
explicit: `Input(displayUser)` observes without loading the inner composition.

Required reset transitions propagate through every level, including when an
inner reset is immediately followed by failure or recovery before its parent
reevaluates. The library retains that invalidation internally; callers use the
same `Input` declarations and `Update` cases. New parents begin with
the current outcome and do not replay historical resets. Initial unavailability
remains silent at every level. Updates still may coalesce; nesting does not make
loads or updates across independent inputs atomic.

## Select dependencies with branches

```swift
let name = Composition {
  if usePreview {
    Input(preview.user)
  } else {
    Input(repo.user)
  }

  if includeFavorites {
    Input(repo.favorites)
  }
} transform: { user, favorites in
  user.name + " (\(favorites?.count ?? 0))"
}
```

Branches are selected once at construction. Both sides of `if/else` must have
the same concrete value types and number of inputs. An `if` without `else`
makes each contained input optional, preserving each transform parameter's
position. Loops are not supported. Applying `.optional()` inside an `if` without
`else` introduces another optional layer; normally let the branch add it.

## Load the composition

```swift
await displayUser.load(using: .cached)
await displayUser.load(using: .cachedThenRemote)
await displayUser.load(using: .remote)
```

The `LoadPolicy` is forwarded unchanged to every configured action. The
composition does not interpret `.cached` as permission to skip an action; each
action decides how to apply the policy. Actions run concurrently, and `load` waits for all of them. A failure does not cancel sibling
actions. The composition observes intermediate source changes, so it can publish
a mixture of cached and refreshed inputs while loading proceeds. Action return
values do not establish data; actions must update their observed sources.

`load` is nonthrowing: required errors appear in the combined outcome. Errors
already reported by a bucket follow that bucket's recovery. Other action errors
remain until the next load of that input. Optional action failures contribute
`nil`. Caller cancellation propagates to actions without publishing a
cancellation error. Actions must cooperate with cancellation.

A bucket load returns its snapshot or throws. A composition load returns
`Void`; consume `latest` or iterate for its outcomes. `isLoading` describes
composition load calls, not a long-lived stream subscription or work started
independently on an input.

Overlapping calls run independently, and `isLoading` remains true until all
calls finish. The newest invocation of an action owns its action-error state;
underlying bucket policies still control source work. Declaring the same input
twice invokes both declared actions; bucket coalescing still applies where its
policy allows it.

## Observe outcomes and resets

```swift
switch displayUser.latest {
case .result(.success(let user)):
  // Present the current combined value.
case .result(.failure(let error)):
  // Present the current failure.
case .reset:
  // No combined outcome is available.
}

for await update in displayUser {
  // The same Update<DisplayUser> representation.
}
```

Before an outcome is available, `latest` equals `.reset`. Inputs that already
have values can establish a result during construction. Initial and repeated
unavailability remain silent in the sequence. Each new iterator receives the
current result, if one exists, then subsequent updates independently.

A required error fails the combined outcome without ending observation. The
transform can also throw. By default, the first required failure in declaration
order is published; pass `mapFailures:` before `transform:` to combine failures.
Optional errors never enter that list.

A required input becoming unavailable resets a previously emitted outcome. When
another required input is also failing, the sequence emits reset first, then
failure, so consumers can clear obsolete retained data. An optional reset simply
recomputes the output with `nil`. Loading alone does not reset the outcome.
Changes may coalesce before reevaluation; this sequence is not a mutation log.

### Reset one input in a mixed composition

A reset invalidates the combined output according to the reset input's
requirement. It does not reset, reload, or restart other inputs:

| Event | Combined behavior |
| --- | --- |
| Required bucket unavailable before any outcome, with no failure | Stays silent |
| Required bucket resets after an outcome, with no failure | Emits `.reset` and waits for required data |
| Optional bucket resets | Recomputes with nil when all required inputs are available |
| Required bucket resets while a required stream is failing | Emits reset, then failure |
| A result stream completes or its subscription restarts | Retains that input's last outcome |

While a required bucket is unavailable, sequence inputs keep receiving results
and observable expressions keep their current values. When the bucket becomes
available again, the transform uses the current values of all inputs.
`subscriptionOnLoad` applies only when `composition.load(using:)` is called;
a bucket reset never triggers it. If the stream is optional, its failure
contributes nil instead of failing the combined outcome.

Buffering is unbounded to preserve reset-before-failure ordering for slow
consumers. Keep consumers timely. Cancelling or releasing an iterator ends only
its subscription. The composition observes for its own lifetime; an active
iterator retains it. Releasing the composition cancels its sequence inputs.
Avoid having input closures capture an owner that retains the composition,
which would create a retain cycle. Repeated equal results are permitted,
including after a cached load that changes no source data.

## Bind to Ensemble

[Ensemble](https://github.com/mtzaquia/ensemble) is a separate SwiftUI
presentation-state library. Its `ViewData` holds the latest successful data and
presentation phase; `ViewDataContext` binds asynchronous sources and manages
reload and retry. Continuum supplies data outcomes, while Ensemble decides what
the UI can retain during loading or failure.

This integration is optional. Add the Ensemble package and its product to the
application target alongside Continuum. Retain the context, view data, and
composition in the feature owner. Configure the binding on the main actor:

```swift
import Continuum
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
  case .result(let result):
    sink.receive(result)
  case .reset:
    sink.reset()
  }
}

await displayUser.load(using: .cached)
```

Binding begins in loading. Initial silence preserves that phase until the first
outcome. A failure preserves Ensemble's last successful value; a reset clears
it without ending the binding. The refresh action enables reload and retry
without replacing the composition. The `Task` created by the synchronous reload
callback has its own lifetime; retain and cancel it in the owner if refresh work
must stop when the screen disappears. Loading started outside Ensemble does not
automatically change Ensemble's loading phase.

The [Ensemble consumer fixture](../IntegrationTests/EnsembleConsumer/README.md) verifies
this lifecycle against a local Ensemble checkout.

Continuum itself does not depend on Ensemble. Applications can extract this
handler into a constrained `bind` overload at their integration boundary.

Next: [Loading snapshots](loading.md) · [Resource lifetime](resource-lifetime.md)
