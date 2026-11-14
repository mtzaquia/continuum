# Compose live values

A `Composition` turns current values from several sources into one observable,
asynchronously iterable outcome. Retain it at the feature or use-case boundary;
repositories can continue owning their individual buckets.

## Declare inputs

```swift
let displayUser = Composition {
  Input(repo.user)
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
| `Input(source)` | Observe an `UpdateSource`. Forward loads when the source conforms to `LoadableUpdateSource`. |
| `Input(source, loads: false)` | Observe a source without forwarding loads. |
| `Input { expression }` | Read an always-available observable value. |
| `.optional()` | Supply nil when the input is unavailable or failed. Its error still contributes to the composition error. |

Required inputs block the transform until available. Empty collections are
available. An optional-valued bucket remains required unless `.optional()` is
applied; the modifier flattens an existing optional rather than adding a layer.
A successfully loaded nil, a failure, and unavailability then all contribute nil.

Construct and access compositions on the main actor. Observable read closures
should have no side effects; nonobservable mutations cannot trigger updates.

## Load the composition

```swift
let overview = Composition {
  Input(repo.card)
  Input(repo.fundingSources[cardID])
} transform: { card, sources in
  CardOverview(card: card, fundingSources: sources)
}

await overview.load(using: .remote)
try await repo.fundingSources[cardID].reset()
```

A load forwards the policy unchanged and waits for each distinct loadable source.
Independent sources load concurrently, and shared sources load once across
nested branches. Each source publishes its own errors. `isLoading` tracks
active calls to the composition's `load`.

Call reset, entry loading, pagination, and mutations directly on the source.
The composition observes the resulting changes.

[Bucket loading policies →](loading.md)

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

Failures do not end observation. A throwing transform produces a failure with
the mapped error. A retained bucket snapshot supplies its value to required and
optional inputs. When a bucket has only a failure, starting a retry keeps that
outcome visible until a new result arrives or the bucket is reset.

If a required reset accompanies a failure, the composition emits reset first
so consumers can clear invalid retained data. Reset history from buckets and
nested compositions survives observation coalescing. For a custom source,
the composition reacts when it observes an unavailable outcome.

Each iterator is independent. Observation may coalesce changes, equal results
may repeat, and buffering is unbounded. [Ownership and buffering →](resource-lifetime.md)

## Observe composition errors

`error` is observable and can be set while `latest` is successful. It includes
current errors from required and optional inputs, including bucket operation
errors with retained data. During a bucket retry, the previous error stays
available alongside any usable snapshot. Nested compositions contribute their
underlying errors in place. The order follows input declaration order, with a
thrown transform error last. Errors clear as their sources recover.

The default `mapFailures:` returns a `CompositionError` with an ordered `errors`
array. Each evaluation maps the full list once when errors are present. If a
required input blocks the transform or the transform throws, `latest` publishes
the same mapped error as `error`. That failure can include errors from optional
inputs. Keep the mapper free of side effects.

## Compose a repository query

Have a repository conform to `UpdateSource` when it owns an observable
`latest: Update<Snapshot>`. Add `LoadableUpdateSource` to participate in
composition loads.

Publish an unavailable query's failure through `latest`. If a refresh fails
while its snapshot is still usable, keep that snapshot in `latest` and publish
the refresh error through `error`. Preserve both during retry. On reset, clear
the snapshot and error, publish `.reset`, and discard results from requests
that started before that reset.

A query can own an upstream sequence subscription or fetch a joined snapshot
through bucket entry loaders. For example, a feed query can load posts, fetch
the distinct author IDs through `authors.load(id:using:)`, and publish the
joined result before returning. Pass that query to `Input(feed)`. When the
required data is already available in buckets, pass those buckets as separate
inputs and join their snapshots in the transform.

## Nest and select inputs

Use a composition as an input to observe its outcome and load it with the
parent. In this example, `screen.load(using: .remote)` forwards `.remote` to
`displayUser` and to `repo.recommendations` when that input is included.

```swift
let screen = Composition {
  Input(displayUser)
  if includeRecommendations {
    Input(repo.recommendations)
  }
} transform: { user, recommendations in
  ScreenModel(user: user, recommendations: recommendations)
}
```

Each top-level load visits a shared source once, even when multiple inputs or
branches reference it. Separate top-level calls run independently. Use
`Input(displayUser, loads: false)` for observation alone.

Branches are selected once at construction. Both sides of `if/else` supply the
same parameter types in order. An `if` without `else` makes each parameter
optional.

## Bind to Ensemble

[Ensemble](https://github.com/mtzaquia/ensemble) is a separate SwiftUI library
for presentation state. An Ensemble 3 model can pass each Continuum outcome
to `ViewData` and expose the composition's errors alongside it.

```swift
import Ensemble

@MainActor
final class DisplayUserModel {
  let displayUser: Composition<DisplayUser>
  var error: (any Error)? { displayUser.error }

  @ViewDataSource(
    make: { (model: Self) in model.displayUser },
    refresh: { model, _ in await model.displayUser.load(using: .remote) },
    receive: { _, update, sink in
      switch update {
      case .result(let result): sink.receive(result)
      case .reset: sink.reset()
      }
    }
  )
  var viewData: ViewData<DisplayUser>

  init(displayUser: Composition<DisplayUser>) {
    self.displayUser = displayUser
  }
}
```

Retain the model for the life of the feature. `AsyncContent` connects the source
when rendered. Call `viewData.connect()` if consuming it without `AsyncContent`.
The binding starts empty, and reset clears the displayed value. A failed bucket
refresh with retained data delivers success to `ViewData`. To show a refresh
banner or log the failure, observe the model's `error`.
[Migration guide →](migrating-to-2.md)

The [consumer fixture](../IntegrationTests/EnsembleConsumer/README.md) verifies
bucket, nested-composition, and repository query bindings.

Next: [Loading](loading.md) · [Resource lifetime](resource-lifetime.md)
