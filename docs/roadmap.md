# Continuum roadmap

Continuum provides observable data buckets owned by repositories with narrow
responsibilities. Each unpartitioned bucket or selected partition loads one
atomic snapshot. Use cases coordinate repositories and assemble their domain
models into display-ready results.

This document records the implemented foundation, decisions that should not
drift, and the remaining work toward storage drivers, lifecycle guarantees,
extension points, and an initial release.

## Current surface

`Key` owns one atomic value. `IndexedKey` owns one atomic,
insertion-ordered collection:

```swift
nonisolated struct Post: Identifiable, Sendable {
  let id: Int
}

enum PostsData {
  static let all = IndexedKey<Post.ID, Post>("posts")
}

let posts = Bucket(PostsData.all) {
  LocalSource {
    try await database.posts()
  }
  RemoteSource {
    try await client.posts()
  }
}

try await posts.load()

posts.isLoaded
posts.isLoading
posts.error
posts.values
posts[postID]
```

The current implementation includes:

- [x] Typed singleton snapshots with `Key<Value>`.
- [x] Atomic indexed snapshots with `IndexedKey<Index, Value>`.
- [x] Default index derivation from `Identifiable.id`.
- [x] Custom index derivation from a key path or `@Sendable` operation.
- [x] Main actor-isolated, `@Observable` data buckets.
- [x] Per-bucket or per-partition `LoadState`, `isLoading`, `isLoaded`, and
  `error`.
- [x] An explicit distinction between untouched and successfully loaded empty
  snapshots.
- [x] Insertion-ordered `keys`, `values`, and `elements` with `count` and
  `isEmpty`.
- [x] Model-returning indexed subscripts.
- [x] Atomic snapshot publication and typed store and removal operations.
- [x] Zero or more ordered `LocalSource` expressions.
- [x] Optional write-through persistence on each `LocalSource`.
- [x] Zero or one `RemoteSource` expression.
- [x] Optional typed `Store` and `Remove` capabilities inside `RemoteSource`.
- [x] Server-authoritative stored values, including identifier replacement.
- [x] Cached, cached-then-remote, and remote loading policies.
- [x] Single-flight cached loading when memory is absent.
- [x] Latest-wins remote loading with cancellation and generation guards.
- [x] Two-phase cache publication followed by a required remote snapshot.
- [x] Generation checks that prevent superseded work from publishing.
- [x] Reset behavior that returns a bucket to untouched state.
- [x] Local-inclusive reset through `reset(including: .localSources)`.
- [x] Event-driven reset through `InvalidationSignal`.
- [x] One optional `partitionedBy:` initializer dimension.
- [x] Lazily created observable partitions with independent atomic snapshots,
  loading state, mutation, and coalescing.
- [x] Paginated remote sources composed from `Load` and `NextPage`.
- [x] Ordered next-page merging, coalescing, observable continuation state, and
  refresh supersession.
- [x] Nested collection accumulation that refreshes sibling snapshot values.

## Decisions to preserve

- `Continuum` is the library and module name. `Bucket` is the entity, described
  as a data bucket.
- A repository owns buckets for a narrow domain responsibility. Continuum does
  not require a global store or top-level `Continuum` instance.
- A use case coordinates several repositories and assembles display-ready
  models. Repositories do not join unrelated domain models for individual
  screens.
- The key is supplied once to `Bucket.init`. DSL members infer the complete
  snapshot without receiving the key again.
- Keys remain the organizing structure. The DSL will not replace them with an
  open-ended resource model.
- `IndexedKey` is an atomic ordered collection, not a family of independently
  loadable entries.
- Indexed sources return the complete `[Value]` snapshot. The key derives
  indices and normalizes duplicates.
- Duplicate indices retain their first position and use their last value.
- Indexed subscripting returns the model: `posts[id] -> Post?`.
- Loading state belongs to the complete unpartitioned bucket or selected
  partition. Individual indexed values do not have fragmented loading state.
- `isLoaded` means that a complete snapshot has been established. It is
  independent from `isEmpty`.
- Replacing an indexed snapshot with `[]` produces `isLoaded == true`.
  `reset()` removes the snapshot and produces `isLoaded == false`.
- A bucket accepts `LocalSource` zero or more times and `RemoteSource` at most
  once. Local sources run in declaration order.
- A local source returns an optional complete snapshot. `nil` is a cache miss;
  an indexed `[]` is a successful snapshot; a thrown error stops the load.
- A `LocalSource` progressively discloses writable behavior with a labeled
  `persist:` closure receiving the normalized optional atomic snapshot.
- `nil` persistence removes the snapshot; an indexed `[]` persists a known
  empty snapshot.
- Store, remove, and local-inclusive reset publish immediately, then write
  through to every writable local source. Remote snapshots and merged
  pagination snapshots retain persist-before-publish ordering.
- A configured remote `Store` or `Remove` runs after optimistic local
  persistence. Omitting a capability keeps that bucket mutation local-only.
- `Store` returns the server-authoritative value. If an indexed identifier
  changes, the submitted identifier is removed before the authoritative value
  enters the snapshot. A `Void`-returning overload retains the submitted value.
- A mutation failure restores the previous observable snapshot and attempts to
  restore local persistence. Continuum reports the original error; this is not
  a distributed transaction with the remote system.
- Reset operations never invoke remote mutation capabilities.
- Writable local sources run sequentially in declaration order. The first
  failure stops the sequence and triggers an attempted rollback to the prior
  snapshot.
- Concurrent explicit mutations are serialized and derive from the last
  successfully published snapshot.
- `reset()` affects memory only and does not call local persistence.
- `reset(including: .localSources)` clears observable state immediately, sends
  `nil` to writable local sources, restores the prior state on failure, and
  exposes that failure through `error`.
- Any number of `InvalidationSignal` expressions observe asynchronous
  application events and perform the local-inclusive reset.
- A bucket owns invalidation observation for its lifetime. Selected partitions
  create and own their observations independently.
- An invalidation reset failure keeps its event observation active. An event
  sequence failure becomes observable state and ends that sequence.
- Source implementations own transport-to-domain transformation. There is no
  separate `Map` DSL member.
- `.cached` checks an established memory snapshot, then local sources, then the
  remote source. Established memory may return while refresh work remains
  active; otherwise concurrent cached calls share one flight.
- `.cachedThenRemote` publishes established memory or the first local hit, then
  always awaits and publishes remote. It throws before using cache when no
  remote source is configured.
- `.remote` cancels and supersedes active source work, skips memory and local
  sources, and starts a new remote flight.
- Repeated remote calls are latest-wins. Generation checks prevent superseded
  results and errors from publishing even when source cancellation is not
  cooperative.
- Pagination extends an indexed snapshot. It does not turn entries into
  independently loadable resources.
- A partition selects a stable, independently cached list. A pagination cursor
  advances that list and is not itself a partition.
- Partitioning is structural and declared once with `partitionedBy:`. It does
  not modify the key or enter the source capability DSL.
- The partition value enters through the initializer's configuration closure.
  Local and remote source operations capture it instead of receiving it
  repeatedly.
- A selected partition is created lazily and retains its own observable atomic
  snapshot, state, mutation, and coalesced source work.
- Paginated remote loading keeps `Load` and `NextPage` together inside the
  `RemoteSource` DSL. Pagination is not a disconnected sibling source.
- Nested pagination declares `accumulating:indexedBy:` on `NextPage`, where the
  merge behavior first takes effect. Incoming sibling values replace established
  siblings while only the selected collection accumulates.
- Operation dependencies and request context are captured by the narrow
  repository's source closures. Cross-repository inputs belong to use cases.

## Stabilize the atomic loading foundation

- [ ] Decide whether `DataBucket` and `IndexedBucket` improve annotations enough
  to remain public alongside `Bucket`.
- [ ] Review whether `ContinuumKeySpace`, `SingletonInput`, `BucketScope`,
  `PartitionedScope`, `BucketPartition`, `BucketConfiguration`, and
  `BucketBuilder` should remain ordinary public API or become advanced
  extension-facing surface.
- [ ] Strengthen the zero-or-one `RemoteSource` constraint beyond the current
  construction-time precondition if it can be expressed without harming
  progressive disclosure.
- [ ] Define cancellation behavior when one caller leaves a coalesced load.
- [ ] Define task lifetime when a repository or bucket is released.
- [ ] Decide whether `LoadState.error` should retain an existential error,
  expose a typed failure, or provide both.
- [x] Test cancellation, superseded generations, and reset during in-flight
  source and mutation work.
- [ ] Evaluate lookup performance for large indexed snapshots and replace the
  current array-backed implementation if measurements justify it.

## Extend durable local persistence

- [x] Define a writable local DSL capability without requiring repetitive
  repository forwarding methods.
- [x] Allow any number of local destinations to receive a snapshot.
- [x] Define atomic snapshot write ordering, partial-failure reporting, and
  rollback behavior when several destinations participate.
- [ ] Introduce storage-driver boundaries for memory, databases, files, and
  other resilient stores without exposing those choices to bucket consumers.
- [ ] Define serialization, schema-version, migration, and incompatible-data
  behavior using the key namespace and version.
- [x] Connect `store` and `remove` to configured persistence.
- [x] Keep `reset()` memory-only while offering a local-inclusive reset.
- [x] Keep asynchronous throwing mutation signatures for in-memory-only and
  persistent buckets.
- [ ] Add transaction or batch primitives where several repository snapshots
  must update atomically.

## Coordinate caches and remote data

- [x] Make remote snapshots automatically populate local destinations that
  explicitly declare `persist:`.
- [ ] Define promotion when a later local source succeeds, such as copying a
  database snapshot into an earlier memory cache.
- [ ] Add freshness metadata, expiry, and invalidation without making the basic
  bucket surface state-heavy.
- [ ] Define stale-while-revalidate and offline behavior while preserving
  `isLoaded` for an established stale snapshot.
- [x] Publish mutations optimistically before local persistence and declared
  remote mutation capabilities.
- [x] Preserve the accumulated pagination snapshot and continuation checkpoint
  when storing or removing individual values.
- [x] Serialize explicit mutations across remote, local, and memory work.
- [ ] Define interaction between active read flights and explicit remote
  mutations beyond the current supersession behavior.

## First-class pagination

The basic `RemoteSource { ... }` form remains unchanged. A bucket opts into
pagination by using the progressively disclosed remote-source builder:

```swift
Bucket(
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

`Load` establishes the cursor type and the initial atomic snapshot.
`NextPage` receives only the current cursor because the remote source already
captures the stable partition input. The selected partition owns the merged
snapshot and exposes `loadNext()`; transport response models remain inside the
source closures.

- [x] Add the advanced `RemoteSource` builder with `Load` and `NextPage`
  while preserving the single-operation form for non-paginated sources.
- [x] Introduce a typed `Page` result containing domain values and the next
  cursor.
- [x] Treat the first successful remote page as the atomic initial snapshot.
- [x] Add `loadNext()` to a paginated bucket or selected partition.
- [x] Append new page indices in page order while replacing existing values
  without moving them.
- [x] Accumulate a nested indexed collection while using the incoming aggregate
  as the latest value for its sibling properties.
- [x] Expose page-specific state through `PaginationState`,
  `isLoadingNextPage`, `hasNextPage`, and `nextPageError` beside the bucket-wide
  initial `LoadState`; keep the transport cursor private to the source DSL.
- [x] Coalesce requests for the same next-page position and serialize initial,
  refresh, and next-page publication within one partition.
- [x] Allow different query partitions to load and paginate independently.
- [x] Define refresh as complete snapshot replacement and reset as returning to
  untouched state.
- [ ] Define offset, page-number, and cursor adapters without leaking transport
  response models into repository consumers.
- [x] Persist the complete merged snapshot rather than individual pages.
- [ ] Decide whether local persistence also restores the next-cursor checkpoint.
- [x] Prevent page results superseded by refresh or reset from entering the
  current snapshot.

## Support modular repositories and use cases

- [x] Document repositories as owners of narrow atomic domain snapshots.
- [x] Document use cases as the layer that coordinates repositories and creates
  display-ready models.
- [x] Support one flat partition scope whose source configuration lives in the
  owning feature module.
- [x] Document stable query partitions as an alternative to duplicating
  repositories or forwarding query-specific methods.
- [ ] Add a public extension point for new DSL building blocks so feature
  modules and companion packages can add capabilities without modifying
  Continuum.
- [ ] Verify that keys, repository factories, and bucket configuration can live
  in separate feature modules without a shared registry.
- [ ] Explore low-level concurrent load and transaction helpers only where they
  reduce orchestration boilerplate without moving presentation composition into
  Continuum.
- [ ] Define cancellation and error aggregation conventions for use cases that
  coordinate several repositories.

## Complete observation and lifecycle behavior

- [x] Make initial success, empty success, refreshing, and failed refresh
  representable without inferring state from the value.
- [x] Preserve established values and `isLoaded` after a failed refresh.
- [x] Provide explicit reset behavior.
- [x] Observe application invalidation sequences for each bucket or selected
  partition.
- [x] Cancel invalidation observation when its bucket partition is released.
- [ ] Decide whether stale state, last update time, or source provenance should
  become first-class.
- [ ] Audit actor isolation and `Sendable` behavior for database-backed and
  remote implementations.
- [x] Add diagnostics and tracing hooks without coupling consumers to a logging
  framework.

## Prepare the library for release

- [ ] Add DocC concept guides after persistence and pagination stabilize.
- [ ] Add a sample app covering atomic empty state, layered local sources,
  remote fallback, narrow repositories, composed use cases, persistence, and
  pagination.
- [ ] Add performance tests for large indexed snapshots and coalesced workloads.
- [ ] Validate the public API across all declared platforms.
- [ ] Establish semantic-versioning and migration expectations.
- [ ] Publish an initial release only after storage-driver and mutation
  contracts are stable.

## Open API decisions

- Should a hit from a later local source promote its snapshot into earlier
  writable local sources?
- How should durable storage encode partition values so namespaces remain stable
  across launches and schema migrations?
- Should lazily created partitions have explicit eviction or retention policy
  when applications use an unbounded query type?
- Should persisted pagination checkpoints be required for resumable
  `loadNext()` after restoring a merged local snapshot, or remain optional?
- Which builder and key-space types are intentional extension points, and which
  should disappear from normal documentation?

## Non-goals for the current direction

- A required global Continuum container.
- Independently loaded entries inside an `IndexedKey`.
- Entry-level loading state for an atomic indexed snapshot.
- Repeating the key in each DSL building block.
- More than one partition dimension or nested partition scopes.
- Replacing typed keys with open-ended resources.
- A separate mapping stage after a source.
- More than one remote source per bucket.
- Hidden time-based debounce behavior inside a load policy.
- Repositories that join unrelated domain data into screen-specific models.
