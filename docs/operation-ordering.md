# Coordinate overlapping operations

Each unpartitioned bucket or selected partition owns its operation ordering.
Memory publication happens on the main actor. Local I/O runs asynchronously,
with a queue that orders complete persistence sequences and local reads across
suspension points.

## Choose the replacement policy

| Incoming operation | Pending work |
| --- | --- |
| `store` or `remove` | Waits for prior mutations, then supersedes active loads and pages. |
| `reset` | Supersedes queued and running mutations, loads, and pages. Clears memory before awaiting local deletion. |
| `.cached` with memory | Returns the current snapshot immediately, including optimistic values. |
| `.cached` without memory | Waits for pending mutations/reset, rechecks memory, then shares or starts a load. |
| `.cachedThenRemote` | Waits for pending mutations/reset, then shares or starts a load that reaches remote. |
| `.remote` | Supersedes queued and running mutations, loads, and pages. |
| `loadNext()` | Waits for mutations/reset and initial loading, then coalesces requests for the current cursor. |

Store reconciliation checks that it still owns the state before publishing
or persisting the server-authoritative result. A page captures its base only
after pending mutations finish. These rules prevent an older store from
overwriting a refresh or dropping a newly published page.

A forced remote load retains the current memory snapshot while it runs. If
that snapshot was optimistic, it remains readable until the refresh resolves;
superseding a mutation does not confirm or undo its remote side effects.

## Finish persistence in order

Cancellation is cooperative. A writable source that is already executing may
still commit after cancellation. Continuum waits for it to return before
starting the next local operation. A successful reset therefore finishes after
older writes and cannot be undone by one of those writes finishing late.

Local source closures must eventually return or throw for queued work to
advance. Do not await another operation on the same bucket from inside one of
its source closures. Different partitions have separate queues; shared backing
stores must coordinate access from multiple partitions or direct callers.

Persistence ordering is not a transaction across destinations. See
[Persistence failures](persistence.md#use-several-writable-destinations).

## Cancel a mutation

When the caller cancels store, remove, or reset, the operation restores its
previous memory snapshot and attempts to restore persistence if it still owns
the state. Restoration can run despite caller cancellation. The call throws
`CancellationError` after cleanup, without recording cancellation as `error`.

If reset or a forced remote load has superseded the operation, its rollback is
skipped so it cannot overwrite the replacement. Cancellation cannot guarantee
that a remote server rejected a request it already received.

Load flights are shared work: cancellation of one awaiting load caller does not
cancel the underlying source flight. Reset, mutation, or forced remote loading
supersedes source work according to the table above.

## Use checked key paths

The key-path overloads of `IndexedKey` and nested `NextPage` accumulation now
require `Sendable` key paths. This is a source-compatibility change for callers
that erased that capability or used actor-isolated paths.

Ordinary property literals on nonisolated domain values keep the same syntax.
Preserve sendability when naming a key path:

```swift
nonisolated struct Post: Identifiable, Sendable {
  let id: Int
}

let identity: KeyPath<Post, Int> & Sendable = \.id
let posts = IndexedKey<Int, Post>("posts", indexedBy: identity)
```

Nested accumulation similarly accepts `WritableKeyPath<Snapshot, [Element]>
& Sendable` and `KeyPath<Element, Index> & Sendable`. In projects with default
main-actor isolation, mark value-only domain models `nonisolated`.

Key paths can capture subscript arguments. A mutable non-Sendable capture is
rejected even when the key-path object itself never changes. For computed
indexing, the existing `@Sendable` closure overload remains available.

Next: [Loading snapshots](loading.md) · [Remote mutations](remote-mutations.md) ·
[Resource lifetime](resource-lifetime.md)
