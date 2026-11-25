# Coordinate overlapping operations

Each bucket, including each selected partition, owns its operation order. Memory publication happens on the main actor, and local I/O is serialized per bucket across suspension points.

| Incoming operation | Interaction with earlier work |
| --- | --- |
| `.cached` with fresh memory | Returns immediately, including optimistic values during a mutation. |
| `.cached` without fresh memory | Waits for pending mutations and resets, then rechecks memory and shares or starts a load. |
| `.cachedThenRemote` | Waits for pending mutations and resets, then supersedes active paging and shares or starts a load that reaches remote. A pending cached flight that returns a local hit can continue to remote for its waiting callers. |
| `.remote` | Waits for pending mutations and resets, then supersedes active reads. |
| `load(id:using:)` | A cached hit returns immediately. Fetches wait for mutations and initial loading. They share work per ID unless `.remote` replaces it. Reconciliation uses the mutation queue and supersedes active pages while preserving the cursor. |
| `loadNext()` | Waits for mutations, resets, and initial loading before capturing the snapshot to merge. Concurrent calls for the same cursor share work. |
| `mutate(_:)` | Waits for prior mutations, then supersedes active loads, entry fetches, and pages. |
| `reset()` | Clears memory and revokes ownership before suspending. Cancels reads and queued mutations. An endpoint already running can return to its caller, while any late edit loses ownership of the snapshot. |

## Finish persistence in order

A writable source can finish a write after cancellation. The bucket waits for it to return before the next local operation, so an old write finishes before reset clears storage. Source closures and mutation handlers must return or throw for queued work to advance. Awaiting another queued operation on the same bucket from one of those closures can deadlock. [Persistence](persistence.md#use-several-writable-destinations)

## Understand cancellation

A caller cancelling its wait leaves a shared snapshot or page flight running. Reset, mutation, or forced remote loading supersedes reads as described above. After reset, callers awaiting the earlier load receive `CancellationError`.

Cancelling a mutation or reset restores the previous state and attempts to restore storage while the operation still owns that state. Cleanup can finish after cancellation, and cancellation is thrown without becoming the bucket's `error`. An endpoint already in progress can still produce a server side effect.

[Loading snapshots](loading.md) · [Remote mutations](remote-mutations.md)
