# Coordinate overlapping operations

Each unpartitioned bucket or selected partition owns its operation ordering.
Memory publication happens on the main actor. Local I/O runs asynchronously,
with a queue that orders complete persistence sequences and local reads across
suspension points.

## Choose the replacement policy

| Incoming operation | Pending work |
| --- | --- |
| `mutate` | Waits for prior mutations, then supersedes active loads, entry fetches, and pages. |
| `reset` | Clears memory and revokes ownership from earlier work before suspending. Cancels reads and queued mutations. A running mutation can finish its request, while its edits lose ownership of the snapshot. |
| `.cached` with fresh memory | Returns the current snapshot immediately, including optimistic values. Without `staleAfter`, memory stays fresh indefinitely. |
| `.cached` without fresh memory | Waits for pending mutations/reset, rechecks memory, then shares or starts a load. Configured freshness requires remote validation of stale or unvalidated data. |
| `.cachedThenRemote` | Waits for pending mutations/reset, then shares or starts a load that reaches remote. |
| `.remote` | Waits for pending mutations and resets, then supersedes active reads and starts a fresh remote load. |
| `load(id:using:)` | Cached hits return immediately. Otherwise waits for mutations and initial loading, then shares work per ID unless `.remote` replaces it. Replacements use the mutation queue and cancel pending pages, preserving the cursor for retry. |
| `loadNext()` | Waits for mutations/reset and initial loading, then coalesces requests for the current cursor. |

A mutation checks ownership before publishing or persisting a server response.
A page captures its base after pending mutations finish.

A forced remote load retains the current snapshot while it waits and fetches.
After a reset, callers awaiting earlier reads receive `CancellationError` even
if a new load has already started.

## Finish persistence in order

Cancellation is cooperative. A writable source that is already executing may
still commit after cancellation. Continuum waits for it to return before
starting the next local operation. A successful reset therefore finishes after
older writes and cannot be undone by one of those writes finishing late.

Local source closures and mutation handlers must return or throw for queued
work to advance. Do not await another operation on the same bucket from inside
one of these closures. Different partitions have separate queues. Shared backing
stores must coordinate access from multiple partitions or direct callers.

Persistence ordering is not a transaction across destinations. See
[Persistence failures](persistence.md#use-several-writable-destinations).

## Cancel a mutation

When the caller cancels a mutation or reset, an operation that edited the
snapshot and still owns it restores the previous memory snapshot and attempts
to restore persistence. Restoration can run despite caller cancellation. The
call throws `CancellationError` after cleanup, without recording cancellation
as `error`.

Reset revokes ownership from the running mutation, so its late result can
neither write to the snapshot nor roll it back. An endpoint already in progress
can return success or failure to the mutation's caller. A paired endpoint is
skipped if reset revokes ownership before it starts. Explicit caller
cancellation still reaches the request and may leave its server outcome
unknown.

Load flights are shared work: cancellation of one awaiting load caller does not
cancel the underlying source flight. Reset, mutation, or forced remote loading
supersedes reads according to the table above.

Next: [Loading snapshots](loading.md) · [Remote mutations](remote-mutations.md) ·
[Resource lifetime](resource-lifetime.md)
