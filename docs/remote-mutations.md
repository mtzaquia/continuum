# Mutate a bucket snapshot

Declare one `Mutate` handler beside the bucket's sources. The handler's first
parameter determines the operation type accepted by `mutate(_:)`. A
partitioned bucket can declare a handler for each selected partition.

```swift
enum CardOperation: Sendable {
  case rename(Card.ID, name: String)
  case terminate(Card.ID)
  case request(Product.ID)
}

let cards = Bucket(IndexedKey<Card.ID, Card>("cards")) {
  LocalSource {
    try await cache.cards()
  } persist: { snapshot in
    try await cache.replaceCards(with: snapshot)
  }

  RemoteSource { try await api.cards() }

  Mutate { (operation: CardOperation, mutation) in
    switch operation {
    case let .rename(id, name):
      try await mutation.update(id: id) { card in
        card.renamed(to: name)
      } remote: {
        .updated(try await api.renameCard(id: id, name: name))
      }

    case let .terminate(id):
      try await mutation.update { cards in
        cards.filter { $0.id != id }
      } remote: {
        try await api.terminateCard(id: id)
        return .acknowledged
      }

    case let .request(productID):
      try await api.requestCard(productID: productID)
    }
  }
}

try await cards.mutate(.rename(cardID, name: "Travel"))
```

The handler runs when its operation reaches the front of the mutation queue.
Continuum publishes the optimistic edit, passes the **complete normalized
snapshot** to each writable `LocalSource`, then runs the paired endpoint.
A failed local write prevents the endpoint from starting.

Call the endpoint in the `remote:` trailing closure. Return `.acknowledged`
to keep the optimistic edit, or `.updated(value)` to apply the server's
entry or whole snapshot. For an entry edit, the returned entry can change
its ID. Continuum publishes and persists the resulting complete snapshot.
Omit `remote:` for a local edit.

## Annotate a typed bucket

The builder infers the operation type from `Mutate`. Use the corresponding
alias when you declare a repository property or protocol requirement.

| Snapshot | Unpartitioned | Partitioned |
| --- | --- | --- |
| Single value | `MutableValueBucket<Value, Operation>` | `PartitionedMutableValueBucket<Partition, Value, Operation>` |
| Collection with a custom index | `MutableIndexedBucket<Index, Value, Operation>` | `PartitionedMutableIndexedBucket<Partition, Index, Value, Operation>` |
| Collection indexed by `Identifiable.ID` | `MutableIdentifiedBucket<Value, Operation>` | `PartitionedMutableIdentifiedBucket<Partition, Value, Operation>` |

For a custom key space, use `MutableBucket<Space, Operation>` or
`PartitionedMutableBucket<Partition, Space, Operation>`. Buckets without a
typed handler use the regular aliases, such as
`IdentifiedBucket<Card>` or `PartitionedIdentifiedBucket<ProfileID, Card>`.

## Mutate before the first load

`update` needs an established snapshot for the optimistic and authoritative
edits. An established empty array counts as a snapshot. With `update(id:)`,
the ID must also be present. The endpoint still runs on a cache miss. The
bucket skips publication and persistence for that edit.

A handler can call an endpoint directly, as in `.request`. This runs in the
mutation queue without touching the snapshot. An independent service call
can also run outside the bucket. When a requested card is created later, a
subsequent collection load can publish it.

Call `load()` to establish a snapshot before editing it. A `LocalSource`
can supply an initial snapshot for an in-memory bucket. A source returning
`[]` establishes an empty collection.

## Supply a mutation closure at the call site

Buckets created without a `Mutate` handler accept a closure through the same
method. The closure can edit an established snapshot and pair its edit with
a remote action.

```swift
let favorites = Bucket(IndexedKey<Card.ID, Card>("favorites")) {
  LocalSource { [firstFavorite] }
}
try await favorites.load()
try await favorites.mutate { mutation in
  try await mutation.update { $0.filter { $0.id != firstFavorite.id } }
}
```

## Understand failure and ordering

A thrown error or cancellation restores the previous snapshot and attempts to
restore persistence while the mutation still owns the bucket. Reset cancels
queued mutations and revokes ownership from a running handler. An in-progress
endpoint still returns its outcome to the caller, while a late response loses
the ability to publish or persist its result. Remote side effects already
accepted by the server can survive local rollback.

Forced remote loads, cached-then-remote loads, and `loadNext()` wait for
mutations. A snapshot edit preserves the pagination cursor. Keep edits
sequential within a handler, and use the mutation handle within that invocation.

[Operation ordering](operation-ordering.md) · [Persistence](persistence.md)
