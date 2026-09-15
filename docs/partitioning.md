# Partition a bucket

Use partitions when the same snapshot shape has independent identities, such as
accounts for different purposes or an author selected by ID.

```swift
nonisolated enum Purpose: Hashable, Sendable {
  case buy, sell
}

let accounts = Bucket(
  IndexedKey<Account.ID, Account>("accounts"),
  partitionedBy: Purpose.self
) { purpose in
  LocalSource { try await database.accounts(purpose: purpose) }
  RemoteSource { try await client.accounts(purpose: purpose) }
}

let buying = accounts[.buy]
try await buying.load()
print(buying.values)
```

The first access creates a `BucketPartition`. Equal keys return the same
partition. Each owns its values, errors, source work, persistence, and pagination.
A load or reset of `.buy` does not affect `.sell`.

`IndexedKey` uses an identifiable model's `id`. For a different index, pass
`indexedBy: \.code` or a sendable closure. Stored key-path variables must retain
`& Sendable` in their declared type.

Partition keys must be `Hashable & Sendable`. Use a single query struct when
several fields identify a snapshot; authentication or tracing context belongs
in the captured dependencies. Value-only domain types should be `nonisolated`
in projects with default main-actor isolation.

## Observe and compose selected partitions

```swift
for await update in accounts[.buy] { /* Handle Update<[Account]>. */ }

let comparison = Composition {
  Input(accounts[.buy])
  Input(accounts[.sell]).optional()
} transform: { buying, selling in
  buying.count + (selling?.count ?? 0)
}
```

Select a partition before reading `latest` or iterating: the outer bucket has no
combined outcome. The example inputs observe only; attach loading closures to
make them participate in composition loads.

For partitions selected by entries in another collection, use
`Input(posts).resolving(\.authorID, from: authors)`. Resolved snapshots can be
single values or collections. [Relationship resolution →](composition.md#resolve-relationships)

## Bound partition identities

The outer bucket retains every accessed partition until released. Reset clears
data without evicting partitions or stopping their invalidation subscriptions.
Prefer bounded identities, or scope repositories using arbitrary queries to a
session lifetime. [Ownership details →](resource-lifetime.md)

Next: [Loading](loading.md) · [Compositions](composition.md)
