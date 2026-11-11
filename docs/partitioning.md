# Partition a bucket

Use partitions when the same snapshot shape has independent identities, such as
account lists for different purposes or pages for different search queries.

```swift
nonisolated enum Purpose: Hashable, Sendable {
  case buy, sell
}

let accounts: PartitionedIdentifiedBucket<Purpose, Account> = Bucket(
  IndexedKey("accounts"),
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

Add `staleAfter: .seconds(300)` after `partitionedBy:` to give each partition
its own freshness interval. A refresh of `.buy` does not renew `.sell`.
[Snapshot freshness →](loading.md#refresh-stale-snapshots)

`PartitionedIdentifiedBucket<Purpose, Account>` infers the index type from
`Account.ID`, and `IndexedKey("accounts")` uses the model's `id`. To select
entries by a string code, declare `PartitionedIndexedBucket<Purpose, String, Account>`
and pass `indexedBy: \.code` or a sendable closure. Stored key-path variables must
retain `& Sendable` in their declared type. For a singleton snapshot in each
partition, use `PartitionedValueBucket<Partition, Value>` with `Key`.

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

Use [entry loading](loading.md#load-one-indexed-entry) to fetch an individual
entity from an indexed collection. For
[relationship resolution](composition.md#resolve-relationships), pass the indexed
bucket or select a query partition explicitly; foreign keys identify entries.

## Bound partition identities

The outer bucket retains every accessed partition until released. Reset clears
data without evicting partitions or stopping their invalidation subscriptions.
Prefer bounded identities, or scope repositories using arbitrary queries to a
session lifetime. [Ownership details →](resource-lifetime.md)

Next: [Loading](loading.md) · [Compositions](composition.md)
