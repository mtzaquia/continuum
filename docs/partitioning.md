# Partition a bucket

Partitioning lets one typed key own several independently loaded snapshots.
Use it for stable query variants that share a model and index shape, such as
buying and selling accounts.

## Introduce the partition once

Keep the key focused on snapshot shape and model identity:

```swift
import Continuum

nonisolated struct Account: Identifiable, Sendable {
  let id: Int
  let name: String
}

enum AccountsData {
  static let all = IndexedKey<Account.ID, Account>("accounts")
}
```

Pass the stable query type to `partitionedBy:`. The configuration receives the
partition value once; source operations capture that value and retain their
ordinary zero-argument shape:

```swift
import Observation

nonisolated enum Purpose: Hashable, Sendable {
  case buy
  case sell
}

@Observable
final class AccountsRepository {
  let accounts: PartitionedIndexedBucket<
    Purpose,
    Account.ID,
    Account
  >

  init(database: Database, client: APIClient) {
    accounts = Bucket(
      AccountsData.all,
      partitionedBy: Purpose.self
    ) { purpose in
      LocalSource {
        try await database.accounts(purpose: purpose)
      }
      RemoteSource {
        try await client.accounts(purpose: purpose)
      }
    }
  }
}
```

Partition values must be `Hashable` and `Sendable`. In a target whose default
isolation is `MainActor`, declaring a value-only partition type `nonisolated`
also keeps its `Hashable` conformance available to sendable source operations.

## Work through a selected partition

Subscript the outer bucket with a partition value before loading, reading, or
mutating:

```swift
let buying = repository.accounts[.buy]
let selling = repository.accounts[.sell]

try await buying.load()

buying.isLoaded
buying.values
buying[accountID]

try await buying.store(account)
try await selling.remove(accountID)
```

The first access creates a partition lazily. Repeated access with an equal value
returns the same observable `BucketPartition`, preserving its snapshot, loading
state, errors, and in-flight work.

Each partition is one atomic list:

- A successful empty `.buy` result makes `accounts[.buy].isLoaded` true without
  loading `.sell`.
- Cached loads without memory coalesce within the same partition; repeated
  remote loads are latest-wins.
- Different partitions can load concurrently.
- Store, remove, and reset affect only the selected partition.
- Indexed ordering and duplicate normalization apply independently inside each
  partition.
- Paginated partitions retain independent cursors, continuation state, and
  coalesced next-page work.

Loading policies also apply independently. A remote `.buy` load supersedes
active `.buy` work without cancelling `.sell`; see
[Loading snapshots](loading.md).

The outer bucket intentionally has no aggregate `isLoaded` or `values`.
Completeness is meaningful only for a selected partition.

## Keep partitioning flat

The initializer accepts one partition type, so a bucket has exactly one flat
partition dimension. Its `BucketBuilder` contains source and invalidation
capabilities rather than structural partition declarations. Use a single
`Hashable` query value when several request fields jointly identify a list:

```swift
nonisolated struct AccountsQuery: Hashable, Sendable {
  let purpose: Purpose
  let region: Region
  let sort: AccountSort
}
```

Authentication, tracing, and other request context that does not identify a
cached list remains captured by the repository's dependencies rather than
becoming part of the partition value.

Next: [Loading snapshots](loading.md) · [Paginating buckets](pagination.md) ·
[Repository composition](repository-composition.md) · [Roadmap](roadmap.md)
