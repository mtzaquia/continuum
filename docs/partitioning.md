# Partition a bucket

Use `PartitionedBucket` when the same snapshot shape has independent identities. For example, each account can have its own ordered list of posts.

```swift
let postsByAccount: PartitionedIdentifiedBucket<Account.ID, Post> = PartitionedBucket(
  IndexedKey("account-posts"),
  partitionedBy: Account.ID.self
) { accountID in
  RemoteSource { try await client.posts(accountID: accountID) }
}

let posts = postsByAccount[accountID]
try await posts.load()
```

Subscripting creates a `Bucket` on first access and reuses it for equal keys. Load, mutate, reset, paginate, or pass that selected bucket to `Input`. Each bucket owns its own snapshot, error, freshness, in-flight work, and pagination cursor. Configure local storage with the partition key if snapshots need distinct destinations.

Partition keys must be `Hashable & Sendable`. Combine fields in a value type when several fields identify one snapshot. The container retains every accessed bucket. For unbounded keys such as search text, scope the container to a session. [Resource lifetime](resource-lifetime.md)

## Choose a bucket type

| Snapshot | Standalone | Partitioned |
| --- | --- | --- |
| One value (`Key`) | `ValueBucket<Value>` | `PartitionedValueBucket<Partition, Value>` |
| Collection indexed by `Identifiable.id` | `IdentifiedBucket<Value>` | `PartitionedIdentifiedBucket<Partition, Value>` |
| Collection with a custom index | `IndexedBucket<ID, Value>` | `PartitionedIndexedBucket<Partition, ID, Value>` |

For a custom index, pass `indexedBy:` with a sendable key path or closure. When storing a key path separately, preserve its `& Sendable` type. For buckets with typed operations, use the [mutation aliases](remote-mutations.md#annotate-a-typed-bucket).
