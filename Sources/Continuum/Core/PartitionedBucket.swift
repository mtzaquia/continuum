//
//  Copyright (c) 2026 @mtzaquia
//
//  Permission is hereby granted, free of charge, to any person obtaining a copy
//  of this software and associated documentation files (the "Software"), to deal
//  in the Software without restriction, including without limitation the rights
//  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//  copies of the Software, and to permit persons to whom the Software is
//  furnished to do so, subject to the following conditions:
//
//  The above copyright notice and this permission notice shall be included in all
//  copies or substantial portions of the Software.
//
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//  SOFTWARE.
//

/// Selects an independently loaded bucket for each partition key.
///
/// Select and configure buckets on the main actor. Each bucket owns its
/// snapshot, errors, and pagination.
@MainActor
public final class PartitionedBucket<Partition: Hashable & Sendable, Space: ContinuumKeySpace, Operation: Sendable> {
    /// The key space shared by the selected buckets.
    public let keySpace: Space
    private let staleAfter: Duration?
    private let configuration: @MainActor @Sendable (Partition) -> BucketConfiguration<Space, Operation>
    private var buckets: [Partition: Bucket<Space, Operation>] = [:]

    /// Creates a container that configures each bucket on first access.
    ///
    /// - Parameters:
    ///   - keySpace: The key space shared by the selected buckets.
    ///   - partitionedBy: The identity type used to select a bucket.
    ///   - staleAfter: The freshness interval tracked independently by each
    ///     bucket. Pass `nil` for unlimited reuse, or a nonnegative duration.
    ///   - configuration: Builds the sources and optional mutation handler
    ///     for the requested partition key.
    public init(
        _ keySpace: Space,
        partitionedBy _: Partition.Type,
        staleAfter: Duration? = nil,
        @BucketBuilder<Space> configuration:
            @escaping @MainActor @Sendable (Partition) -> BucketConfiguration<Space, Operation>
    ) {
        precondition(staleAfter.map { $0 >= .zero } ?? true, "staleAfter must be nonnegative.")
        self.keySpace = keySpace
        self.staleAfter = staleAfter
        self.configuration = configuration
    }

    /// Returns the bucket for this key, creating it on first access.
    ///
    /// Equal keys share the same bucket. The container retains every selected
    /// bucket until it is released.
    public subscript(partition: Partition) -> Bucket<Space, Operation> {
        if let bucket = buckets[partition] { return bucket }
        let bucket = Bucket(keySpace, staleAfter: staleAfter, configuration: configuration(partition))
        buckets[partition] = bucket
        return bucket
    }
}

/// An unpartitioned bucket whose snapshot is one value.
public typealias ValueBucket<Value: Sendable> =
    Bucket<Key<Value>, MutationAction<Key<Value>>>

/// A bucket whose partitions each own one value.
public typealias PartitionedValueBucket<
    Partition: Hashable & Sendable,
    Value: Sendable
> = PartitionedBucket<Partition, Key<Value>, MutationAction<Key<Value>>>

/// An unpartitioned collection bucket whose index type is `Value.ID`.
public typealias IdentifiedBucket<Value: Identifiable & Sendable> =
    IndexedBucket<Value.ID, Value> where Value.ID: Hashable & Sendable

/// A bucket whose partitions each own a collection with index type `Value.ID`.
public typealias PartitionedIdentifiedBucket<
    Partition: Hashable & Sendable,
    Value: Identifiable & Sendable
> = PartitionedIndexedBucket<Partition, Value.ID, Value> where Value.ID: Hashable & Sendable

/// An unpartitioned single-value bucket with a typed mutation operation.
public typealias MutableValueBucket<Value: Sendable, Operation: Sendable> =
    Bucket<Key<Value>, Operation>

/// A bucket whose partitions each own one value and accept a typed mutation operation.
public typealias PartitionedMutableValueBucket<
    Partition: Hashable & Sendable,
    Value: Sendable,
    Operation: Sendable
> = PartitionedBucket<Partition, Key<Value>, Operation>

/// An unpartitioned collection bucket with a typed mutation operation and `Value.ID` as its index.
public typealias MutableIdentifiedBucket<Value: Identifiable & Sendable, Operation: Sendable> =
    MutableIndexedBucket<Value.ID, Value, Operation> where Value.ID: Hashable & Sendable

/// A bucket whose partitions each own an identified collection and accept a typed mutation operation.
public typealias PartitionedMutableIdentifiedBucket<
    Partition: Hashable & Sendable,
    Value: Identifiable & Sendable,
    Operation: Sendable
> = PartitionedMutableIndexedBucket<Partition, Value.ID, Value, Operation> where Value.ID: Hashable & Sendable
