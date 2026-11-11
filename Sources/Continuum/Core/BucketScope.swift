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

/// The scope dimension owned by a ``Bucket``.
///
/// Ordinary callers rely on type inference and the bucket type aliases rather
/// than declaring scope types directly.
nonisolated public protocol BucketScope {
    /// The value selecting one independently loaded partition.
    associatedtype Partition: Hashable & Sendable
}

/// The scope used by a bucket that owns one unpartitioned snapshot.
nonisolated public enum UnpartitionedBucketScope: BucketScope {
    /// The internal partition witness for the single snapshot.
    public typealias Partition = SingletonInput
}

/// A bucket scope that exposes independently loaded partitions.
nonisolated public protocol PartitionedBucketScope: BucketScope {}

/// The scope used by a bucket partitioned by `Partition`.
///
/// Callers create this scope through
/// ``Bucket/init(_:partitionedBy:staleAfter:configuration:)``.
nonisolated public enum PartitionedScope<
    Value: Hashable & Sendable
>: PartitionedBucketScope {
    /// The stable value that selects one partition.
    public typealias Partition = Value
}

/// An observable data bucket containing independently loaded partitions.
public typealias PartitionedBucket<
    Partition: Hashable & Sendable,
    Space: ContinuumKeySpace
> = Bucket<Space, PartitionedScope<Partition>, MutationAction<Space>>

/// An unpartitioned bucket whose snapshot is one value.
public typealias ValueBucket<Value: Sendable> =
    Bucket<Key<Value>, UnpartitionedBucketScope, MutationAction<Key<Value>>>

/// A bucket whose partitions each own one value.
public typealias PartitionedValueBucket<
    Partition: Hashable & Sendable,
    Value: Sendable
> = PartitionedBucket<Partition, Key<Value>>

/// An unpartitioned collection bucket whose index type is `Value.ID`.
public typealias IdentifiedBucket<Value: Identifiable & Sendable> =
    IndexedBucket<Value.ID, Value> where Value.ID: Hashable & Sendable

/// A bucket whose partitions each own a collection with index type `Value.ID`.
public typealias PartitionedIdentifiedBucket<
    Partition: Hashable & Sendable,
    Value: Identifiable & Sendable
> = PartitionedIndexedBucket<Partition, Value.ID, Value> where Value.ID: Hashable & Sendable

/// An unpartitioned bucket with a typed mutation operation.
public typealias MutableBucket<Space: ContinuumKeySpace, Operation: Sendable> =
    Bucket<Space, UnpartitionedBucketScope, Operation>

/// A bucket whose selected partitions accept a typed mutation operation.
public typealias PartitionedMutableBucket<
    Partition: Hashable & Sendable,
    Space: ContinuumKeySpace,
    Operation: Sendable
> = Bucket<Space, PartitionedScope<Partition>, Operation>

/// An unpartitioned single-value bucket with a typed mutation operation.
public typealias MutableValueBucket<Value: Sendable, Operation: Sendable> =
    MutableBucket<Key<Value>, Operation>

/// A bucket whose partitions each own one value and accept a typed mutation operation.
public typealias PartitionedMutableValueBucket<
    Partition: Hashable & Sendable,
    Value: Sendable,
    Operation: Sendable
> = PartitionedMutableBucket<Partition, Key<Value>, Operation>

/// An unpartitioned collection bucket with a typed mutation operation and `Value.ID` as its index.
public typealias MutableIdentifiedBucket<Value: Identifiable & Sendable, Operation: Sendable> =
    MutableIndexedBucket<Value.ID, Value, Operation> where Value.ID: Hashable & Sendable

/// A bucket whose partitions each own an identified collection and accept a typed mutation operation.
public typealias PartitionedMutableIdentifiedBucket<
    Partition: Hashable & Sendable,
    Value: Identifiable & Sendable,
    Operation: Sendable
> = PartitionedMutableIndexedBucket<Partition, Value.ID, Value, Operation> where Value.ID: Hashable & Sendable
