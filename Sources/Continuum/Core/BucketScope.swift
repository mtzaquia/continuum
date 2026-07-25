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
/// ``Bucket/init(_:partitionedBy:configuration:)``.
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
> = Bucket<Space, PartitionedScope<Partition>>
