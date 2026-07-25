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

/// A typed key for one atomic, insertion-ordered collection.
///
/// Sources load the whole collection as `[Value]`. The index operation derives
/// each value's lookup key. Duplicate indices keep their first position and
/// their last value. Identifiable values use `id` by default; provide
/// `indexedBy:` for another identity.
///
/// ```swift
/// static let posts = IndexedKey<Post.ID, Post>("posts")
/// ```
nonisolated public struct IndexedKey<
    Index: Hashable & Sendable,
    Value: Sendable
>: ContinuumKeySpace {
    private struct SendableKeyPath<Root, Result>: @unchecked Sendable {
        let value: KeyPath<Root, Result>
    }

    /// The index type that selects one value.
    public typealias Input = Index

    /// The ordered collection loaded and published atomically.
    public typealias Snapshot = [Value]

    /// The stable namespace of this indexed key.
    public let namespace: String

    /// The schema version associated with this collection.
    public let version: Int

    private let indexOperation: @Sendable (Value) -> Index

    /// Creates a typed indexed key using an identifiable value's `id`.
    ///
    /// The ``Identifiable`` conformance must be nonisolated because buckets
    /// derive indices from sendable source results. In a target with default
    /// `MainActor` isolation, declare the domain value itself `nonisolated`.
    /// The namespace must not be empty, and the version must be greater than
    /// zero.
    ///
    /// - Parameters:
    ///   - namespace: A stable name for the complete collection.
    ///   - version: The schema version that participates in key identity. The
    ///     default is `1`.
    public init(
        _ namespace: String,
        version: Int = 1
    ) where Value: Identifiable, Value.ID == Index {
        self.init(
            namespace,
            version: version,
            indexedBy: \.id
        )
    }

    /// Creates a typed indexed key.
    ///
    /// The namespace must not be empty, and the version must be greater than
    /// zero.
    ///
    /// - Parameters:
    ///   - namespace: A stable name for the complete collection.
    ///   - version: The schema version that participates in key identity. The
    ///     default is `1`.
    ///   - indexOperation: An operation deriving each value's lookup index.
    public init(
        _ namespace: String,
        version: Int = 1,
        indexedBy indexOperation: @escaping @Sendable (Value) -> Index
    ) {
        precondition(namespace.isEmpty == false, "A Continuum key namespace cannot be empty.")
        precondition(version > 0, "A Continuum key version must be greater than zero.")
        self.namespace = namespace
        self.version = version
        self.indexOperation = indexOperation
    }

    /// Creates a typed indexed key using a key path.
    ///
    /// The namespace must not be empty, and the version must be greater than
    /// zero.
    ///
    /// - Parameters:
    ///   - namespace: A stable name for the complete collection.
    ///   - version: The schema version that participates in key identity. The
    ///     default is `1`.
    ///   - keyPath: The key path containing each value's lookup index.
    public init(
        _ namespace: String,
        version: Int = 1,
        indexedBy keyPath: KeyPath<Value, Index>
    ) {
        let keyPath = SendableKeyPath(value: keyPath)
        self.init(
            namespace,
            version: version,
            indexedBy: { value in
                value[keyPath: keyPath.value]
            }
        )
    }

    /// Returns the index derived from a value.
    ///
    /// - Parameter value: The value whose index should be derived.
    public func input(for value: Value) -> Index {
        indexOperation(value)
    }

    /// Removes duplicate indices while preserving first-insertion order.
    ///
    /// When an index appears more than once, its last value replaces the earlier
    /// value without moving the index.
    public func normalized(_ snapshot: [Value]) -> [Value] {
        var positions: [Index: Int] = [:]
        var result: [Value] = []

        for value in snapshot {
            let index = indexOperation(value)
            if let position = positions[index] {
                result[position] = value
            } else {
                positions[index] = result.endIndex
                result.append(value)
            }
        }

        return result
    }

    /// Returns the value matching one index.
    public func value(for input: Index, in snapshot: [Value]) -> Value? {
        snapshot.first { indexOperation($0) == input }
    }

    /// Returns the snapshot's indices in insertion order.
    public func inputs(in snapshot: [Value]) -> [Index] {
        snapshot.map(indexOperation)
    }

    /// Returns the supplied values in insertion order.
    public func values(in snapshot: [Value]) -> [Value] {
        snapshot
    }

    /// Inserts or replaces one value while preserving insertion order.
    public func storing(_ value: Value, in snapshot: [Value]?) -> [Value] {
        var result = normalized(snapshot ?? [])
        let index = indexOperation(value)

        if let position = result.firstIndex(where: { indexOperation($0) == index }) {
            result[position] = value
        } else {
            result.append(value)
        }

        return result
    }

    /// Removes one indexed value while preserving the remaining order.
    public func removing(
        _ input: Index,
        from snapshot: [Value]?
    ) -> [Value]? {
        guard var result = snapshot.map(normalized) else {
            return nil
        }
        result.removeAll { indexOperation($0) == input }
        return result
    }

    /// Compares the namespace and schema version that establish key identity.
    ///
    /// The index operation does not participate in identity.
    public static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.namespace == rhs.namespace && lhs.version == rhs.version
    }

    /// Hashes the namespace and schema version that establish key identity.
    public func hash(into hasher: inout Hasher) {
        hasher.combine(namespace)
        hasher.combine(version)
    }
}

/// An observable data bucket anchored to one ``IndexedKey``.
public typealias IndexedBucket<
    Index: Hashable & Sendable,
    Value: Sendable
> = Bucket<
    IndexedKey<Index, Value>,
    UnpartitionedBucketScope
>

/// A partitioned observable bucket whose partitions each own one atomic
/// ``IndexedKey`` snapshot.
public typealias PartitionedIndexedBucket<
    Partition: Hashable & Sendable,
    Index: Hashable & Sendable,
    Value: Sendable
> = PartitionedBucket<Partition, IndexedKey<Index, Value>>
