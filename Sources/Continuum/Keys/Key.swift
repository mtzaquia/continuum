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

/// A typed address for one atomic value.
///
/// The namespace and version define identity; `Value` prevents callers from
/// reading or writing another value type through the same declaration.
///
/// ```swift
/// static let session = Key<Session>("accounts.session")
/// ```
nonisolated public struct Key<Value: Sendable>: ContinuumKeySpace {
    /// The internal singleton input used by this key.
    public typealias Input = SingletonInput

    /// The complete value loaded and published for this key.
    public typealias Snapshot = Value

    /// The stable namespace of this key.
    public let namespace: String

    /// The schema version associated with the stored value.
    public let version: Int

    /// Creates a typed singleton key.
    ///
    /// The namespace must not be empty, and the version must be greater than
    /// zero.
    ///
    /// - Parameters:
    ///   - namespace: A stable name that remains unchanged across launches.
    ///   - version: The schema version that participates in key identity. The
    ///     default is `1`.
    public init(_ namespace: String, version: Int = 1) {
        precondition(namespace.isEmpty == false, "A Continuum key namespace cannot be empty.")
        precondition(version > 0, "A Continuum key version must be greater than zero.")
        self.namespace = namespace
        self.version = version
    }

    /// Returns the singleton input for a value.
    ///
    /// - Parameter value: The value being addressed.
    public func input(for value: Value) -> SingletonInput {
        .shared
    }

    /// Returns the supplied value unchanged.
    public func normalized(_ snapshot: Value) -> Value {
        snapshot
    }

    /// Returns the singleton value.
    public func value(
        for input: SingletonInput,
        in snapshot: Value
    ) -> Value? {
        .some(snapshot)
    }

    /// Returns the singleton input.
    public func inputs(in snapshot: Value) -> [SingletonInput] {
        [.shared]
    }

    /// Returns the singleton value as an ordered collection.
    public func values(in snapshot: Value) -> [Value] {
        [snapshot]
    }

    /// Replaces the singleton snapshot.
    public func storing(_ value: Value, in snapshot: Value?) -> Value {
        value
    }

    /// Removes the singleton snapshot.
    public func removing(
        _ input: SingletonInput,
        from snapshot: Value?
    ) -> Value? {
        nil
    }
}

/// An observable data bucket anchored to one singleton ``Key``.
public typealias DataBucket<Value: Sendable> = Bucket<
    Key<Value>,
    UnpartitionedBucketScope
>

/// A partitioned observable bucket whose partitions each own one singleton
/// ``Key`` snapshot.
public typealias PartitionedDataBucket<
    Partition: Hashable & Sendable,
    Value: Sendable
> = PartitionedBucket<Partition, Key<Value>>
