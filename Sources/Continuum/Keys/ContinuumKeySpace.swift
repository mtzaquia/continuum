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

/// A typed namespace and atomic snapshot contract owned by a ``Bucket``.
///
/// ``Key`` uses its value as the snapshot. ``IndexedKey`` uses an ordered array
/// of values and derives each value's index.
nonisolated public protocol ContinuumKeySpace: Hashable, Sendable {
    /// The value that selects one entry in the snapshot.
    associatedtype Input: Hashable & Sendable

    /// The value stored at one selected entry.
    associatedtype Value: Sendable

    /// The value loaded and published atomically by the bucket.
    associatedtype Snapshot: Sendable

    /// The stable namespace of this key space.
    var namespace: String { get }

    /// The schema version that participates in this key space's identity.
    var version: Int { get }

    /// Returns the input derived from one value.
    ///
    /// - Parameter value: The value whose input should be derived.
    func input(for value: Value) -> Input

    /// Normalizes a snapshot before the bucket publishes it.
    ///
    /// - Parameter snapshot: The snapshot returned by a source or caller.
    /// - Returns: The snapshot in its stored ordering and uniqueness form.
    func normalized(_ snapshot: Snapshot) -> Snapshot

    /// Returns one value from a snapshot.
    ///
    /// - Parameters:
    ///   - input: The entry to find.
    ///   - snapshot: The snapshot to inspect.
    func value(for input: Input, in snapshot: Snapshot) -> Value?

    /// Returns the snapshot's inputs in storage order.
    ///
    /// - Parameter snapshot: The snapshot to inspect.
    func inputs(in snapshot: Snapshot) -> [Input]

    /// Returns the snapshot's values in storage order.
    ///
    /// - Parameter snapshot: The snapshot to inspect.
    func values(in snapshot: Snapshot) -> [Value]

    /// Returns a snapshot containing an inserted or replaced value.
    ///
    /// - Parameters:
    ///   - value: The value to insert or replace.
    ///   - snapshot: The current snapshot, or `nil` when none is established.
    func storing(_ value: Value, in snapshot: Snapshot?) -> Snapshot

    /// Returns a snapshot with one input removed.
    ///
    /// - Parameters:
    ///   - input: The entry to remove.
    ///   - snapshot: The current snapshot, or `nil` when none is established.
    /// - Returns: The resulting snapshot, or `nil` when no snapshot remains.
    func removing(_ input: Input, from snapshot: Snapshot?) -> Snapshot?
}

/// The internal entry selector used by a singleton ``Key``.
///
/// Callers do not construct this value. It appears publicly only as the input
/// witness that allows singleton and indexed keys to share one generic bucket.
nonisolated public struct SingletonInput: Hashable, Sendable {
    static let shared = SingletonInput()

    private init() {}
}
