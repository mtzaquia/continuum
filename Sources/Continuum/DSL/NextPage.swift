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

/// The continuation operation of a paginated ``RemoteSource``.
///
/// This operation receives the cursor returned by the previous ``Page``. Stable
/// query inputs remain captured by the surrounding repository or
/// partitioned bucket configuration. The continuation also defines how its
/// snapshot merges with the established snapshot.
nonisolated public struct NextPage<
    Snapshot: Sendable,
    Cursor: Sendable
>: Sendable {
    // Key paths are immutable after construction. These wrappers permit the
    // merge closure to carry them across isolation boundaries without exposing
    // unchecked sendability through the public API.
    private struct SendableKeyPath<Root, Value>: @unchecked Sendable {
        let value: KeyPath<Root, Value>
    }

    private struct SendableWritableKeyPath<Root, Value>: @unchecked Sendable {
        let value: WritableKeyPath<Root, Value>
    }

    let operation:
        @Sendable (Cursor) async throws -> Page<Snapshot, Cursor>
    let merge: @Sendable (Snapshot, Snapshot) -> Snapshot

    /// Creates a continuation operation that appends a page of values.
    ///
    /// Existing indices retain their position and values from the new page
    /// replace duplicates when the bucket normalizes the merged snapshot.
    ///
    /// - Parameter operation: An operation receiving the current cursor and
    ///   returning the next page.
    public init<Element: Sendable>(
        _ operation: @escaping @Sendable (Cursor) async throws
            -> Page<Snapshot, Cursor>
    ) where Snapshot == [Element] {
        self.operation = operation
        merge = { current, incoming in
            current + incoming
        }
    }

    /// Creates a continuation operation that accumulates one nested collection.
    ///
    /// Each incoming snapshot becomes the new base, so sibling properties use
    /// their latest remote values. Values at `collection` merge with the
    /// established collection in insertion order. Duplicate indices retain
    /// their first position and use their latest value.
    ///
    /// - Parameters:
    ///   - collection: The nested collection that accumulates across pages.
    ///   - index: The stable index of each accumulated value.
    ///   - operation: An operation receiving the current cursor and returning
    ///     the next complete snapshot.
    public init<Element: Sendable, Index: Hashable & Sendable>(
        accumulating collection: WritableKeyPath<Snapshot, [Element]>,
        indexedBy index: KeyPath<Element, Index>,
        _ operation: @escaping @Sendable (Cursor) async throws
            -> Page<Snapshot, Cursor>
    ) {
        let collection = SendableWritableKeyPath(value: collection)
        let index = SendableKeyPath(value: index)
        self.operation = operation
        merge = { current, incoming in
            var result = incoming
            result[keyPath: collection.value] = mergedValues(
                current[keyPath: collection.value],
                incoming[keyPath: collection.value],
                indexedBy: index.value
            )
            return result
        }
    }
}

nonisolated private func mergedValues<
    Element: Sendable,
    Index: Hashable & Sendable
>(
    _ current: [Element],
    _ incoming: [Element],
    indexedBy index: KeyPath<Element, Index>
) -> [Element] {
    var positions: [Index: Int] = [:]
    var result: [Element] = []

    for value in current + incoming {
        let valueIndex = value[keyPath: index]
        if let position = positions[valueIndex] {
            result[position] = value
        } else {
            positions[valueIndex] = result.endIndex
            result.append(value)
        }
    }

    return result
}
