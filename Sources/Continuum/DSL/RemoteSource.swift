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

nonisolated struct RemoteSnapshot<
    Space: ContinuumKeySpace
>: Sendable {
    let snapshot: Space.Snapshot
    let nextPage: RemotePageContinuation<Space>?
}

nonisolated struct RemotePageContinuation<
    Space: ContinuumKeySpace
>: Sendable {
    let operation: @Sendable () async throws -> RemoteSnapshot<Space>
    let merge:
        @Sendable (Space.Snapshot, Space.Snapshot) -> Space.Snapshot
}

/// The remote loading capability for one atomic ``Bucket`` snapshot.
///
/// A bucket accepts at most one remote source. Cached loads reach it after all
/// local sources miss, or when configured snapshot freshness requires remote
/// validation. Cached-then-remote loads require and always reach it,
/// while remote loads use it directly and supersede active source work.
/// Successful remote snapshots write through to every writable ``LocalSource``
/// before entering observable memory.
nonisolated public struct RemoteSource<
    Space: ContinuumKeySpace
>: Sendable {
    let operation: @Sendable () async throws -> RemoteSnapshot<Space>
    let isPaginated: Bool
    let loadEntry: (@Sendable (Space.Input) async throws -> Space.Value)?

    /// Loads a complete snapshot and optionally loads individual entries.
    ///
    /// - Parameters:
    ///   - load: The operation that returns the full snapshot.
    ///   - loadEntry: An indexed entry lookup. The returned entry must match its requested ID.
    public init(
        _ load: @escaping @Sendable () async throws -> Space.Snapshot,
        loadEntry: (@Sendable (Space.Input) async throws -> Space.Value)? = nil
    ) {
        operation = { RemoteSnapshot(snapshot: try await load(), nextPage: nil) }
        isPaginated = false
        self.loadEntry = loadEntry
    }

    /// Loads an initial array page and appends subsequent pages.
    ///
    /// Indexed buckets retain the first position of each index and use the
    /// latest value when subsequent pages include duplicates.
    ///
    /// - Parameters:
    ///   - firstPage: The operation that returns the initial page.
    ///   - loadEntry: An optional lookup for individual indexed entries.
    ///   - nextPage: The operation that fetches a page using the preceding cursor.
    public init<Element: Sendable, Cursor: Sendable>(
        _ firstPage: @escaping @Sendable () async throws -> Page<Space.Snapshot, Cursor>,
        loadEntry: (@Sendable (Space.Input) async throws -> Space.Value)? = nil,
        nextPage: @escaping @Sendable (Cursor) async throws -> Page<Space.Snapshot, Cursor>
    ) where Space.Snapshot == [Element] {
        self.init(firstPage: firstPage, loadEntry: loadEntry, nextPage: nextPage, merge: { $0 + $1 })
    }

    /// Loads pages and accumulates one nested collection within the snapshot.
    ///
    /// Each incoming page supplies the fields outside the collection. Within
    /// the collection, each index keeps its first position and latest value.
    ///
    /// - Parameters:
    ///   - collection: The writable path of the collection that accumulates across pages.
    ///   - index: The stable index of each accumulated entry.
    ///   - firstPage: The operation that returns the initial page.
    ///   - loadEntry: An optional lookup for individual indexed entries.
    ///   - nextPage: The operation that fetches a page using the preceding cursor.
    public init<Element: Sendable, Index: Hashable & Sendable, Cursor: Sendable>(
        accumulating collection: WritableKeyPath<Space.Snapshot, [Element]> & Sendable,
        indexedBy index: KeyPath<Element, Index> & Sendable,
        _ firstPage: @escaping @Sendable () async throws -> Page<Space.Snapshot, Cursor>,
        loadEntry: (@Sendable (Space.Input) async throws -> Space.Value)? = nil,
        nextPage: @escaping @Sendable (Cursor) async throws -> Page<Space.Snapshot, Cursor>
    ) {
        self.init(firstPage: firstPage, loadEntry: loadEntry, nextPage: nextPage) { current, incoming in
            var result = incoming
            result[keyPath: collection] = orderedUniqueValues(
                current[keyPath: collection] + incoming[keyPath: collection],
                indexedBy: { $0[keyPath: index] }
            )
            return result
        }
    }

    private init<Cursor: Sendable>(
        firstPage: @escaping @Sendable () async throws -> Page<Space.Snapshot, Cursor>,
        loadEntry: (@Sendable (Space.Input) async throws -> Space.Value)?,
        nextPage: @escaping @Sendable (Cursor) async throws -> Page<Space.Snapshot, Cursor>,
        merge: @escaping @Sendable (Space.Snapshot, Space.Snapshot) -> Space.Snapshot
    ) {
        operation = {
            let page = try await firstPage()
            return remoteSnapshot(from: page, nextPage: nextPage, merge: merge)
        }
        isPaginated = true
        self.loadEntry = loadEntry
    }
}

private nonisolated func remoteSnapshot<
    Space: ContinuumKeySpace,
    Cursor: Sendable
>(
    from page: Page<Space.Snapshot, Cursor>,
    nextPage: @escaping @Sendable (Cursor) async throws -> Page<Space.Snapshot, Cursor>,
    merge: @escaping @Sendable (Space.Snapshot, Space.Snapshot) -> Space.Snapshot
) -> RemoteSnapshot<Space> {
    let continuation = page.next.map { cursor in
        RemotePageContinuation<Space>(
            operation: {
                let page = try await nextPage(cursor)
                return remoteSnapshot(from: page, nextPage: nextPage, merge: merge)
            },
            merge: merge
        )
    }

    return RemoteSnapshot(
        snapshot: page.value,
        nextPage: continuation
    )
}
