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
/// while remote loads use it directly and supersede active source work. An
/// advanced source can declare a continuation and an entry loader.
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

    /// Loads an initial page and defines how to fetch and merge subsequent pages.
    ///
    /// - Parameters:
    ///   - firstPage: The operation that returns the initial page.
    ///   - nextPage: The continuation operation and merge behavior.
    ///   - loadEntry: An optional lookup for individual indexed entries.
    public init<Cursor: Sendable>(
        _ firstPage: @escaping @Sendable () async throws -> Page<Space.Snapshot, Cursor>,
        nextPage: NextPage<Space.Snapshot, Cursor>,
        loadEntry: (@Sendable (Space.Input) async throws -> Space.Value)? = nil
    ) {
        operation = {
            let page = try await firstPage()
            return remoteSnapshot(from: page, nextPage: nextPage)
        }
        isPaginated = true
        self.loadEntry = loadEntry
    }
}

nonisolated func remoteSnapshot<
    Space: ContinuumKeySpace,
    Cursor: Sendable
>(
    from page: Page<Space.Snapshot, Cursor>,
    nextPage: NextPage<Space.Snapshot, Cursor>
) -> RemoteSnapshot<Space> {
    let continuation = page.next.map { cursor in
        RemotePageContinuation<Space>(
            operation: {
                let page = try await nextPage.operation(cursor)
                return remoteSnapshot(
                    from: page,
                    nextPage: nextPage
                )
            },
            merge: nextPage.merge
        )
    }

    return RemoteSnapshot(
        snapshot: page.value,
        nextPage: continuation
    )
}
