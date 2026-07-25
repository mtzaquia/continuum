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
/// local sources miss. Cached-then-remote loads require and always reach it,
/// while remote loads use it directly and supersede active source work. A
/// source can progressively disclose pagination or mutations by declaring
/// ``Load``, ``NextPage``, ``Store``, and ``Remove`` members.
/// Successful remote snapshots write through to every writable ``LocalSource``
/// before entering observable memory.
nonisolated public struct RemoteSource<
    Space: ContinuumKeySpace
>: Sendable {
    let operation: @Sendable () async throws -> RemoteSnapshot<Space>
    let isPaginated: Bool
    let store: Store<Space.Value>?
    let remove: Remove<Space.Input>?

    /// Creates a remote source.
    ///
    /// - Parameter operation: An operation returning the complete snapshot.
    @_disfavoredOverload
    public init(
        _ operation: @escaping @Sendable () async throws -> Space.Snapshot
    ) {
        self.operation = {
            RemoteSnapshot(
                snapshot: try await operation(),
                nextPage: nil
            )
        }
        isPaginated = false
        store = nil
        remove = nil
    }

    /// Creates an advanced remote source.
    ///
    /// Declare one ``Load``, followed by an optional ``NextPage`` and optional
    /// ``Store`` and ``Remove`` capabilities:
    ///
    /// ```swift
    /// RemoteSource {
    ///     Load {
    ///         try await client.posts()
    ///     }
    ///     Store { post in
    ///         try await client.store(post)
    ///     }
    ///     Remove { id in
    ///         try await client.removePost(id: id)
    ///     }
    /// }
    /// ```
    ///
    /// - Parameter configuration: The ordered remote capabilities.
    public init(
        @RemoteSourceBuilder<Space>
        _ configuration: () -> RemoteSource<Space>
    ) {
        self = configuration()
    }

    init(
        load: Load<Space.Snapshot>,
        store: Store<Space.Value>? = nil,
        remove: Remove<Space.Input>? = nil
    ) {
        operation = {
            RemoteSnapshot(
                snapshot: try await load.operation(),
                nextPage: nil
            )
        }
        isPaginated = false
        self.store = store
        self.remove = remove
    }

    init<Cursor: Sendable>(
        load: Load<Page<Space.Snapshot, Cursor>>,
        nextPage: NextPage<Space.Snapshot, Cursor>,
        store: Store<Space.Value>? = nil,
        remove: Remove<Space.Input>? = nil
    ) {
        operation = {
            let page = try await load.operation()
            return remoteSnapshot(
                from: page,
                nextPage: nextPage
            )
        }
        isPaginated = true
        self.store = store
        self.remove = remove
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
