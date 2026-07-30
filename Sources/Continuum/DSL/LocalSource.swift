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

/// A local loading capability for one atomic ``Bucket`` snapshot.
///
/// A bucket may contain any number of local sources. Cached loads try them in
/// declaration order until one returns a snapshot. Returning `nil` indicates a
/// cache miss; an empty indexed snapshot is a successful result.
/// ``LoadPolicy/cachedThenRemote`` publishes the first local hit before
/// continuing to the required remote source. Add a `persist:` closure to make
/// the same source a writable destination for normalized bucket snapshots.
/// Writable local sources run in declaration order. Explicit mutations publish
/// before persistence; a failure restores their prior snapshot. The first
/// persistence error stops the sequence without rolling back destinations that
/// already completed.
public struct LocalSource<Space: ContinuumKeySpace>: Sendable {
    let operation: @Sendable () async throws -> Space.Snapshot?
    let persistence:
        (@Sendable (Space.Snapshot?) async throws -> Void)?

    /// Creates a read-only local source.
    ///
    /// - Parameter operation: An operation returning the complete snapshot, or
    ///   `nil` when this source has no snapshot.
    public init(
        _ operation: @escaping @Sendable () async throws -> Space.Snapshot?
    ) {
        self.operation = operation
        persistence = nil
    }

    /// Creates a readable and writable local source.
    ///
    /// The persistence operation receives the bucket's normalized atomic state.
    /// `nil` means no snapshot is persisted; an empty indexed snapshot remains
    /// a present, successfully established value. Remote results and explicit
    /// bucket mutations write through to this operation. ``Bucket/reset()``
    /// also removes writable local snapshots.
    ///
    /// ```swift
    /// LocalSource {
    ///     try await database.posts()
    /// } persist: { posts in
    ///     try await database.replacePosts(with: posts)
    /// }
    /// ```
    ///
    /// - Parameters:
    ///   - operation: An operation returning the complete snapshot, or `nil`
    ///     when this source has no snapshot.
    ///   - persistence: An operation replacing or removing the complete
    ///     persisted snapshot.
    public init(
        _ operation: @escaping @Sendable () async throws -> Space.Snapshot?,
        persist persistence:
            @escaping @Sendable (Space.Snapshot?) async throws -> Void
    ) {
        self.operation = operation
        self.persistence = persistence
    }
}
