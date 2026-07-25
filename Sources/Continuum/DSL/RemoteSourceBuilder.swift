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

/// Builds the ordered capabilities of an advanced ``RemoteSource``.
///
/// A source declares exactly one ``Load``. A load returning ``Page`` must be
/// followed by one ``NextPage``. It may then declare at most one ``Store``
/// followed by at most one ``Remove``. The fixed block shapes reject duplicate,
/// reordered, or nested capabilities.
@resultBuilder
nonisolated public enum RemoteSourceBuilder<
    Space: ContinuumKeySpace
> {
    /// Builds a load-only advanced source.
    public static func buildBlock(
        _ load: Load<Space.Snapshot>
    ) -> RemoteSource<Space> {
        RemoteSource(load: load)
    }

    /// Adds a store capability to a load operation.
    public static func buildBlock(
        _ load: Load<Space.Snapshot>,
        _ store: Store<Space.Value>
    ) -> RemoteSource<Space> {
        RemoteSource(
            load: load,
            store: store
        )
    }

    /// Adds a remove capability to a load operation.
    public static func buildBlock(
        _ load: Load<Space.Snapshot>,
        _ remove: Remove<Space.Input>
    ) -> RemoteSource<Space> {
        RemoteSource(
            load: load,
            remove: remove
        )
    }

    /// Adds store and remove capabilities to a load operation.
    public static func buildBlock(
        _ load: Load<Space.Snapshot>,
        _ store: Store<Space.Value>,
        _ remove: Remove<Space.Input>
    ) -> RemoteSource<Space> {
        RemoteSource(
            load: load,
            store: store,
            remove: remove
        )
    }

    /// Builds a paginated load-only source.
    public static func buildBlock<Cursor: Sendable>(
        _ load: Load<Page<Space.Snapshot, Cursor>>,
        _ nextPage: NextPage<Space.Snapshot, Cursor>
    ) -> RemoteSource<Space> {
        RemoteSource(
            load: load,
            nextPage: nextPage
        )
    }

    /// Adds a store capability to a paginated source.
    public static func buildBlock<Cursor: Sendable>(
        _ load: Load<Page<Space.Snapshot, Cursor>>,
        _ nextPage: NextPage<Space.Snapshot, Cursor>,
        _ store: Store<Space.Value>
    ) -> RemoteSource<Space> {
        RemoteSource(
            load: load,
            nextPage: nextPage,
            store: store
        )
    }

    /// Adds a remove capability to a paginated source.
    public static func buildBlock<Cursor: Sendable>(
        _ load: Load<Page<Space.Snapshot, Cursor>>,
        _ nextPage: NextPage<Space.Snapshot, Cursor>,
        _ remove: Remove<Space.Input>
    ) -> RemoteSource<Space> {
        RemoteSource(
            load: load,
            nextPage: nextPage,
            remove: remove
        )
    }

    /// Adds store and remove capabilities to a paginated source.
    public static func buildBlock<Cursor: Sendable>(
        _ load: Load<Page<Space.Snapshot, Cursor>>,
        _ nextPage: NextPage<Space.Snapshot, Cursor>,
        _ store: Store<Space.Value>,
        _ remove: Remove<Space.Input>
    ) -> RemoteSource<Space> {
        RemoteSource(
            load: load,
            nextPage: nextPage,
            store: store,
            remove: remove
        )
    }
}
