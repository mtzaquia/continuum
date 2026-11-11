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

/// Builds the ordered loading capabilities of an advanced ``RemoteSource``.
///
/// Declare a zero-argument ``Load`` first. If it returns a ``Page``, follow it
/// with ``NextPage``. An indexed bucket can end with a ``Load`` that takes an ID.
@resultBuilder
nonisolated public enum RemoteSourceBuilder<Space: ContinuumKeySpace> {
    /// Builds a source that loads a complete snapshot.
    public static func buildBlock(_ load: Load<Void, Space.Snapshot>) -> RemoteSource<Space> {
        RemoteSource(load: load)
    }

    /// Adds indexed entry loading to a complete snapshot source.
    public static func buildBlock<ID, Value>(
        _ load: Load<Void, Space.Snapshot>,
        _ entry: Load<ID, Value>
    ) -> RemoteSource<Space> where Space == IndexedKey<ID, Value> {
        RemoteSource(load: load, loadEntry: entry)
    }

    /// Builds a source that loads pages.
    public static func buildBlock<Cursor: Sendable>(
        _ load: Load<Void, Page<Space.Snapshot, Cursor>>,
        _ nextPage: NextPage<Space.Snapshot, Cursor>
    ) -> RemoteSource<Space> {
        RemoteSource(load: load, nextPage: nextPage)
    }

    /// Adds indexed entry loading to a paginated source.
    public static func buildBlock<Cursor: Sendable, ID, Value>(
        _ load: Load<Void, Page<Space.Snapshot, Cursor>>,
        _ nextPage: NextPage<Space.Snapshot, Cursor>,
        _ entry: Load<ID, Value>
    ) -> RemoteSource<Space> where Space == IndexedKey<ID, Value> {
        RemoteSource(load: load, nextPage: nextPage, loadEntry: entry)
    }
}
