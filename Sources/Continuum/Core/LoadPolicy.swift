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

/// The way a load selects and refreshes an atomic snapshot.
public enum LoadPolicy: Sendable {
    /// Returns an established memory snapshot, then tries local sources before
    /// the remote source.
    ///
    /// An established memory snapshot returns immediately, including while
    /// refresh work is active. Without memory, concurrent cached loads share
    /// the same source work.
    case cached

    /// Publishes a cached snapshot when available, then always loads and
    /// publishes the remote snapshot.
    ///
    /// This policy returns after the remote phase completes. It requires a
    /// ``RemoteSource`` even when memory or a local source can provide a
    /// snapshot.
    case cachedThenRemote

    /// Supersedes active source work and loads directly from the remote source.
    ///
    /// This policy requires a ``RemoteSource``.
    /// Repeated remote loads use latest-wins behavior. Results and errors from
    /// superseded source work cannot replace the current snapshot or state.
    case remote
}
