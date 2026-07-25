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

/// The current loading state of one atomic data bucket.
///
/// Loading, established-snapshot state, and the latest operation error are
/// independent. A refresh or local-inclusive reset can therefore fail while
/// ``isLoaded`` remains `true`.
public struct LoadState: Sendable {
    /// Whether source work is currently active.
    public let isLoading: Bool

    /// Whether the bucket has established a complete snapshot.
    ///
    /// A successfully loaded empty indexed snapshot sets this value to `true`.
    public let isLoaded: Bool

    /// The latest snapshot-loading, mutation, persistence, or invalidation error.
    ///
    /// Starting another load or mutation clears the previous error. A reset
    /// clears it on success. Continuation failures are reported separately by
    /// ``PaginationState``.
    public let error: (any Error)?

    init(
        isLoading: Bool = false,
        isLoaded: Bool = false,
        error: (any Error)? = nil
    ) {
        self.isLoading = isLoading
        self.isLoaded = isLoaded
        self.error = error
    }
}
