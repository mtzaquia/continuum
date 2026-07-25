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

/// The current continuation state of one paginated bucket.
///
/// Initial and refresh work remains represented by ``LoadState``. This state
/// changes only while loading or resolving a page after the initial snapshot.
public struct PaginationState: Sendable {
    /// Whether the next page is currently loading.
    public let isLoading: Bool

    /// Whether the latest remote page supplied another cursor.
    public let hasNextPage: Bool

    /// The latest next-page error, or `nil` after success, refresh, or reset.
    public let error: (any Error)?

    init(
        isLoading: Bool = false,
        hasNextPage: Bool = false,
        error: (any Error)? = nil
    ) {
        self.isLoading = isLoading
        self.hasNextPage = hasNextPage
        self.error = error
    }
}
