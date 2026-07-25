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

/// One remote-page snapshot and the cursor that follows it.
///
/// Return `nil` from ``next`` when this page is the end of the remote
/// collection. Cursor values remain inside the remote-source configuration and
/// are not exposed by the bucket.
nonisolated public struct Page<
    Snapshot: Sendable,
    Cursor: Sendable
>: Sendable {
    /// The domain payload returned for this page.
    public let value: Snapshot

    /// The cursor for the following page, or `nil` at the end.
    public let next: Cursor?

    /// Creates a page containing a domain payload.
    ///
    /// - Parameters:
    ///   - value: The payload returned for this page.
    ///   - next: The cursor for the following page, or `nil` at the end.
    public init(value: Snapshot, next: Cursor?) {
        self.value = value
        self.next = next
    }
}

public extension Page {
    /// Creates a page of domain values.
    ///
    /// - Parameters:
    ///   - values: The values in remote page order.
    ///   - next: The cursor for the following page, or `nil` at the end.
    init<Element: Sendable>(
        values: [Element],
        next: Cursor?
    ) where Snapshot == [Element] {
        self.init(value: values, next: next)
    }
}
