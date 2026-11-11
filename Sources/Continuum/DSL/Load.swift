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

/// Loads a complete remote snapshot or one indexed entry.
nonisolated public struct Load<Input: Sendable, Output: Sendable>: Sendable {
    let operation: @Sendable (Input) async throws -> Output

    /// Loads a complete snapshot or the initial page of a remote collection.
    public init(_ operation: @escaping @Sendable () async throws -> Output) where Input == Void {
        self.operation = { _ in try await operation() }
    }

    /// Loads an entry by its index. The returned entry must have that index.
    /// Existing entries are replaced in place. Missing entries are returned
    /// without insertion into the collection.
    public init(_ operation: @escaping @Sendable (Input) async throws -> Output) {
        self.operation = operation
    }
}
