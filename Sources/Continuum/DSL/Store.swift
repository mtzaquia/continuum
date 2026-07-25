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

/// The value-storing operation in an advanced ``RemoteSource``.
///
/// The operation receives the value submitted to ``Bucket/store(_:)`` and
/// returns the server-authoritative value that reconciles an already-published
/// optimistic snapshot.
nonisolated public struct Store<Value: Sendable>: Sendable {
    let operation:
        @Sendable (Value) async throws -> Value

    /// Creates a store operation returning the server-authoritative value.
    ///
    /// - Parameter operation: An operation storing a submitted value and
    ///   returning the value established by the remote system.
    public init(
        _ operation:
            @escaping @Sendable (Value) async throws -> Value
    ) {
        self.operation = operation
    }

    /// Creates a store operation that retains the submitted value.
    ///
    /// Use this form when the remote system acknowledges the mutation without
    /// returning a canonical model.
    ///
    /// - Parameter operation: An operation storing the submitted value.
    @_disfavoredOverload
    public init(
        _ operation:
            @escaping @Sendable (Value) async throws -> Void
    ) {
        self.operation = { value in
            try await operation(value)
            return value
        }
    }
}
