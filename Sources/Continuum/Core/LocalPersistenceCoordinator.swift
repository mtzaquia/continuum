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

private typealias PersistenceOperation<Space: ContinuumKeySpace> =
    @Sendable (Space.Snapshot?) async throws -> Void

/// Reserves local reads and writes in bucket-call order. The task chain, rather
/// than actor isolation alone, keeps suspended source operations serialized.
@MainActor
final class LocalPersistenceCoordinator<Space: ContinuumKeySpace> {
    private let operations: [PersistenceOperation<Space>]
    private let logIdentity: BucketLogIdentity
    private var tail: Task<Void, Never>?
    private var identifier: UInt = 0

    init(
        sources: [LocalSource<Space>],
        logIdentity: BucketLogIdentity
    ) {
        operations = sources.compactMap(\.persistence)
        self.logIdentity = logIdentity
    }

    func persist(
        _ snapshot: Space.Snapshot?,
        restoring: Bool = false
    ) async throws {
        let operations = operations
        let logIdentity = logIdentity
        try await enqueue(ignoringCancellation: restoring) {
            guard operations.isEmpty == false else { return }
            continuumDebug(
                .persistenceStarted(logIdentity, destinations: operations.count)
            )
            for operation in operations {
                try Task.checkCancellation()
                try await operation(snapshot)
            }
            try Task.checkCancellation()
            continuumDebug(
                .persistenceCompleted(logIdentity, destinations: operations.count)
            )
        }
    }

    func read(
        _ operation: @escaping @Sendable () async throws -> Space.Snapshot?
    ) async throws -> Space.Snapshot? {
        try await enqueue(operation: operation)
    }

    private func enqueue<Value: Sendable>(
        ignoringCancellation: Bool = false,
        operation: @escaping @Sendable () async throws -> Value
    ) async throws -> Value {
        let predecessor = tail
        identifier &+= 1
        let identifier = identifier
        // Local I/O has no main-actor work. Queue registration above is atomic
        // with bucket publication; the source closures execute off this actor.
        let task = Task { @concurrent in
            if let predecessor { await predecessor.value }
            try Task.checkCancellation()
            return try await operation()
        }
        tail = Task { @concurrent in
            _ = await task.result
        }
        defer {
            if self.identifier == identifier { tail = nil }
        }
        // Rollback must remain able to restore disk after caller cancellation.
        // It is still ordered before every subsequently submitted operation.
        if ignoringCancellation { return try await task.value }
        return try await withTaskCancellationHandler {
            try await task.value
        } onCancel: {
            task.cancel()
        }
    }
}
