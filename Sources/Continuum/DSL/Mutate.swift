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

/// Handles the typed operations submitted to a bucket's `mutate(_:)` method.
public struct Mutate<Operation: Sendable, Space: ContinuumKeySpace>: Sendable {
    let operation: @MainActor @Sendable (Operation, Mutation<Space>) async throws -> Void

    /// Creates a handler for one bucket or selected partition.
    /// The handler runs in the mutation queue and can edit the snapshot, call a
    /// service, or do both. A thrown error rolls back edits made by the handler.
    public init(
        _ operation: @escaping @MainActor @Sendable (Operation, Mutation<Space>) async throws -> Void
    ) {
        self.operation = operation
    }
}

/// A mutation closure accepted by a bucket without a custom operation type.
public typealias MutationAction<Space: ContinuumKeySpace> =
    @MainActor @Sendable (Mutation<Space>) async throws -> Void

/// Edits the snapshot owned by a running bucket mutation.
@MainActor
public final class Mutation<Space: ContinuumKeySpace> {
    private struct Edit<Response> {
        let snapshot: Space.Snapshot?
        var confirmed: ((Response) -> Space.Snapshot?)? = nil
    }

    private let keySpace: Space
    private let read: () -> Space.Snapshot?
    private let checkOwnership: () throws -> Void
    private let publish: (Space.Snapshot?) async throws -> Void
    private let remoteStarted: () -> Void
    private var isActive = true
    private var isUpdating = false

    init(
        keySpace: Space,
        read: @escaping () -> Space.Snapshot?,
        checkOwnership: @escaping () throws -> Void,
        publish: @escaping (Space.Snapshot?) async throws -> Void,
        remoteStarted: @escaping () -> Void
    ) {
        self.keySpace = keySpace
        self.read = read
        self.checkOwnership = checkOwnership
        self.publish = publish
        self.remoteStarted = remoteStarted
    }

    /// The complete snapshot, or `nil` while the bucket is unavailable.
    public var snapshot: Space.Snapshot? { isActive ? read() : nil }

    /// Edits an established snapshot and optionally awaits remote confirmation.
    /// The remote action still runs when the snapshot is unavailable. Return
    /// `nil` from the edit to clear the snapshot, or an empty collection to keep
    /// an established empty snapshot.
    @_disfavoredOverload
    public func update(
        _ edit: (Space.Snapshot) throws -> Space.Snapshot?,
        remotely: (() async throws -> Void)? = nil
    ) async throws {
        try await apply({ snapshot in
            guard let snapshot else { return nil }
            return Edit(snapshot: try edit(snapshot))
        }, remotely: remotely)
    }

    /// Publishes an optimistic edit, then replaces it with the server snapshot.
    /// An unavailable snapshot skips both publications and still runs the endpoint.
    public func update(
        _ edit: (Space.Snapshot) throws -> Space.Snapshot?,
        remotely: () async throws -> Space.Snapshot
    ) async throws {
        try await withoutActuallyEscaping(remotely) { remotely in
            try await apply({ snapshot in
                guard let snapshot else { return nil }
                return Edit(snapshot: try edit(snapshot), confirmed: { .some($0) })
            }, remotely: remotely)
        }
    }

    /// Explicitly establishes, replaces, or clears the complete snapshot.
    @_disfavoredOverload
    public func replace(
        with snapshot: Space.Snapshot?,
        remotely: (() async throws -> Void)? = nil
    ) async throws {
        try await apply({ _ in Edit(snapshot: snapshot) }, remotely: remotely)
    }

    /// Publishes the supplied snapshot, then replaces it with the server snapshot.
    public func replace(
        with snapshot: Space.Snapshot?,
        remotely: () async throws -> Space.Snapshot
    ) async throws {
        try await withoutActuallyEscaping(remotely) { remotely in
            try await apply({ _ in
                Edit(snapshot: snapshot, confirmed: { .some($0) })
            }, remotely: remotely)
        }
    }

    /// Edits one established indexed entry and optionally awaits confirmation.
    /// The remote action still runs if the entry or snapshot is unavailable.
    @_disfavoredOverload
    public func update<ID, Value>(
        id: ID,
        _ edit: (Value) throws -> Value,
        remotely: (() async throws -> Void)? = nil
    ) async throws where Space == IndexedKey<ID, Value> {
        try await apply({ snapshot in
            guard let snapshot, let value = keySpace.value(for: id, in: snapshot) else { return nil }
            let updated = try edit(value)
            return Edit(snapshot: replacing(id, with: updated, in: snapshot))
        }, remotely: remotely)
    }

    /// Edits one established indexed entry, then persists the server's value.
    /// The returned value may change the entry's index. Other entries retain
    /// their positions. An unavailable entry or snapshot skips both edits.
    public func update<ID, Value>(
        id: ID,
        _ edit: (Value) throws -> Value,
        remotely: () async throws -> Value
    ) async throws where Space == IndexedKey<ID, Value> {
        try await withoutActuallyEscaping(remotely) { remotely in
            try await apply({ snapshot in
                guard let snapshot, let value = keySpace.value(for: id, in: snapshot) else { return nil }
                let updated = try edit(value)
                let optimistic = replacing(id, with: updated, in: snapshot)
                let optimisticID = keySpace.input(for: updated)
                return Edit(snapshot: optimistic, confirmed: { confirmed in
                    self.replacing(optimisticID, with: confirmed, in: optimistic)
                })
            }, remotely: remotely)
        }
    }

    private func replacing(
        _ id: Space.Input, with value: Space.Value, in snapshot: Space.Snapshot
    ) -> Space.Snapshot {
        let base = keySpace.input(for: value) == id ? snapshot : keySpace.removing(id, from: snapshot)
        return keySpace.storing(value, in: base)
    }

    private func apply<Response>(
        _ prepare: (Space.Snapshot?) throws -> Edit<Response>?,
        remotely: (() async throws -> Response)?
    ) async throws {
        try requireActive()
        guard !isUpdating else { throw ContinuumError.concurrentMutationEdit }
        isUpdating = true
        defer { isUpdating = false }

        let edit = try prepare(read())
        if let edit { try await publish(edit.snapshot) }
        if let remotely {
            try requireActive()
            remoteStarted()
            let response = try await remotely()
            try requireActive()
            if let confirm = edit?.confirmed {
                try await publish(confirm(response))
            }
        }
    }

    private func requireActive() throws {
        guard isActive else { throw CancellationError() }
        try checkOwnership()
    }

    func finish() throws {
        invalidate()
        guard !isUpdating else { throw ContinuumError.concurrentMutationEdit }
    }

    func invalidate() { isActive = false }
}
