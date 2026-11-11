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
    /// The successful outcome of the remote step paired with an optimistic edit.
    public enum RemoteOutcome<Value: Sendable>: Sendable {
        /// Keeps the optimistic edit.
        case acknowledged

        /// Applies the returned entry or snapshot and persists the resulting snapshot.
        case updated(Value)
    }

    private struct Edit<Response> {
        let snapshot: Space.Snapshot?
        let confirmed: (Response) -> Space.Snapshot?
    }

    private let keySpace: Space
    private let read: () -> Space.Snapshot?
    private let ownsState: () -> Bool
    private let publish: (Space.Snapshot?) async throws -> Void
    private let remoteStarted: () -> Void
    private var isActive = true
    private var isUpdating = false

    init(
        keySpace: Space,
        read: @escaping () -> Space.Snapshot?,
        ownsState: @escaping () -> Bool,
        publish: @escaping (Space.Snapshot?) async throws -> Void,
        remoteStarted: @escaping () -> Void
    ) {
        self.keySpace = keySpace
        self.read = read
        self.ownsState = ownsState
        self.publish = publish
        self.remoteStarted = remoteStarted
    }

    /// The complete snapshot, or `nil` while the bucket is unavailable.
    public var snapshot: Space.Snapshot? { isActive && ownsState() ? read() : nil }

    /// Edits an established snapshot and optionally runs a remote step.
    ///
    /// Return `nil` from the edit to clear the snapshot, or an empty collection
    /// to keep an established empty snapshot. The remote step runs after the
    /// optimistic write. Return `.acknowledged` to keep the edit or `.updated`
    /// with the snapshot returned by the server. An unavailable snapshot skips
    /// publication and still runs the remote step.
    public func update(
        _ edit: (Space.Snapshot) throws -> Space.Snapshot?,
        remote: (@MainActor () async throws -> RemoteOutcome<Space.Snapshot>)? = nil
    ) async throws {
        try await apply({ snapshot in
            guard let snapshot else { return nil }
            return Edit(snapshot: try edit(snapshot), confirmed: { .some($0) })
        }, remote: remote)
    }

    /// Edits one established indexed entry and optionally runs a remote step.
    ///
    /// Return `.acknowledged` from the remote step to keep the optimistic edit,
    /// or `.updated` to apply the server's entry, including a changed index.
    /// The remote step still runs if the entry or snapshot is unavailable.
    public func update<ID, Value>(
        id: ID,
        _ edit: (Value) throws -> Value,
        remote: (@MainActor () async throws -> RemoteOutcome<Value>)? = nil
    ) async throws where Space == IndexedKey<ID, Value> {
        try await apply({ snapshot in
            guard let snapshot, let value = keySpace.value(for: id, in: snapshot) else { return nil }
            let updated = try edit(value)
            let optimistic = replacing(id, with: updated, in: snapshot)
            let optimisticID = keySpace.input(for: updated)
            return Edit(snapshot: optimistic, confirmed: { confirmed in
                self.replacing(optimisticID, with: confirmed, in: optimistic)
            })
        }, remote: remote)
    }

    private func replacing(
        _ id: Space.Input, with value: Space.Value, in snapshot: Space.Snapshot
    ) -> Space.Snapshot {
        let base = keySpace.input(for: value) == id ? snapshot : keySpace.removing(id, from: snapshot)
        return keySpace.storing(value, in: base)
    }

    private func apply<Response>(
        _ prepare: (Space.Snapshot?) throws -> Edit<Response>?,
        remote: (@MainActor () async throws -> RemoteOutcome<Response>)?
    ) async throws {
        try requireActive()
        guard !isUpdating else { throw ContinuumError.concurrentMutationEdit }
        isUpdating = true
        defer { isUpdating = false }

        let edit = try prepare(read())
        if let edit {
            try await publish(edit.snapshot)
            try requireActive()
        }
        guard let remote else { return }
        try requireActive()
        remoteStarted()

        let outcome = try await remote()
        try Task.checkCancellation()
        // Reset revokes the snapshot edit while the endpoint's outcome still belongs to its caller.
        guard isActive, ownsState(), let edit, case let .updated(response) = outcome else { return }
        do {
            try await publish(edit.confirmed(response))
        } catch {
            try Task.checkCancellation()
            if ownsState() { throw error }
        }
    }

    private func requireActive() throws {
        try Task.checkCancellation()
        guard isActive, ownsState() else { throw CancellationError() }
    }

    func finish() throws {
        invalidate()
        guard !isUpdating else { throw ContinuumError.concurrentMutationEdit }
    }

    func invalidate() { isActive = false }
}
