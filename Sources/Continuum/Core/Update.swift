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

import Observation
import os

/// The result or unavailability reported by an observable source.
nonisolated public enum Update<Snapshot: Sendable>: Sendable {
    /// An available snapshot or a failure that prevents a result.
    case result(Result<Snapshot, any Error>)

    /// The source has yet to produce a result or has cleared it.
    case reset
}

extension Update {
    var snapshot: Snapshot? {
        guard case .result(.success(let value)) = self else { return nil }
        return .some(value)
    }
}

/// The public observation and loading contract for inputs to a composition.
///
/// Buckets and compositions conform to this protocol. Apps and libraries can
/// pass their own observable sources to ``Input``. Each source owns its loading,
/// reset, and subscription lifecycle.
@MainActor
public protocol UpdateSource<Snapshot>: AnyObject, Observable {
    /// The value published when the source succeeds.
    associatedtype Snapshot: Sendable

    /// The current snapshot, failure, or unavailability.
    var latest: Update<Snapshot> { get }
    /// An observable operation error that can coexist with an available snapshot.
    ///
    /// The default is `nil`. Publish a failure that prevents an available
    /// snapshot through ``latest``.
    var error: (any Error)? { get }

    /// Loads or refreshes the source using the requested policy.
    ///
    /// The default for external sources is a no-op. Implement this method to
    /// participate in composition loads. Publish failures through ``latest``
    /// or ``error``.
    func load(using policy: LoadPolicy) async
}

public extension UpdateSource {
    var error: (any Error)? { nil }

    func load(using policy: LoadPolicy) async {
        if let source = self as? any LoadableSource {
            await SourceLoadContext.load(source, using: policy)
        }
    }
}

// Keeps invalidation visible when Observation coalesces a reset and its replacement.
@MainActor
protocol CompositionResetSource: AnyObject {
    var resetRevision: UInt { get }
}

/// A participant in a composition load.
@MainActor
protocol LoadableSource: AnyObject, Sendable {
    var loadIdentity: ObjectIdentifier { get }
    func load(in context: SourceLoadContext) async
}

extension LoadableSource {
    var loadIdentity: ObjectIdentifier { ObjectIdentifier(self) }
}

/// Shares dependent loads for the duration of a single top-level load.
final class SourceLoadContext: Sendable {
    private struct State: Sendable {
        var isCancelled = false
        var loads: [ObjectIdentifier: Task<Void, Never>] = [:]
    }

    let policy: LoadPolicy
    private let state = OSAllocatedUnfairLock(initialState: State())

    private init(policy: LoadPolicy) { self.policy = policy }

    @MainActor
    static func load(_ source: any LoadableSource, using policy: LoadPolicy) async {
        let context = Self(policy: policy)
        await withTaskCancellationHandler {
            await source.load(in: context)
        } onCancel: {
            context.cancel()
        }
    }

    @MainActor
    func loadAll(_ sources: [any LoadableSource]) async {
        await withTaskGroup(of: Void.self) { group in
            for source in sources {
                group.addTask { await self.load(source) }
            }
        }
    }

    @MainActor
    func load(_ source: any LoadableSource) async {
        guard !Task.isCancelled else { return }
        let id = source.loadIdentity
        let task = state.withLock { state -> Task<Void, Never>? in
            guard !state.isCancelled else { return nil }
            if let task = state.loads[id] { return task }
            let task = Task { @MainActor in
                guard !Task.isCancelled else { return }
                await source.load(in: self)
            }
            state.loads[id] = task
            return task
        }
        await task?.value
    }

    private func cancel() {
        // The top-level call owns shared work, even if an individual branch stops waiting.
        let tasks = state.withLock { state in
            state.isCancelled = true
            return state.loads.values
        }
        for task in tasks { task.cancel() }
    }
}

func makeSourceObservation<Source: UpdateSource & Sendable>(
    _ source: Source
) -> (stream: AsyncStream<Update<Source.Snapshot>>, cancel: @Sendable () -> Void) {
    let (stream, continuation) = AsyncStream<Update<Source.Snapshot>>.makeStream(
        bufferingPolicy: .unbounded
    )
    let producer = Task { @MainActor in
        let (changes, changeContinuation) = AsyncStream<UInt>.makeStream(
            bufferingPolicy: .unbounded
        )
        var changeIterator = changes.makeAsyncIterator()
        var observationID: UInt = 0
        var emittedResult = false

        @MainActor func read(_ id: UInt) -> Update<Source.Snapshot> {
            withObservationTracking {
                source.latest
            } onChange: {
                changeContinuation.yield(id)
            }
        }

        defer {
            changeContinuation.finish()
            continuation.finish()
        }

        while Task.isCancelled == false {
            observationID &+= 1
            var observedID = observationID
            var update = read(observedID)

            if case .reset = update, emittedResult {
                await Task.yield()
                observationID &+= 1
                observedID = observationID
                update = read(observedID)
            }

            switch update {
            case .result:
                emittedResult = true
                if case .terminated = continuation.yield(update) {
                    return
                }
            case .reset where emittedResult:
                emittedResult = false
                if case .terminated = continuation.yield(.reset) {
                    return
                }
            case .reset:
                break
            }

            var observedChange = false
            while let changedObservationID = await changeIterator.next() {
                if changedObservationID >= observedID {
                    observedChange = true
                    break
                }
            }
            guard observedChange else { return }
        }
    }
    continuation.onTermination = { @Sendable _ in
        producer.cancel()
    }
    return (stream, {
        continuation.finish()
        producer.cancel()
    })
}
