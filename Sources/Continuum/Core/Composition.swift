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

import Foundation
import Observation

@MainActor
struct CompositionInputState<Value: Sendable> {
    let value: Value?
    let unavailable: Bool
    let failures: [any Error]
    var resetRevision: UInt = 0
}

// Internal invalidation history survives observation coalescing across compositions.
@MainActor
protocol CompositionResetSource: AnyObject {
    var compositionResetRevision: UInt { get }
}

@MainActor
@Observable
final class CompositionLoad {
    var error: (any Error)?
    @ObservationIgnored private var currentTask: Task<Void, any Error>?
    let action: @MainActor (LoadPolicy) async throws -> Void
    let sourceReportsFailure: @MainActor () -> Bool

    init(
        _ action: @escaping @MainActor (LoadPolicy) async throws -> Void,
        sourceReportsFailure: @escaping @MainActor () -> Bool = { false }
    ) {
        self.action = action
        self.sourceReportsFailure = sourceReportsFailure
    }

    func run(_ policy: LoadPolicy) async {
        guard !Task.isCancelled else { return }
        error = nil
        let action = action
        let task = Task {
            try Task.checkCancellation()
            try await action(policy)
        }
        currentTask = task
        defer { if currentTask == task { currentTask = nil } }
        do {
            try await withTaskCancellationHandler { try await task.value } onCancel: { task.cancel() }
        } catch {
            guard currentTask == task, !Task.isCancelled,
                  !(error is CancellationError) else { return }
            if !sourceReportsFailure() { self.error = error }
        }
    }
}

@MainActor
struct CompositionEvaluation<Output: Sendable> {
    let unavailable: [Bool]
    let result: Result<Output, any Error>?
    var resetRevisions: [UInt] = []
}

/// A shared live composition whose outcomes can be observed or iterated.
///
/// Each iterator independently receives the current result followed by updates.
/// Initial unavailability is silent. Required reset transitions clear prior
/// outcomes before any accompanying failure. Errors never end observation.
/// Delivery is unbounded and changes may coalesce; this is not a mutation log.
/// Keep this object alive to observe its properties. Iterators retain it until
/// released; cancelling one iterator does not stop other consumers.
///
/// Use `Input(composition)` as an input to another composition. Required
/// reset transitions propagate even when observation coalesces a reset and its
/// following result. Optional inputs consume that invalidation as `nil`.
@MainActor
@Observable
public final class Composition<Output: Sendable>: AsyncSequence {
    /// The result or reset delivered to each subscriber.
    public typealias Element = Update<Output>

    /// The current outcome, initially `.reset`. Loading does not clear it.
    public private(set) var latest: Element = .reset

    /// Whether any composition load call is still executing its actions.
    public var isLoading: Bool { activeLoads > 0 }

    private(set) var compositionResetRevision: UInt = 0
    private var activeLoads = 0
    @ObservationIgnored private let evaluate: @MainActor () -> CompositionEvaluation<Output>
    @ObservationIgnored private let loads: [@MainActor @Sendable (LoadPolicy) async -> Void]
    @ObservationIgnored private var subscribers: [UUID: AsyncStream<Element>.Continuation] = [:]
    @ObservationIgnored private var previousUnavailable: [Bool] = []
    @ObservationIgnored private var previousResetRevisions: [UInt] = []
    @ObservationIgnored private var hasResult = false
    @ObservationIgnored private let observation = ObservationSubscription()

    init(
        loads: [@MainActor @Sendable (LoadPolicy) async -> Void],
        evaluate: @escaping @MainActor () -> CompositionEvaluation<Output>
    ) {
        self.loads = loads
        self.evaluate = evaluate
        recompute()
    }

    /// Runs all configured loading actions concurrently and waits for completion.
    ///
    /// Required action failures are published as outcomes, rather than thrown;
    /// optional failures do not fail the composition. Siblings continue after a
    /// failure. Cancellation propagates to actions and is not published as an
    /// error. Overlapping calls run independently; the newest invocation of each
    /// action owns its error state. Bucket loading policies still govern source
    /// work. Duplicate declarations invoke their actions independently.
    public func load(using policy: LoadPolicy = .cached) async {
        activeLoads += 1
        defer { activeLoads -= 1 }
        await withTaskGroup(of: Void.self) { group in
            for action in loads {
                group.addTask { await action(policy) }
            }
        }
        recompute()
    }

    private func recompute() {
        let state = observation.track { evaluate() } onChange: { [weak self] in
            self?.recompute()
        }
        let reset = zip(state.unavailable, previousUnavailable).contains { $0 && !$1 }
        let nestedReset = zip(state.resetRevisions, previousResetRevisions).contains { $0 != $1 }
        previousUnavailable = state.unavailable
        previousResetRevisions = state.resetRevisions
        if hasResult && (reset || nestedReset || state.result == nil) {
            publish(.reset)
            hasResult = false
        }
        if let result = state.result {
            publish(.result(result))
            hasResult = true
        }
    }

    private func publish(_ update: Element) {
        if case .reset = update { compositionResetRevision &+= 1 }
        latest = update
        for continuation in subscribers.values { continuation.yield(update) }
    }

    /// Creates an independent subscription with unbounded buffering.
    nonisolated public func makeAsyncIterator() -> AsyncIterator {
        let id = UUID()
        let (stream, continuation) = AsyncStream<Element>.makeStream()
        let registration = Task { @MainActor in
            guard !Task.isCancelled else { continuation.finish(); return }
            subscribers[id] = continuation
            if case .result = latest { continuation.yield(latest) }
        }
        continuation.onTermination = { [weak self] _ in
            registration.cancel()
            Task { @MainActor [weak self] in self?.subscribers[id] = nil }
        }
        return AsyncIterator(iterator: stream.makeAsyncIterator(), lifetime: BucketObservationLifetime {
            continuation.finish()
        }, owner: self)
    }

    /// Iterates outcomes while retaining the composition and its subscription.
    nonisolated public struct AsyncIterator: AsyncIteratorProtocol {
        private var iterator: AsyncStream<Element>.Iterator
        private let lifetime: BucketObservationLifetime
        private let owner: Composition

        fileprivate nonisolated init(
            iterator: AsyncStream<Element>.Iterator,
            lifetime: BucketObservationLifetime,
            owner: Composition
        ) {
            self.iterator = iterator
            self.lifetime = lifetime
            self.owner = owner
        }

        /// Waits for the next outcome, or returns `nil` after cancellation.
        @concurrent public mutating func next() async -> Element? {
            await iterator.next()
        }
    }
}

final class BucketObservationLifetime: Sendable {
    let finish: @Sendable () -> Void
    init(_ finish: @escaping @Sendable () -> Void) { self.finish = finish }
    deinit { finish() }
}


extension Composition: UpdateSource, CompositionResetSource {}

public extension Composition {
    /// Creates a live, observable and asynchronously iterable snapshot composition.
    ///
    /// Required failures fail the combined outcome; optional failures supply `nil`.
    /// A thrown transform error also fails the outcome. Loading is explicit through
    /// ``Composition/load(using:)``. Conditions select dependencies once.
    ///
    /// Construction starts observation immediately. Ordinary input loading
    /// actions remain explicit; relationship inputs resolve keys once their
    /// root is available, using the cached policy outside explicit loads.
    ///
    /// - Parameters:
    ///   - inputs: The snapshot and observable dependencies in transform order.
    ///   - mapFailures: Combines current required failures in declaration order.
    ///   - transform: Produces an output when all required inputs are available.
    convenience init<each Value: Sendable>(
        @CompositionBuilder _ inputs: () -> CompositionInputs<repeat each Value>,
        mapFailures: @escaping @MainActor ([any Error]) -> any Error = { $0[0] },
        transform: @escaping @MainActor (repeat each Value) throws -> Output
    ) {
        let inputs = inputs().makeInputs()
        var loads: [@MainActor @Sendable (LoadPolicy) async -> Void] = []
        for input in repeat each inputs {
            if let load = input.load { loads.append(load) }
        }
        self.init(loads: loads) {
            let states = (repeat (each inputs).read())
            var unavailable: [Bool] = []
            var failures: [any Error] = []
            var resetRevisions: [UInt] = []
            for state in repeat each states {
                unavailable.append(state.unavailable)
                resetRevisions.append(state.resetRevision)
                failures.append(contentsOf: state.failures)
            }
            if !failures.isEmpty {
                return .init(unavailable: unavailable, result: .failure(mapFailures(failures)),
                             resetRevisions: resetRevisions)
            }
            if unavailable.contains(true) {
                return .init(unavailable: unavailable, result: nil, resetRevisions: resetRevisions)
            }
            do {
                return .init(unavailable: unavailable,
                             result: .success(try transform(repeat (each states).value!)),
                             resetRevisions: resetRevisions)
            } catch {
                return .init(unavailable: unavailable, result: .failure(error),
                             resetRevisions: resetRevisions)
            }
        }
    }
}
