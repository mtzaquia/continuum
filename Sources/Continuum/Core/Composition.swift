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
import os

/// The default aggregate for errors reported by a composition.
nonisolated public struct CompositionError: LocalizedError {
    /// The underlying errors in input declaration order.
    public let errors: [any Error]

    /// Creates an aggregate, expanding nested aggregates in place.
    public init(_ errors: [any Error]) {
        self.errors = errors.flatMap { ($0 as? CompositionError)?.errors ?? [$0] }
    }

    /// The description of the first underlying error.
    public var errorDescription: String? { errors.first?.localizedDescription }
}

@MainActor
struct CompositionInputState<Value: Sendable> {
    let value: Value?
    let unavailable: Bool
    let failures: [any Error]
    var resetRevision: UInt = 0
    // A separate read keeps error-only changes out of snapshot observation.
    var reportedFailures: (@MainActor () -> [any Error]?)? = nil
}

// Internal invalidation history survives observation coalescing across compositions.
@MainActor
protocol CompositionResetSource: AnyObject {
    var compositionResetRevision: UInt { get }
}

@MainActor
protocol CompositionInputSource<Snapshot>: UpdateSource, Sendable {
    var currentUpdate: Update<Snapshot> { get }
    var underlyingErrors: [any Error] { get }
    func activate(forLoad: Bool) -> (@MainActor () -> Void)?
    func load(in context: CompositionLoadContext) async
}

// The returned closure ends reuse of subscriptions started for the first load.
typealias InputActivation = @MainActor (_ forLoad: Bool) -> (@MainActor () -> Void)?
typealias InputLoad = @MainActor @Sendable (CompositionLoadContext) async -> Void

final class CompositionLoadContext: Sendable {
    private struct State: Sendable {
        var isCancelled = false
        var children: [ObjectIdentifier: Task<Void, Never>] = [:]
    }

    let policy: LoadPolicy
    private let state = OSAllocatedUnfairLock(initialState: State())

    init(policy: LoadPolicy) { self.policy = policy }

    @MainActor
    func load(_ child: any CompositionInputSource) async {
        guard !Task.isCancelled else { return }
        let id = ObjectIdentifier(child)
        let task = state.withLock { state -> Task<Void, Never>? in
            guard !state.isCancelled else { return nil }
            if let task = state.children[id] { return task }
            let task = Task { @MainActor in
                guard !Task.isCancelled else { return }
                await child.load(in: self)
            }
            state.children[id] = task
            return task
        }
        await task?.value
    }

    func cancel() {
        // The top-level call owns shared work, even if an individual branch stops waiting.
        let tasks = state.withLock { state in
            state.isCancelled = true
            return state.children.values
        }
        for task in tasks { task.cancel() }
    }
}

@MainActor
@Observable
final class CompositionLoad {
    var error: (any Error)?
    @ObservationIgnored private var currentTask: Task<Void, any Error>?
    let action: @MainActor (LoadPolicy) async throws -> Void
    let sourceOwnsOutcome: @MainActor () -> Bool

    init(
        _ action: @escaping @MainActor (LoadPolicy) async throws -> Void,
        sourceOwnsOutcome: @escaping @MainActor () -> Bool = { false }
    ) {
        self.action = action
        self.sourceOwnsOutcome = sourceOwnsOutcome
    }

    func run(_ policy: LoadPolicy) async {
        guard !Task.isCancelled else { return }
        let action = action
        let task = Task {
            try Task.checkCancellation()
            try await action(policy)
        }
        currentTask = task
        defer { if currentTask == task { currentTask = nil } }
        let result = await withTaskCancellationHandler { await task.result } onCancel: { task.cancel() }
        guard currentTask == task, !Task.isCancelled else { return }

        let nextError: (any Error)?
        switch result {
        case .success:
            nextError = nil
        case .failure(let error):
            guard !(error is CancellationError) else { return }
            nextError = sourceOwnsOutcome() ? nil : error
        }
        if error != nil || nextError != nil { error = nextError }
    }
}

@MainActor
struct CompositionEvaluation<Output: Sendable> {
    enum Outcome {
        case waiting
        case success(Output)
        case failure
    }

    let unavailable: [Bool]
    let outcome: Outcome
    let resetRevisions: [UInt]
    let reportErrors: @MainActor () -> (underlying: [any Error], mapped: (any Error)?)
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

    /// The current outcome, initially `.reset`. Loading preserves it.
    /// Reading this property starts sequence inputs.
    public var latest: Element {
        activate()
        return currentUpdate
    }

    /// The current errors from inputs and the transform, mapped by `mapFailures`.
    ///
    /// Includes optional and nested input errors and can coexist with a successful
    /// ``latest``. A failed ``latest`` carries the same mapped error. Returns `nil`
    /// when no errors are reported. Reading it starts sequence inputs.
    public var error: (any Error)? {
        activate()
        return currentError
    }

    /// Whether any composition load call is still executing its actions.
    public var isLoading: Bool { activeLoads > 0 }

    private(set) var compositionResetRevision: UInt = 0
    private(set) var currentUpdate: Element = .reset
    private(set) var underlyingErrors: [any Error] = []
    private var currentError: (any Error)?
    private var activeLoads = 0
    @ObservationIgnored private let evaluate: @MainActor () -> CompositionEvaluation<Output>
    @ObservationIgnored private let activations: [InputActivation]
    @ObservationIgnored private let loads: [InputLoad]
    @ObservationIgnored private var isActive = false
    @ObservationIgnored private var subscribers: [UUID: AsyncStream<Element>.Continuation] = [:]
    @ObservationIgnored private var previousUnavailable: [Bool] = []
    @ObservationIgnored private var previousResetRevisions: [UInt] = []
    @ObservationIgnored private var hasResult = false
    @ObservationIgnored private let observation = ObservationSubscription()
    @ObservationIgnored private let errorObservation = ObservationSubscription()
    @ObservationIgnored private var reportErrors: (@MainActor () -> (underlying: [any Error], mapped: (any Error)?))?

    init(
        activations: [InputActivation],
        loads: [InputLoad],
        evaluate: @escaping @MainActor () -> CompositionEvaluation<Output>
    ) {
        self.activations = activations
        self.loads = loads
        self.evaluate = evaluate
        recompute()
    }

    /// Runs input loading actions concurrently and waits for them to finish.
    ///
    /// Bucket and composition inputs load with the supplied policy by default.
    /// Each shared child composition loads once per top-level call. Custom
    /// actions receive the same policy and run for each declaration.
    ///
    /// Required action failures can fail the outcome. A bucket input with an
    /// available snapshot still contributes its value. Read ``error`` for its
    /// refresh failure. Siblings continue after a failure. An action retains its
    /// previous error until it succeeds or fails again. Cancellation preserves
    /// the action's error state. Overlapping calls run independently,
    /// and the newest invocation of each action owns its error state. Bucket
    /// loading policies still govern source work.
    public func load(using policy: LoadPolicy = .cached) async {
        let context = CompositionLoadContext(policy: policy)
        await withTaskCancellationHandler {
            await load(in: context)
        } onCancel: {
            context.cancel()
        }
    }

    func load(in context: CompositionLoadContext) async {
        activeLoads += 1
        let finishActivation = activate(forLoad: true)
        defer {
            finishActivation?()
            activeLoads -= 1
        }
        await withTaskGroup(of: Void.self) { group in
            for action in loads {
                group.addTask { await action(context) }
            }
        }
        recompute()
    }

    @discardableResult
    func activate(forLoad: Bool = false) -> (@MainActor () -> Void)? {
        guard !isActive else { return nil }
        isActive = true
        let finishActivations = activations.compactMap { $0(forLoad) }
        guard !finishActivations.isEmpty else { return nil }
        return { for finish in finishActivations { finish() } }
    }

    private func recompute() {
        let state = observation.track { evaluate() } onChange: { [weak self] in
            self?.recompute()
        }
        reportErrors = state.reportErrors
        recomputeErrors()
        let update: Element? = switch state.outcome {
        case .waiting: nil
        case .success(let output): .result(.success(output))
        case .failure: currentError.map { .result(.failure($0)) }
        }
        let reset = zip(state.unavailable, previousUnavailable).contains { $0 && !$1 }
        let nestedReset = zip(state.resetRevisions, previousResetRevisions).contains { $0 != $1 }
        previousUnavailable = state.unavailable
        previousResetRevisions = state.resetRevisions
        if hasResult && (reset || nestedReset || update == nil) {
            publish(.reset)
            hasResult = false
        }
        if let update {
            publish(update)
            hasResult = true
        }
    }

    private func recomputeErrors() {
        guard let reportErrors else { return }
        let errors = errorObservation.track { reportErrors() } onChange: { [weak self] in
            // Let snapshot evaluation handle changes observed by both passes.
            guard let self, self.observation.change.isCurrent else { return }
            self.recomputeErrors()
            if case .result(.failure) = self.currentUpdate, let error = self.currentError {
                self.publish(.result(.failure(error)))
            }
        }
        guard !underlyingErrors.isEmpty || !errors.underlying.isEmpty else { return }
        underlyingErrors = errors.underlying
        currentError = errors.mapped
    }

    private func publish(_ update: Element) {
        if case .reset = update { compositionResetRevision &+= 1 }
        currentUpdate = update
        for continuation in subscribers.values { continuation.yield(update) }
    }

    /// Creates an independent subscription with unbounded buffering.
    nonisolated public func makeAsyncIterator() -> AsyncIterator {
        let id = UUID()
        let (stream, continuation) = AsyncStream<Element>.makeStream()
        let registration = Task { @MainActor in
            guard !Task.isCancelled else { continuation.finish(); return }
            activate()
            subscribers[id] = continuation
            if case .result = currentUpdate { continuation.yield(currentUpdate) }
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


extension Composition: UpdateSource, CompositionResetSource, CompositionInputSource {}

public extension Composition {
    /// Creates a live, observable and asynchronously iterable snapshot composition.
    ///
    /// Required failures fail the combined outcome; optional failures supply `nil`.
    /// A thrown transform error also fails the outcome. Loading is explicit through
    /// ``Composition/load(using:)``. Conditions select dependencies once.
    ///
    /// Construction starts observing current values. Sequence subscriptions
    /// start on the first read of ``latest`` or ``error``, iteration, or load.
    /// Relationship inputs resolve keys once their root is available, using the
    /// cached policy outside explicit loads.
    ///
    /// - Parameters:
    ///   - inputs: The snapshot and observable dependencies in transform order.
    ///   - mapFailures: Combines the full list of errors in declaration order,
    ///     followed by any thrown transform error. The default creates a
    ///     ``CompositionError``. Each evaluation maps the list once and shares
    ///     the result between ``error`` and any failure in ``latest``. Keep the
    ///     closure free of side effects.
    ///   - transform: Produces an output when all required inputs are available.
    convenience init<each Value: Sendable>(
        @CompositionBuilder _ inputs: () -> CompositionInputs<repeat each Value>,
        mapFailures: @escaping @MainActor ([any Error]) -> any Error = { CompositionError($0) },
        transform: @escaping @MainActor (repeat each Value) throws -> Output
    ) {
        let inputs = inputs().makeInputs()
        var activations: [InputActivation] = []
        var loads: [InputLoad] = []
        for input in repeat each inputs {
            if let activation = input.activate { activations.append(activation) }
            if let load = input.load { loads.append(load) }
        }
        self.init(activations: activations, loads: loads) {
            let states = (repeat (each inputs).read())
            var unavailable: [Bool] = []
            var hasBlockingFailure = false
            var reports: [(failures: [any Error], read: (@MainActor () -> [any Error]?)?)] = []
            var resetRevisions: [UInt] = []
            for state in repeat each states {
                unavailable.append(state.unavailable)
                resetRevisions.append(state.resetRevision)
                hasBlockingFailure = hasBlockingFailure || !state.failures.isEmpty
                reports.append((state.failures, state.reportedFailures))
            }
            let outcome: CompositionEvaluation<Output>.Outcome
            var transformError: (any Error)?
            if hasBlockingFailure {
                outcome = .failure
            } else if unavailable.contains(true) {
                outcome = .waiting
            } else {
                do {
                    outcome = .success(try transform(repeat (each states).value!))
                } catch {
                    outcome = .failure
                    transformError = error
                }
            }
            return .init(unavailable: unavailable, outcome: outcome, resetRevisions: resetRevisions) {
                var errors = reports.flatMap { $0.read?() ?? $0.failures }
                if let transformError { errors.append(transformError) }
                return (errors, errors.isEmpty ? nil : mapFailures(errors))
            }
        }
    }
}
