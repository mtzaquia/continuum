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
    let outcome: Update<Value>
    var resetRevision: UInt = 0
    // Read operation errors separately from the transform's dependencies.
    var reportedFailures: (@MainActor () -> [any Error]?)? = nil

    func errors() -> [any Error] {
        if let failures = reportedFailures?() { return failures }
        if case .result(.failure(let error)) = outcome { return [error] }
        return []
    }
}

@MainActor
struct CompositionEvaluation<Output: Sendable> {
    struct InputStatus {
        let isUnavailable: Bool
        let resetRevision: UInt
    }

    enum Outcome {
        case waiting
        case success(Output)
        case failure
    }

    let inputStatuses: [InputStatus]
    let outcome: Outcome
    let reportErrors: @MainActor () -> (underlying: [any Error], mapped: (any Error)?)
}

/// Combines observed inputs into a live result on the main actor.
///
/// Each iterator independently receives the current result followed by updates.
/// Initial unavailability is silent. Required reset transitions clear prior
/// outcomes before any accompanying failure. Errors never end observation.
/// Iterators retain the composition and buffer emitted updates independently.
/// Input changes can coalesce before an update is emitted.
///
/// Use `Input(composition)` as an input to another composition. Required
/// reset transitions propagate even when observation coalesces a reset and its
/// following result. Optional inputs consume that invalidation as `nil`.
@MainActor
@Observable
public final class Composition<Output: Sendable>: AsyncSequence {
    /// The result or reset delivered to each subscriber.
    public typealias Element = Update<Output>

    /// The current outcome. Before a result is available, this is `.reset`.
    ///
    /// Starting a load preserves this outcome until an input change affects it.
    public var latest: Element { currentUpdate }

    /// The current errors from inputs and the transform, mapped by `mapFailures`.
    ///
    /// Includes optional and nested input errors and can coexist with a successful
    /// ``latest``. A failed ``latest`` carries the same mapped error. Returns `nil`
    /// when no errors are reported.
    public var error: (any Error)? { currentError }

    /// Whether any composition load is still waiting for its sources.
    public var isLoading: Bool { activeLoads > 0 }

    private(set) var resetRevision: UInt = 0
    private var currentUpdate: Element = .reset
    private(set) var underlyingErrors: [any Error] = []
    private var currentError: (any Error)?
    private var activeLoads = 0
    @ObservationIgnored private let evaluate: @MainActor () -> CompositionEvaluation<Output>
    @ObservationIgnored private let loadSources: [any LoadableSource]
    @ObservationIgnored private var subscribers: [UUID: AsyncStream<Element>.Continuation] = [:]
    @ObservationIgnored private var previousInputs: [CompositionEvaluation<Output>.InputStatus] = []
    @ObservationIgnored private let observation = ObservationSubscription()
    @ObservationIgnored private let errorObservation = ObservationSubscription()
    @ObservationIgnored private var reportErrors: (@MainActor () -> (underlying: [any Error], mapped: (any Error)?))?

    init(
        loadSources: [any LoadableSource],
        evaluate: @escaping @MainActor () -> CompositionEvaluation<Output>
    ) {
        self.loadSources = loadSources
        self.evaluate = evaluate
        recompute()
    }

    /// Loads participating sources and processes pending input observations.
    ///
    /// Each source loads once per call, including across nested compositions.
    /// Independent sources load concurrently. Observe failures through
    /// ``error`` or ``latest``.
    ///
    /// - Parameter policy: The policy forwarded unchanged to each source.
    public func load(using policy: LoadPolicy = .cached) async {
        await SourceLoadContext.load(self, using: policy)
    }

    func load(in context: SourceLoadContext) async {
        activeLoads += 1
        defer { activeLoads -= 1 }
        await context.loadAll(loadSources)
        flushObservations()
    }

    private func flushObservations() {
        if !observation.change.isCurrent {
            recompute()
        } else if !errorObservation.change.isCurrent {
            recomputeErrors()
            if case .result(.failure) = currentUpdate, let currentError {
                publish(.result(.failure(currentError)))
            }
        }
    }

    private func recompute() {
        let state = observation.track { evaluate() } onChange: { [weak self] in
            self?.flushObservations()
        }
        reportErrors = state.reportErrors
        recomputeErrors()
        let update: Element? = switch state.outcome {
        case .waiting: nil
        case .success(let output): .result(.success(output))
        case .failure: currentError.map { .result(.failure($0)) }
        }
        let invalidated = zip(state.inputStatuses, previousInputs).contains { current, previous in
            (current.isUnavailable && !previous.isUnavailable) || current.resetRevision != previous.resetRevision
        }
        previousInputs = state.inputStatuses
        if case .result = currentUpdate, invalidated || update == nil {
            publish(.reset)
        }
        if let update {
            publish(update)
        }
    }

    private func recomputeErrors() {
        guard let reportErrors else { return }
        let errors = errorObservation.track { reportErrors() } onChange: { [weak self] in
            self?.flushObservations()
        }
        guard !underlyingErrors.isEmpty || !errors.underlying.isEmpty else { return }
        underlyingErrors = errors.underlying
        currentError = errors.mapped
    }

    private func publish(_ update: Element) {
        if case .reset = update { resetRevision &+= 1 }
        currentUpdate = update
        for continuation in subscribers.values { continuation.yield(update) }
    }

    /// Creates an independent subscription with unbounded buffering.
    nonisolated public func makeAsyncIterator() -> AsyncIterator {
        let id = UUID()
        let (stream, continuation) = AsyncStream<Element>.makeStream()
        let registration = Task { @MainActor in
            guard !Task.isCancelled else { continuation.finish(); return }
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


extension Composition: UpdateSource, LoadableSource, CompositionResetSource {}

public extension Composition {
    /// Creates a live, observable and asynchronously iterable snapshot composition.
    ///
    /// Required failures and thrown transform errors fail the combined outcome.
    /// Optional failures supply `nil` and still contribute to ``error``.
    ///
    /// Construction selects conditional inputs and starts observing their
    /// current values. Call ``Composition/load(using:)`` to load sources.
    ///
    /// - Parameters:
    ///   - inputs: The snapshot and observable dependencies in transform order.
    ///   - mapFailures: Combines the full list of errors in declaration order,
    ///     followed by any thrown transform error. The default creates a
    ///     ``CompositionError``. Each evaluation maps the list once and shares
    ///     the result between ``error`` and any failure in ``latest``. Keep the
    ///     closure free of side effects.
    ///   - transform: Produces an output when all required inputs succeed.
    ///     Keep the closure free of side effects because observation can
    ///     reevaluate it.
    convenience init<each Value: Sendable>(
        @CompositionBuilder _ inputs: () -> CompositionInputs<repeat each Value>,
        mapFailures: @escaping @MainActor ([any Error]) -> any Error = { CompositionError($0) },
        transform: @escaping @MainActor (repeat each Value) throws -> Output
    ) {
        let inputs = inputs().inputs
        var loadSources: [any LoadableSource] = []
        for input in repeat each inputs {
            if let source = input.loadSource { loadSources.append(source) }
        }
        self.init(loadSources: loadSources) {
            let states = (repeat (each inputs).read())
            var inputStatuses: [CompositionEvaluation<Output>.InputStatus] = []
            var hasBlockingFailure = false
            var reports: [@MainActor () -> [any Error]] = []
            for state in repeat each states {
                let isUnavailable = if case .reset = state.outcome {
                    true
                } else {
                    false
                }
                inputStatuses.append(.init(isUnavailable: isUnavailable, resetRevision: state.resetRevision))
                if case .result(.failure) = state.outcome { hasBlockingFailure = true }
                reports.append(state.errors)
            }
            let outcome: CompositionEvaluation<Output>.Outcome
            var transformError: (any Error)?
            if hasBlockingFailure {
                outcome = .failure
            } else if inputStatuses.contains(where: { $0.isUnavailable }) {
                outcome = .waiting
            } else {
                do {
                    outcome = .success(try transform(repeat (each states).outcome.snapshot!))
                } catch {
                    outcome = .failure
                    transformError = error
                }
            }
            return .init(inputStatuses: inputStatuses, outcome: outcome) {
                var errors = reports.flatMap { $0() }
                if let transformError { errors.append(transformError) }
                return (errors, errors.isEmpty ? nil : mapFailures(errors))
            }
        }
    }
}
