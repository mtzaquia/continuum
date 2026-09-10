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

/// An observed bucket, composition, or expression with an optional loading action.
///
/// Inputs are required by default. Use ``optional()`` to supply `nil` when
/// unavailable or failed. Omitting the loading closure makes this input observe
/// only. Construction does not load the source.
@MainActor
public struct Input<Value: Sendable> {
    let read: @MainActor () -> CompositionInputState<Value>
    let loads: [@MainActor @Sendable (LoadPolicy) async -> Void]
    private var materialize: (@MainActor () -> Input<Value>)? = nil

    /// Observes a bucket, selected partition, or composition without loading it.
    public init<Source: UpdateSource>(_ source: Source)
    where Source.Snapshot == Value {
        self.init(source, loader: nil)
    }

    /// Observes a source and forwards composition load policies to an action.
    ///
    /// The action updates the observed source; its completion alone does not
    /// establish a snapshot. Errors already reported by the source follow its
    /// recovery. Other action errors remain until the next load of this input.
    public init<Source: UpdateSource>(
        _ source: Source,
        load: @escaping @MainActor (LoadPolicy) async throws -> Void
    ) where Source.Snapshot == Value {
        self.init(source, loader: CompositionLoad(load, sourceReportsFailure: {
            if case .result(.failure) = source._latestUpdateForObservation() { return true }
            return false
        }))
    }

    /// Observes an expression without loading it.
    ///
    /// Observable properties read by the closure trigger recomputation. The
    /// closure is reevaluated on the main actor and should be free of side
    /// effects. Its returned value, including a domain `nil`, is available.
    public init(_ read: @escaping @MainActor () -> Value) {
        self.init(read: { .init(value: read(), unavailable: false, failures: []) })
    }

    /// Observes an expression and supplies an action to update its observable state.
    ///
    /// The policy is forwarded unchanged. A thrown action error fails this input
    /// until its next load; use ``optional()`` to make such failures contribute nil.
    public init(
        _ read: @escaping @MainActor () -> Value,
        load: @escaping @MainActor (LoadPolicy) async throws -> Void
    ) {
        let loader = CompositionLoad(load)
        self.init(read: {
            .init(value: read(), unavailable: false,
                  failures: loader.error.map { [$0] } ?? [])
        }, loads: [{ policy in await loader.run(policy) }])
    }

    /// Bridges a sequence of results into the composition's required inputs.
    ///
    /// The factory is invoked once per composition and again on loads configured
    /// with `.restart`. Before the first element this input is unavailable.
    /// Failures are outcomes; normal completion retains the last outcome. An
    /// iteration error becomes a failure and ends that subscription. Use
    /// ``optional()`` to turn unavailable and failed outcomes into nil.
    ///
    /// Resubscription cancels the old iterator, retains the current outcome, and
    /// creates a new iterator before invoking `load`. It does not wait for an
    /// element or guarantee upstream hot-stream readiness. Obsolete elements
    /// and errors are ignored. Releasing the composition cancels its subscription.
    ///
    /// - Parameters:
    ///   - updates: Creates a fresh sequence of results for each subscription.
    ///   - subscriptionOnLoad: Whether loading preserves or replaces the subscription.
    ///   - load: An optional action receiving the composition's loading policy.
    public init<Source: AsyncSequence, Failure: Error>(
        updates: @escaping @MainActor () -> Source,
        subscriptionOnLoad: InputSubscriptionBehavior = .keep,
        load: (@MainActor (LoadPolicy) async throws -> Void)? = nil
    ) where Source.Element == Result<Value, Failure>,
          Source.AsyncIterator: SendableMetatype {
        self.init(read: { .init(value: nil, unavailable: true, failures: []) }, materialize: {
            let state = SequenceInputState<Value>(updates)
            state.subscribe()
            let loader = CompositionLoad { policy in
                if case .restart = subscriptionOnLoad { state.subscribe() }
                let subscription = state.generation
                await state.waitUntilSubscribed()
                try Task.checkCancellation()
                guard state.generation == subscription else { throw CancellationError() }
                try await load?(policy)
            }
            let actions: [@MainActor @Sendable (LoadPolicy) async -> Void]
            if load != nil || subscriptionOnLoad == .restart {
                actions = [{ policy in await loader.run(policy) }]
            } else {
                actions = []
            }
            return Input(read: {
                if let error = loader.error {
                    return .init(value: nil, unavailable: false, failures: [error])
                }
                switch state.result {
                case .success(let value):
                    return .init(value: value, unavailable: false, failures: [])
                case .failure(let error):
                    return .init(value: nil, unavailable: false, failures: [error])
                case nil:
                    return .init(value: nil, unavailable: true, failures: [])
                }
            }, loads: actions)
        })
    }

    private init<Source: UpdateSource>(
        _ source: Source, loader: CompositionLoad?
    ) where Source.Snapshot == Value {
        read = {
            let update = source._latestUpdateForObservation()
            let loadError = loader?.error
            let resetRevision = (source as? any CompositionResetSource)?.compositionResetRevision ?? 0
            switch update {
            case .reset:
                return .init(value: nil, unavailable: true,
                             failures: loadError.map { [$0] } ?? [], resetRevision: resetRevision)
            case .result(.failure(let error)):
                return .init(value: nil, unavailable: false, failures: [error], resetRevision: resetRevision)
            case .result(.success(let value)):
                return .init(value: value, unavailable: false,
                             failures: loadError.map { [$0] } ?? [], resetRevision: resetRevision)
            }
        }
        loads = loader.map { loader in [{ policy in await loader.run(policy) }] } ?? []
    }

    init(
        read: @escaping @MainActor () -> CompositionInputState<Value>,
        loads: [@MainActor @Sendable (LoadPolicy) async -> Void] = [],
        materialize: (@MainActor () -> Input<Value>)? = nil
    ) {
        self.read = read
        self.loads = loads
        self.materialize = materialize
    }

    func resolved() -> Self { materialize?() ?? self }

    /// Makes unavailable and failed snapshots contribute `nil` without blocking output.
    ///
    /// Loading participation is preserved. Optional input errors do not fail
    /// the composition.
    // Prefer the flattening overload when Value is already Optional, even
    // when the caller provides no contextual return type.
    @_disfavoredOverload
    public func optional() -> Input<Value?> {
        var factory: (@MainActor () -> Input<Value?>)?
        if let materialize { factory = { materialize().optional() } }
        return Input<Value?>(read: {
            let state = read()
            return .init(value: .some(state.failures.isEmpty ? state.value : nil),
                         unavailable: false, failures: [])
        }, loads: loads, materialize: factory)
    }

    /// Relaxes an already-optional input without adding another optional layer.
    ///
    /// A loaded `nil`, an unavailable snapshot, and a failed input all contribute
    /// `nil`. Loading participation is preserved. Without this modifier, an
    /// optional-valued snapshot remains required and propagates failures.
    public func optional<Wrapped: Sendable>() -> Input<Wrapped?>
    where Value == Wrapped? {
        var factory: (@MainActor () -> Input<Wrapped?>)?
        if let materialize { factory = { materialize().optional() } }
        return Input<Wrapped?>(read: {
            let state = read()
            let value: Wrapped? = state.failures.isEmpty ? (state.value ?? nil) : nil
            return .init(value: .some(value), unavailable: false, failures: [])
        }, loads: loads, materialize: factory)
    }
}

/// Typed inputs accumulated by ``CompositionBuilder``.
@MainActor
public struct CompositionInputs<each Value: Sendable> {
    let inputs: (repeat Input<each Value>)
}

/// Builds a fixed list of typed dependencies, selecting branches at construction.
///
/// Both branches must produce the same value types in the same order. An `if`
/// without `else` makes each contained input optional. Loops are not supported.
@MainActor
@resultBuilder
public enum CompositionBuilder {
    /// Adds one snapshot or observable expression.
    public static func buildExpression<T>(_ input: Input<T>) -> CompositionInputs<T> {
        .init(inputs: input)
    }

    /// Starts a list of inputs.
    public static func buildPartialBlock<each T>(
        first: CompositionInputs<repeat each T>
    ) -> CompositionInputs<repeat each T> { first }

    /// Appends inputs while preserving their individual transform parameters.
    public static func buildPartialBlock<each A, each B>(
        accumulated: CompositionInputs<repeat each A>,
        next: CompositionInputs<repeat each B>
    ) -> CompositionInputs<repeat each A, repeat each B> {
        .init(inputs: (repeat each accumulated.inputs, repeat each next.inputs))
    }

    /// Selects the first branch with a matching output shape.
    public static func buildEither<each T>(
        first: CompositionInputs<repeat each T>
    ) -> CompositionInputs<repeat each T> { first }

    /// Selects the second branch with a matching output shape.
    public static func buildEither<each T>(
        second: CompositionInputs<repeat each T>
    ) -> CompositionInputs<repeat each T> { second }

    /// Supplies optional parameters for conditionally included inputs.
    public static func buildOptional<each T>(
        _ component: CompositionInputs<repeat each T>?
    ) -> CompositionInputs<repeat (each T)?> {
        if let component {
            return .init(inputs: (repeat (each component.inputs).optional()))
        }
        return .init(inputs: (repeat absent((each T).self)))
    }

    private static func absent<T: Sendable>(_: T.Type) -> Input<T?> {
        Input<T?>(read: { .init(value: .some(nil), unavailable: false, failures: []) })
    }
}

