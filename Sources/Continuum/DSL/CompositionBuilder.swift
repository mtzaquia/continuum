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
/// unavailable or failed. Bucket and selected partition inputs load by default
/// when the composition loads. Construction does not load the source.
@MainActor
public struct Input<Value: Sendable> {
    let read: @MainActor () -> CompositionInputState<Value>
    let load: (@MainActor @Sendable (LoadPolicy) async -> Void)?
    let activate: InputActivation?
    private var materialize: (@MainActor () -> Input<Value>)? = nil

    /// Observes a source and lets it participate in explicit composition loads.
    ///
    /// By default, buckets and selected partitions load with the composition's
    /// policy. Other sources are observation-only. An explicit `load` closure
    /// replaces the default action. Use `{ _ in }` for an observation-only bucket.
    ///
    /// The action updates the observed source. An available bucket snapshot
    /// takes precedence over a thrown load error. Errors already published by
    /// the source follow its recovery. Other action errors remain until the next
    /// load of this input.
    public init<Source: UpdateSource>(
        _ source: Source,
        load: (@MainActor (LoadPolicy) async throws -> Void)? = nil
    ) where Source.Snapshot == Value {
        let bucket = source as? any BucketSnapshotSource
        let action: (@MainActor (LoadPolicy) async throws -> Void)?
        if let load {
            action = load
        } else if let bucket {
            action = { policy in _ = try await bucket.load(using: policy) }
        } else {
            action = nil
        }
        let loader = action.map { action in
            CompositionLoad(action, sourceOwnsOutcome: {
                switch source.latest {
                case .result(.failure): true
                case .result(.success): bucket != nil
                case .reset: false
                }
            })
        }
        let resetSource = source as? any CompositionResetSource
        let nested = source as? any CompositionInputSource<Value>
        self.init(
            readUpdate: {
                if let nested { return nested.currentUpdate }
                return source.latest
            },
            resetRevision: { resetSource?.compositionResetRevision ?? 0 },
            loader: loader,
            bucket: bucket,
            nested: nested
        )
    }

    /// Observes an expression without loading it.
    ///
    /// Observable properties read by the closure trigger recomputation. The
    /// closure is reevaluated on the main actor and should be free of side
    /// effects. Its returned value, including a domain `nil`, is available.
    public init(_ read: @escaping @MainActor () -> Value) {
        self.init(readUpdate: { .result(.success(read())) })
    }

    /// Observes an expression and supplies an action to update its observable state.
    ///
    /// The policy is forwarded unchanged. A thrown action error fails this input
    /// until its next load; use ``optional()`` to make such failures contribute nil.
    public init(
        _ read: @escaping @MainActor () -> Value,
        load: @escaping @MainActor (LoadPolicy) async throws -> Void
    ) {
        self.init(readUpdate: { .result(.success(read())) }, loader: CompositionLoad(load))
    }

    /// Observes a closure that reports its value, failure, or unavailability.
    ///
    /// The closure runs on the main actor under observation and should be free
    /// of side effects. A required input waits on `.reset` and fails on a failure
    /// result. An observed reset clears any previous composition result. Changes
    /// made before the closure is read again may coalesce.
    ///
    /// - Parameters:
    ///   - update: Reads the current outcome from observable state.
    ///   - load: An optional action receiving the composition's loading policy.
    ///     If the closure already reports failure, that failure takes precedence
    ///     over an error thrown by the action.
    public init(
        update: @escaping @MainActor () -> Update<Value>,
        load: (@MainActor (LoadPolicy) async throws -> Void)? = nil
    ) {
        let loader = load.map { action in
            CompositionLoad(action, sourceOwnsOutcome: {
                if case .result(.failure) = update() { return true }
                return false
            })
        }
        self.init(readUpdate: update, loader: loader)
    }

    /// Bridges a sequence of results into the composition's required inputs.
    ///
    /// The factory is invoked once per composition on first use, and again on
    /// subsequent loads configured with `.restart`. Reading `latest` or `error`,
    /// iterating, or loading starts sequence inputs, including nested ones.
    /// Before the first element this input is unavailable.
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
    ///   - updates: Creates a fresh sendable sequence of results for each subscription.
    ///   - subscriptionOnLoad: Whether loading preserves or replaces the subscription.
    ///   - load: An optional action receiving the composition's loading policy.
    public init<Source: AsyncSequence & Sendable, Failure: Error>(
        updates: @escaping @MainActor () -> Source,
        subscriptionOnLoad: InputSubscriptionBehavior = .keep,
        load: (@MainActor (LoadPolicy) async throws -> Void)? = nil
    ) where Source.Element == Result<Value, Failure> {
        self.init(read: { .init(value: nil, unavailable: true, failures: []) }, materialize: {
            let state = SequenceInputState<Value>(updates)
            let loader = CompositionLoad { policy in
                state.subscribeForLoad(subscriptionOnLoad)
                try await state.waitUntilSubscribed()
                try await load?(policy)
            }
            let action: (@MainActor @Sendable (LoadPolicy) async -> Void)?
            if load != nil || subscriptionOnLoad == .restart {
                action = { policy in await loader.run(policy) }
            } else {
                action = nil
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
            }, load: action, activate: { forLoad in
                state.activate(forLoad: forLoad && subscriptionOnLoad == .restart)
            })
        })
    }

    private init(
        readUpdate: @escaping @MainActor () -> Update<Value>,
        resetRevision: @escaping @MainActor () -> UInt = { 0 },
        loader: CompositionLoad? = nil,
        bucket: (any BucketSnapshotSource)? = nil,
        nested: (any CompositionInputSource<Value>)? = nil
    ) {
        read = {
            let update = readUpdate()
            let failures = loader?.error.map { [$0] } ?? []
            let revision = resetRevision()
            let reportErrors: @MainActor () -> [any Error]? = {
                if let errors = nested?.underlyingErrors, !errors.isEmpty {
                    if case .result(.failure) = update { return errors }
                    return errors + failures
                }
                if failures.isEmpty, let error = bucket?.error { return [error] }
                return nil
            }
            switch update {
            case .reset:
                return .init(value: nil, unavailable: true,
                             failures: failures, resetRevision: revision, reportedFailures: reportErrors)
            case .result(.failure(let error)):
                return .init(value: nil, unavailable: false, failures: [error], resetRevision: revision,
                             reportedFailures: nested == nil ? nil : reportErrors)
            case .result(.success(let value)):
                return .init(value: value, unavailable: false,
                             failures: failures, resetRevision: revision, reportedFailures: reportErrors)
            }
        }
        if let nested {
            activate = { forLoad in nested.activate(forLoad: forLoad) }
        } else {
            activate = nil
        }
        if let loader {
            load = { policy in await loader.run(policy) }
        } else {
            load = nil
        }
    }

    init(
        read: @escaping @MainActor () -> CompositionInputState<Value>,
        load: (@MainActor @Sendable (LoadPolicy) async -> Void)? = nil,
        activate: InputActivation? = nil,
        materialize: (@MainActor () -> Input<Value>)? = nil
    ) {
        self.read = read
        self.load = load
        self.activate = activate
        self.materialize = materialize
    }

    func resolved() -> Self { materialize?() ?? self }

    /// Makes unavailable and failed snapshots contribute `nil` without blocking output.
    ///
    /// Loading participation is preserved. Optional input errors are available
    /// through ``Composition/error`` while the transform receives `nil`.
    // Prefer the flattening overload when Value is already Optional, even
    // when the caller provides no contextual return type.
    @_disfavoredOverload
    public func optional() -> Input<Value?> {
        var factory: (@MainActor () -> Input<Value?>)?
        if let materialize { factory = { materialize().optional() } }
        return Input<Value?>(read: {
            let state = read()
            return .init(value: .some(state.failures.isEmpty ? state.value : nil),
                         unavailable: false, failures: [], reportedFailures: {
                             state.reportedFailures?() ?? (state.failures.isEmpty ? nil : state.failures)
                         })
        }, load: load, activate: activate, materialize: factory)
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
            return .init(value: .some(value), unavailable: false, failures: [], reportedFailures: {
                state.reportedFailures?() ?? (state.failures.isEmpty ? nil : state.failures)
            })
        }, load: load, activate: activate, materialize: factory)
    }
}

/// Typed inputs accumulated by ``CompositionBuilder``.
@MainActor
public struct CompositionInputs<each Value: Sendable> {
    let makeInputs: @MainActor () -> (repeat Input<each Value>)
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
        .init(makeInputs: { input.resolved() })
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
        .init(makeInputs: {
            let a = accumulated.makeInputs()
            let b = next.makeInputs()
            return (repeat each a, repeat each b)
        })
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
            return .init(makeInputs: {
                let inputs = component.makeInputs()
                return (repeat (each inputs).optional())
            })
        }
        return .init(makeInputs: { (repeat absent((each T).self)) })
    }

    private static func absent<T: Sendable>(_: T.Type) -> Input<T?> {
        Input<T?>(read: { .init(value: .some(nil), unavailable: false, failures: []) })
    }
}
