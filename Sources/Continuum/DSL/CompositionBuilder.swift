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

/// An observed source or expression. Required inputs gate the transform.
@MainActor
public struct Input<Value: Sendable> {
    let read: @MainActor () -> CompositionInputState<Value>
    let loadSource: (any LoadableSource)?

    /// Observes a Continuum or external ``UpdateSource``.
    ///
    /// By default, composition loads also load buckets and nested
    /// compositions. Set `loads` to `false` to skip loading through this
    /// input. Call an external source's operations directly.
    public init(_ source: any UpdateSource<Value>, loads: Bool = true) {
        let resetSource = source as? any CompositionResetSource
        read = {
            let outcome = source.latest
            return .init(outcome: outcome, resetRevision: resetSource?.resetRevision ?? 0, reportedFailures: {
                if let child = source as? Composition<Value> { return child.underlyingErrors }
                if case .result(.failure) = outcome { return nil }
                return source.error.map { [$0] }
            })
        }
        loadSource = loads ? source as? any LoadableSource : nil
    }

    /// Reads an observable expression. A domain `nil` is an available value.
    public init(_ read: @escaping @MainActor () -> Value) {
        self.init(read: { .init(outcome: .result(.success(read()))) })
    }

    init(
        read: @escaping @MainActor () -> CompositionInputState<Value>,
        loadSource: (any LoadableSource)? = nil
    ) {
        self.read = read
        self.loadSource = loadSource
    }

    /// Supplies `nil` for an unavailable or failed input and retains its errors.
    @_disfavoredOverload
    public func optional() -> Input<Value?> {
        optional { $0 }
    }

    /// Relaxes an optional snapshot while keeping a single optional layer.
    public func optional<Wrapped: Sendable>() -> Input<Wrapped?> where Value == Wrapped? {
        optional { $0 ?? nil }
    }

    private func optional<Wrapped: Sendable>(
        _ flatten: @escaping @MainActor (Value?) -> Wrapped?
    ) -> Input<Wrapped?> {
        Input<Wrapped?>(read: {
            let state = read()
            return .init(
                outcome: .result(.success(flatten(state.outcome.snapshot))),
                reportedFailures: { state.errors() }
            )
        }, loadSource: loadSource)
    }
}

/// Typed dependencies accumulated by ``CompositionBuilder``.
@MainActor
public struct CompositionInputs<each Value: Sendable> {
    let inputs: (repeat Input<each Value>)
}

/// Combines typed dependencies, selecting conditional branches at construction.
@MainActor
@resultBuilder
public enum CompositionBuilder {
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
        let a = accumulated.inputs
        let b = next.inputs
        return .init(inputs: (repeat each a, repeat each b))
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
            let inputs = component.inputs
            return .init(inputs: (repeat (each inputs).optional()))
        }
        return .init(inputs: (repeat absent((each T).self)))
    }

    private static func absent<T: Sendable>(_: T.Type) -> Input<T?> {
        Input { Optional<T>.none }
    }
}
