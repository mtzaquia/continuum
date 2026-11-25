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

/// The accumulated source configuration for one atomic bucket partition.
///
/// Callers create this value through ``BucketBuilder`` rather than constructing
/// it directly.
public struct BucketConfiguration<Space: ContinuumKeySpace, Operation: Sendable>: Sendable {
    let localSources: [LocalSource<Space>]
    let remoteSources: [RemoteSource<Space>]
    let invalidationSignals: [InvalidationSignal]
    let mutations: [Mutate<Operation, Space>]

    init(
        localSources: [LocalSource<Space>] = [],
        remoteSources: [RemoteSource<Space>] = [],
        invalidationSignals: [InvalidationSignal] = [],
        mutations: [Mutate<Operation, Space>] = []
    ) {
        self.localSources = localSources
        self.remoteSources = remoteSources
        self.invalidationSignals = invalidationSignals
        self.mutations = mutations
    }

    func merging(_ other: Self) -> Self {
        Self(
            localSources: localSources + other.localSources,
            remoteSources: remoteSources + other.remoteSources,
            invalidationSignals:
                invalidationSignals + other.invalidationSignals,
            mutations: mutations + other.mutations
        )
    }

    fileprivate func withMutations<Other>(
        _ mutations: [Mutate<Other, Space>]
    ) -> BucketConfiguration<Space, Other> {
        BucketConfiguration<Space, Other>(
            localSources: localSources,
            remoteSources: remoteSources,
            invalidationSignals: invalidationSignals,
            mutations: mutations
        )
    }
}

/// Builds the sources and mutation handler for one atomic bucket partition.
///
/// The key space determines the snapshot type. A ``Mutate`` expression
/// determines the operation type. Source-only configurations accept a
/// ``MutationAction`` at the call site. Each partition accepts at most one
/// ``RemoteSource`` and one ``Mutate`` handler.
@resultBuilder
public enum BucketBuilder<Space: ContinuumKeySpace>: Sendable {
    /// A configuration that has only source capabilities.
    public typealias Sources = BucketConfiguration<Space, Never>

    /// Builds an empty source configuration.
    public static func buildBlock() -> Sources { Sources() }

    /// Adds a local source.
    public static func buildExpression(_ expression: LocalSource<Space>) -> Sources {
        Sources(localSources: [expression])
    }

    /// Adds a remote source.
    public static func buildExpression(_ expression: RemoteSource<Space>) -> Sources {
        Sources(remoteSources: [expression])
    }

    /// Adds an invalidation signal.
    public static func buildExpression(_ expression: InvalidationSignal) -> Sources {
        Sources(invalidationSignals: [expression])
    }

    /// Adds a mutation handler and carries its operation type into the configuration.
    public static func buildExpression<Operation>(
        _ expression: Mutate<Operation, Space>
    ) -> BucketConfiguration<Space, Operation> {
        BucketConfiguration(mutations: [expression])
    }

    /// Starts an ordered configuration.
    public static func buildPartialBlock<Operation>(
        first: BucketConfiguration<Space, Operation>
    ) -> BucketConfiguration<Space, Operation> {
        first
    }

    /// Combines two source configurations.
    public static func buildPartialBlock(accumulated: Sources, next: Sources) -> Sources {
        accumulated.merging(next)
    }

    /// Adds the first mutation handler after a group of sources.
    public static func buildPartialBlock<Operation>(
        accumulated: Sources, next: BucketConfiguration<Space, Operation>
    ) -> BucketConfiguration<Space, Operation> {
        accumulated.withMutations([]).merging(next)
    }

    /// Appends sources to a configuration with a mutation operation type.
    public static func buildPartialBlock<Operation>(
        accumulated: BucketConfiguration<Space, Operation>, next: Sources
    ) -> BucketConfiguration<Space, Operation> {
        accumulated.merging(next.withMutations([]))
    }

    /// Combines configurations with the same operation type.
    public static func buildPartialBlock<Operation>(
        accumulated: BucketConfiguration<Space, Operation>,
        next: BucketConfiguration<Space, Operation>
    ) -> BucketConfiguration<Space, Operation> {
        accumulated.merging(next)
    }

    /// Includes a conditionally produced configuration when present.
    public static func buildOptional<Operation>(
        _ component: BucketConfiguration<Space, Operation>?
    ) -> BucketConfiguration<Space, Operation> {
        component ?? BucketConfiguration()
    }

    /// Includes the first branch of a conditional configuration.
    public static func buildEither<Operation>(
        first component: BucketConfiguration<Space, Operation>
    ) -> BucketConfiguration<Space, Operation> {
        component
    }

    /// Includes the second branch of a conditional configuration.
    public static func buildEither<Operation>(
        second component: BucketConfiguration<Space, Operation>
    ) -> BucketConfiguration<Space, Operation> {
        component
    }

    /// Includes a source-only first branch alongside a typed second branch.
    @_disfavoredOverload
    public static func buildEither<Operation>(first component: Sources) -> BucketConfiguration<Space, Operation> {
        component.withMutations([])
    }

    /// Includes a source-only second branch alongside a typed first branch.
    @_disfavoredOverload
    public static func buildEither<Operation>(second component: Sources) -> BucketConfiguration<Space, Operation> {
        component.withMutations([])
    }

    /// Combines configurations produced by a loop.
    public static func buildArray<Operation>(
        _ components: [BucketConfiguration<Space, Operation>]
    ) -> BucketConfiguration<Space, Operation> {
        components.reduce(BucketConfiguration()) { $0.merging($1) }
    }

    /// Gives a source-only bucket its closure-based operation type.
    public static func buildFinalResult(_ component: Sources) -> BucketConfiguration<Space, MutationAction<Space>> {
        component.withMutations([])
    }

    /// Preserves the operation type declared by a mutation handler.
    @_disfavoredOverload
    public static func buildFinalResult<Operation>(
        _ component: BucketConfiguration<Space, Operation>
    ) -> BucketConfiguration<Space, Operation> {
        component
    }
}
