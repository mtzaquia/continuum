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
public struct BucketConfiguration<Space: ContinuumKeySpace>: Sendable {
    let localSources: [LocalSource<Space>]
    let remoteSources: [RemoteSource<Space>]
    let invalidationSignals: [InvalidationSignal]

    init(
        localSources: [LocalSource<Space>] = [],
        remoteSources: [RemoteSource<Space>] = [],
        invalidationSignals: [InvalidationSignal] = []
    ) {
        self.localSources = localSources
        self.remoteSources = remoteSources
        self.invalidationSignals = invalidationSignals
    }

    func merging(_ other: Self) -> Self {
        Self(
            localSources: localSources + other.localSources,
            remoteSources: remoteSources + other.remoteSources,
            invalidationSignals:
                invalidationSignals + other.invalidationSignals
        )
    }
}

/// Builds additive source capabilities for one atomic bucket partition.
///
/// The outer key space provides the complete snapshot type to each expression.
/// ``LocalSource`` expressions are tried in declaration order, and a bucket
/// accepts at most one ``RemoteSource``. Any number of
/// ``InvalidationSignal`` expressions can reset its local and in-memory state.
/// Partitioning belongs to
/// ``Bucket/init(_:partitionedBy:configuration:)`` rather than this capability
/// builder. The bucket validates the accumulated remote-source count when it
/// creates the corresponding atomic partition.
@resultBuilder
public enum BucketBuilder<Space: ContinuumKeySpace> {
    /// Combines the building blocks declared at one configuration level.
    public static func buildBlock(
        _ components: BucketConfiguration<Space>...
    ) -> BucketConfiguration<Space> {
        components.reduce(BucketConfiguration()) { result, component in
            result.merging(component)
        }
    }

    /// Adds a local source expression to the accumulated configuration.
    public static func buildExpression(
        _ expression: LocalSource<Space>
    ) -> BucketConfiguration<Space> {
        BucketConfiguration(localSources: [expression])
    }

    /// Adds a remote source expression to the accumulated configuration.
    public static func buildExpression(
        _ expression: RemoteSource<Space>
    ) -> BucketConfiguration<Space> {
        BucketConfiguration(remoteSources: [expression])
    }

    /// Adds an invalidation event stream to the accumulated configuration.
    public static func buildExpression(
        _ expression: InvalidationSignal
    ) -> BucketConfiguration<Space> {
        BucketConfiguration(invalidationSignals: [expression])
    }

    /// Includes a conditionally produced configuration when present.
    public static func buildOptional(
        _ component: BucketConfiguration<Space>?
    ) -> BucketConfiguration<Space> {
        component ?? BucketConfiguration()
    }

    /// Includes the first branch of a conditional configuration.
    public static func buildEither(
        first component: BucketConfiguration<Space>
    ) -> BucketConfiguration<Space> {
        component
    }

    /// Includes the second branch of a conditional configuration.
    public static func buildEither(
        second component: BucketConfiguration<Space>
    ) -> BucketConfiguration<Space> {
        component
    }

    /// Combines configuration produced by a loop.
    public static func buildArray(
        _ components: [BucketConfiguration<Space>]
    ) -> BucketConfiguration<Space> {
        components.reduce(BucketConfiguration()) { result, component in
            result.merging(component)
        }
    }
}
