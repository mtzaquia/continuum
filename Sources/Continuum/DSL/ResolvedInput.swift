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

/// A collection input paired with the values required by its foreign keys.
///
/// Create this declaration with `Input.resolving`. It contributes two transform
/// parameters: the root array and a dictionary of resolved snapshots. One
/// relationship per root declaration is supported; chaining is not supported.
@MainActor
public struct ResolvedInput<Element: Sendable, ID: Hashable & Sendable, Resolved: Sendable> {
    let root: Input<[Element]>
    let key: KeyPath<Element, ID> & Sendable
    let select: @MainActor (ID) -> RelationshipSource<Resolved>
}

public extension Input {
    /// Resolves and observes the partition selected by each distinct foreign key.
    ///
    /// Explicit loads finish the root action before loading selected partitions
    /// with the same policy. New keys introduced by observation use `.cached`.
    /// All required snapshots must be available before the root and dictionary
    /// reach the transform together. Selected partitions remain live; resets and
    /// failures propagate through the composition, and subsequent values recover.
    ///
    /// - Parameters:
    ///   - key: The foreign key on each root entry. Stored key paths must be sendable.
    ///   - bucket: The partitioned bucket owning the related snapshots.
    /// - Returns: A declaration contributing the array and `[ID: Space.Snapshot]`.
    func resolving<Element: Sendable, ID: Hashable & Sendable, Space: ContinuumKeySpace>(
        _ key: KeyPath<Element, ID> & Sendable,
        from bucket: Bucket<Space, PartitionedScope<ID>>
    ) -> ResolvedInput<Element, ID, Space.Snapshot> where Value == [Element] {
        .init(root: self, key: key, select: { id in
            let partition = bucket[id]
            let input = Input<Space.Snapshot>(partition)
            return RelationshipSource(read: input.read, load: { policy in
                try await partition.load(using: policy)
            })
        })
    }

    /// Resolves each distinct foreign key with a one-shot compatibility lookup.
    ///
    /// Returned values are retained only while their keys are needed. They are
    /// not observed after return. Prefer buckets for shared caching, persistence,
    /// mutations, or invalidation. Explicit loads forward their policy unchanged
    /// after loading the root; observation resolves new keys with `.cached`.
    /// Failures are retryable with another composition load. Cancellation and
    /// superseding root snapshots prevent obsolete results from being published.
    ///
    /// - Parameters:
    ///   - key: The foreign key on each root entry.
    ///   - resolve: Fetches one required snapshot using the supplied policy.
    /// - Returns: A declaration contributing the array and `[ID: Resolved]`.
    func resolving<Element: Sendable, ID: Hashable & Sendable, Resolved: Sendable>(
        _ key: KeyPath<Element, ID> & Sendable,
        resolve: @escaping @MainActor @Sendable (ID, LoadPolicy) async throws -> Resolved
    ) -> ResolvedInput<Element, ID, Resolved> where Value == [Element] {
        .init(root: self, key: key, select: { id in
            RelationshipSource(read: nil, load: { policy in try await resolve(id, policy) })
        })
    }
}

public extension CompositionBuilder {
    /// Adds the root array and its resolved dictionary as adjacent parameters.
    static func buildExpression<Element, ID, Resolved>(
        _ input: ResolvedInput<Element, ID, Resolved>
    ) -> CompositionInputs<[Element], [ID: Resolved]> {
        .init(makeInputs: {
            let state = RelationshipInputState(root: input.root.resolved(), key: input.key, select: input.select)
            state.start()
            return (
                Input(read: { state.rootState }, load: { policy in await state.load(policy) }),
                Input(read: { state.relatedState })
            )
        })
    }
}
