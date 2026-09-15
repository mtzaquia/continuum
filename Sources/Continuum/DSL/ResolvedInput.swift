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
    /// Resolves foreign keys through an indexed bucket's entry loader.
    ///
    /// Existing entries remain observable. Out-of-list results are retained only
    /// by this composition while needed, without inserting them into the bucket.
    /// Resetting the bucket invalidates both. Explicit loads forward their policy;
    /// new keys use `.cached`. Entry failures fail the relationship, not the list,
    /// and remain until a successful relationship load or reset. Collection edits
    /// update retained values without clearing load errors.
    /// - Parameters:
    ///   - key: The foreign key on each root entry.
    ///   - bucket: The indexed collection with an optional ``LoadEntry`` capability.
    /// - Returns: A declaration contributing the root array and `[ID: Related]`.
    func resolving<Element: Sendable, ID: Hashable & Sendable, Related: Sendable>(
        _ key: KeyPath<Element, ID> & Sendable,
        from bucket: IndexedBucket<ID, Related>
    ) -> ResolvedInput<Element, ID, Related> where Value == [Element] {
        resolving(key, from: bucket.storage)
    }

    /// Resolves foreign keys through one selected indexed partition.
    ///
    /// Shares the indexed bucket overload's retention, observation, and reset rules.
    /// - Parameters:
    ///   - key: The foreign key on each root entry.
    ///   - partition: The indexed partition owning the entry loader.
    /// - Returns: A declaration contributing the root array and `[ID: Related]`.
    func resolving<Element: Sendable, ID: Hashable & Sendable, Related: Sendable>(
        _ key: KeyPath<Element, ID> & Sendable,
        from partition: BucketPartition<IndexedKey<ID, Related>>
    ) -> ResolvedInput<Element, ID, Related> where Value == [Element] {
        .init(root: self, key: key, select: { id in
            let entry = EntryRelationshipState(partition: partition, id: id)
            return RelationshipSource(read: { entry.read() }, load: { try await entry.load($0) })
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
        resolve: @escaping @Sendable @concurrent (ID, LoadPolicy) async throws -> Resolved
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
