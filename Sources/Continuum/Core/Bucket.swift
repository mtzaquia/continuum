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

import Observation

/// An observable data bucket for one typed key space.
///
/// An unpartitioned bucket owns one atomic snapshot. Use `partitionedBy:` when
/// several independently loaded snapshots share the same key shape:
///
/// ```swift
/// let accounts = Bucket(
///     AccountsData.all,
///     partitionedBy: Purpose.self
/// ) { purpose in
///     LocalSource {
///         try await database.accounts(purpose: purpose)
///     }
///     RemoteSource {
///         try await client.accounts(purpose: purpose)
///     }
/// }
/// ```
///
/// Select a configured partition with `accounts[.buy]`. Each selected
/// ``BucketPartition`` owns independent values, loading state, and coalesced
/// source work. Bucket state is main actor-isolated; source operations are
/// asynchronous, sendable closures.
@Observable
public final class Bucket<
    Space: ContinuumKeySpace,
    Scope: BucketScope
> {
    /// The key that establishes this bucket's namespace and snapshot behavior.
    public let keySpace: Space

    @ObservationIgnored
    private let unpartitioned: BucketPartition<Space>?

    @ObservationIgnored
    private let partitionConfiguration:
        (@MainActor @Sendable (Scope.Partition) -> BucketConfiguration<Space>)?

    @ObservationIgnored
    private var partitionStorage: [Scope.Partition: BucketPartition<Space>] = [:]

    /// Creates an observable in-memory data bucket.
    ///
    /// This bucket has no loading sources. Calling ``Bucket/load(using:)``
    /// emits a runtime warning; use mutations to manage its in-memory state.
    ///
    /// - Parameter keySpace: The key whose atomic snapshot this bucket owns.
    public init(_ keySpace: Space) where Scope == UnpartitionedBucketScope {
        self.keySpace = keySpace
        unpartitioned = BucketPartition(keySpace)
        partitionConfiguration = nil
    }

    /// Creates an observable data bucket with additive source capabilities.
    ///
    /// The builder is typed by `keySpace`, so sources infer the complete
    /// snapshot type without receiving the key again. A bucket accepts any
    /// number of local sources and invalidation signals and at most one remote
    /// source. Construction traps if the resulting configuration contains more
    /// than one ``RemoteSource``.
    ///
    /// - Parameters:
    ///   - keySpace: The key whose atomic snapshot this bucket owns.
    ///   - configuration: The sources attached to this bucket.
    public init(
        _ keySpace: Space,
        @BucketBuilder<Space> configuration: () -> BucketConfiguration<Space>
    ) where Scope == UnpartitionedBucketScope {
        self.keySpace = keySpace
        unpartitioned = BucketPartition(
            keySpace,
            configuration: configuration()
        )
        partitionConfiguration = nil
    }

    /// Creates a bucket containing independently loaded partitions.
    ///
    /// Partitioning is part of the bucket's structure. The configuration closure
    /// receives each selected partition once and builds that partition's source
    /// capabilities lazily. The first access to a partition traps if its
    /// configuration contains more than one ``RemoteSource``.
    ///
    /// - Parameters:
    ///   - keySpace: The key shape shared by every partition.
    ///   - partition: The stable type identifying independent snapshots.
    ///   - configuration: The sources for one selected partition.
    public init<Partition>(
        _ keySpace: Space,
        partitionedBy _: Partition.Type,
        @BucketBuilder<Space> configuration:
            @escaping @MainActor @Sendable (Partition) ->
                BucketConfiguration<Space>
    ) where Scope == PartitionedScope<Partition> {
        self.keySpace = keySpace
        unpartitioned = nil
        partitionConfiguration = configuration
    }

    // Work around a Swift 6.3 EarlyPerfInliner crash in the synthesized
    // deinitializer for this generic observable type.
    @_optimize(none)
    isolated deinit {}
}

public extension Bucket where Scope == UnpartitionedBucketScope {
    /// The bucket's complete loading state.
    var state: LoadState {
        storage.state
    }

    /// An opaque value that changes whenever this bucket returns to an
    /// unavailable state.
    ///
    /// Retain the last value handled by an observation source and compare it
    /// with this property to recognize reset independently from the current
    /// snapshot. Seed that retained value from the current value when an
    /// observation starts to avoid replaying a reset that happened before
    /// subscription.
    var resetValue: BucketResetValue {
        storage.resetValue
    }

    /// Whether source work is currently active for this bucket.
    var isLoading: Bool {
        storage.isLoading
    }

    /// Whether the bucket has established a complete snapshot.
    ///
    /// A successful empty indexed snapshot sets this property to `true`.
    var isLoaded: Bool {
        storage.isLoaded
    }

    /// The latest snapshot-loading, mutation, persistence, or invalidation error.
    ///
    /// Starting another load or mutation clears the previous error. A reset
    /// clears it on success. Continuation failures appear in
    /// ``nextPageError`` instead.
    var error: (any Error)? {
        storage.error
    }

    /// Returns the current value for one input without starting a load.
    ///
    /// - Parameter input: The value to read from the atomic snapshot.
    func value(for input: Space.Input) -> Space.Value? {
        storage.value(for: input)
    }

    /// Returns one value from the current snapshot.
    subscript(input: Space.Input) -> Space.Value? {
        storage[input]
    }

    /// The stored inputs in insertion order.
    var keys: [Space.Input] {
        storage.keys
    }

    /// The stored values in insertion order.
    var values: [Space.Value] {
        storage.values
    }

    /// The stored key-value pairs in insertion order.
    ///
    /// The returned array is an observation-tracked snapshot.
    var elements: [(key: Space.Input, value: Space.Value)] {
        storage.elements
    }

    /// The number of values in the current snapshot.
    var count: Int {
        storage.count
    }

    /// Whether the current snapshot contains no values.
    ///
    /// Use ``isLoaded`` to distinguish a successful empty snapshot from a
    /// bucket that has not established a snapshot.
    var isEmpty: Bool {
        storage.isEmpty
    }

    /// Loads and atomically publishes the complete snapshot.
    ///
    /// A cached load returns established memory immediately, then otherwise
    /// shares active work or tries local sources before remote. A
    /// cached-then-remote load publishes available cache data and returns after
    /// its required remote phase. A remote load supersedes active work and starts
    /// directly at the remote source. Before a remote result enters memory,
    /// writable local sources persist it in declaration order.
    /// Loading a bucket with no local or remote source emits a runtime warning;
    /// such a bucket is intended to be used as an in-memory bucket.
    ///
    /// - Parameter policy: The way established and local snapshots should be
    ///   treated.
    /// - Returns: The established or loaded complete snapshot.
    /// - Throws: ``ContinuumError/missingRemoteSource(namespace:)`` when remote
    ///   work is required but no ``RemoteSource`` is configured, or an error
    ///   raised while loading or persisting a source.
    @discardableResult
    func load(
        using policy: LoadPolicy = .cached
    ) async throws -> Space.Snapshot {
        try await storage.load(using: policy)
    }

    /// Inserts or replaces one value in the atomic snapshot.
    ///
    /// For an ``IndexedKey``, the key's index operation selects the entry and
    /// replacement retains its insertion position. This operation establishes a
    /// complete snapshot and supersedes source work. It immediately publishes
    /// and persists the submitted value. When ``RemoteSource`` declares
    /// ``Store``, Continuum then reconciles the server-authoritative value.
    ///
    /// - Parameter value: The value to insert or replace.
    /// - Throws: An error raised by ``Store`` or a writable ``LocalSource``.
    ///   A failed mutation restores the previously established snapshot in
    ///   observable memory and attempts to restore local persistence.
    func store(_ value: Space.Value) async throws {
        try await storage.store(value)
    }

    /// Removes one value from the atomic snapshot.
    ///
    /// Indexed snapshots remain established when their final value is removed.
    /// Removing a singleton value returns that bucket to an unloaded state.
    /// The normalized result is immediately published and persisted. When
    /// ``RemoteSource`` declares ``Remove``, that operation then confirms the
    /// mutation.
    ///
    /// - Parameter input: The value to remove.
    /// - Throws: An error raised by ``Remove`` or a writable ``LocalSource``.
    ///   A failed mutation restores the previously established snapshot in
    ///   observable memory and attempts to restore local persistence.
    func remove(_ input: Space.Input) async throws {
        try await storage.remove(input)
    }

    /// Forgets the current snapshot, pagination state, and writable local
    /// snapshots, returning the bucket to its initial state.
    ///
    /// Reset differs from an established empty indexed snapshot: reset makes
    /// ``isLoaded`` false, while a loaded `[]` is a known successful snapshot.
    /// Observable memory resets before writable local sources receive `nil` in
    /// declaration order. A failure restores the previous observable state,
    /// attempts to restore local persistence, and becomes ``error``.
    ///
    /// - Throws: An error raised by a writable ``LocalSource``.
    func reset() async throws {
        try await storage.reset()
    }

    /// Resets observable memory and writable local snapshots.
    ///
    /// This compatibility overload is deprecated; ``reset()`` now always
    /// removes writable local snapshots.
    ///
    /// - Parameter scope: Ignored. It is retained for source compatibility.
    /// - Throws: An error raised by a writable ``LocalSource``.
    @available(
        *,
        deprecated,
        message: "Use reset(); it always clears writable local snapshots."
    )
    func reset(including _: ResetScope) async throws {
        try await storage.reset()
    }
}

public extension Bucket
where
    Scope == UnpartitionedBucketScope,
    Space.Input == SingletonInput
{
    /// The current singleton value without starting a load.
    var value: Space.Value? {
        storage.value
    }

    /// Removes the singleton value and returns the bucket to an unloaded state.
    ///
    /// The value is immediately removed from observable memory and writable
    /// local sources before a configured remote ``Remove`` confirms it.
    ///
    /// - Throws: An error raised by ``Remove`` or a writable ``LocalSource``.
    ///   A failure restores the prior value in observable memory and attempts
    ///   to restore local persistence.
    func remove() async throws {
        try await storage.remove()
    }
}

public extension Bucket
where Scope == UnpartitionedBucketScope
{
    /// The current next-page loading state.
    var pagination: PaginationState {
        storage.pagination
    }

    /// Whether a continuation page is currently loading.
    var isLoadingNextPage: Bool {
        storage.isLoadingNextPage
    }

    /// Whether the latest remote page supplied another cursor.
    var hasNextPage: Bool {
        storage.hasNextPage
    }

    /// The latest next-page error, or `nil` after success, refresh, or reset.
    var nextPageError: (any Error)? {
        storage.nextPageError
    }

    /// Loads and merges the next remote page.
    ///
    /// Concurrent calls for the same cursor share one source operation.
    /// Array pages append using the key's ordered-index semantics. A
    /// ``NextPage`` with nested accumulation uses the incoming snapshot as its
    /// base and accumulates only the selected collection. When the latest page
    /// has no next cursor, this method returns the current snapshot without
    /// starting source work.
    ///
    /// Load the initial remote page with ``load(using:)`` before calling this
    /// method. A local-only initial load does not establish a remote cursor.
    ///
    /// - Returns: The complete merged snapshot after the page resolves.
    /// - Throws: ``ContinuumError/missingPaginatedRemoteSource(namespace:)`` when
    ///   the remote source is not paginated,
    ///   ``ContinuumError/initialPageNotLoaded(namespace:)`` before an initial
    ///   remote page, or an error raised while loading or persisting
    ///   ``NextPage``.
    @discardableResult
    func loadNext() async throws -> Space.Snapshot {
        try await storage.loadNext()
    }
}

public extension Bucket where Scope: PartitionedBucketScope {
    /// Returns the observable atomic bucket for one partition value.
    ///
    /// The first access creates the partition lazily. Later accesses with an
    /// equal value return the same partition and preserve its snapshot, state,
    /// and in-flight work.
    ///
    /// - Parameter partition: The stable identity of the list to select.
    subscript(partition: Scope.Partition) -> BucketPartition<Space> {
        if let existing = partitionStorage[partition] {
            return existing
        }

        guard let partitionConfiguration else {
            preconditionFailure("A partitioned bucket requires a source configuration.")
        }

        let created = BucketPartition(
            keySpace,
            configuration: partitionConfiguration(partition)
        )
        partitionStorage[partition] = created
        return created
    }
}

private extension Bucket where Scope == UnpartitionedBucketScope {
    var storage: BucketPartition<Space> {
        guard let unpartitioned else {
            preconditionFailure("An unpartitioned bucket requires atomic storage.")
        }
        return unpartitioned
    }
}
