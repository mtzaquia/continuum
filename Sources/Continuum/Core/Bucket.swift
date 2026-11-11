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
@MainActor
@Observable
public final class Bucket<
    Space: ContinuumKeySpace,
    Scope: BucketScope,
    Operation: Sendable
> {
    /// The key that establishes this bucket's namespace and snapshot behavior.
    public let keySpace: Space

    @ObservationIgnored
    private let unpartitioned: BucketPartition<Space, Operation>?

    @ObservationIgnored
    private let staleAfter: Duration?

    @ObservationIgnored
    private let partitionConfiguration:
        (@MainActor @Sendable (Scope.Partition) -> BucketConfiguration<Space, Operation>)?

    @ObservationIgnored
    private var partitionStorage: [Scope.Partition: BucketPartition<Space, Operation>] = [:]

    /// Creates an observable bucket without loading sources.
    ///
    /// A bucket without loading sources can run remote-only mutation actions.
    /// Supply a ``LocalSource`` or ``RemoteSource`` to establish a snapshot.
    ///
    /// - Parameter keySpace: The key whose atomic snapshot this bucket owns.
    public convenience init(_ keySpace: Space)
    where Scope == UnpartitionedBucketScope, Operation == MutationAction<Space> {
        self.init(keySpace, staleAfter: nil, configuration: .init())
    }

    /// Creates an observable data bucket with configurable snapshot freshness.
    ///
    /// The builder is typed by `keySpace`, so sources infer the complete
    /// snapshot type without receiving the key again. A bucket accepts any
    /// number of local sources and invalidation signals and at most one remote
    /// source. Construction traps if the resulting configuration contains more
    /// than one ``RemoteSource``.
    ///
    /// With `staleAfter`, cached snapshot loads validate stale or unvalidated
    /// data remotely while keeping existing values readable. Only successful
    /// full remote loads restart freshness; expiration starts no work itself.
    /// Local snapshots have no freshness metadata and are exposed before remote
    /// validation on the first cached load. Local-only buckets ignore freshness.
    /// Entry loads keep their existing policies without a freshness interval.
    ///
    /// - Parameters:
    ///   - keySpace: The key whose atomic snapshot this bucket owns.
    ///   - staleAfter: How long a successful full remote snapshot remains fresh.
    ///     Must be nonnegative. `nil` preserves unlimited cache reuse; zero
    ///     requires remote validation on every cached snapshot load.
    ///   - configuration: The sources attached to this bucket.
    public convenience init(
        _ keySpace: Space,
        staleAfter: Duration? = nil,
        @BucketBuilder<Space> configuration: () -> BucketConfiguration<Space, Operation>
    ) where Scope == UnpartitionedBucketScope {
        self.init(keySpace, staleAfter: staleAfter, configuration: configuration())
    }

    private init(
        _ keySpace: Space,
        staleAfter: Duration?,
        configuration: BucketConfiguration<Space, Operation>
    ) where Scope == UnpartitionedBucketScope {
        self.keySpace = keySpace
        self.staleAfter = staleAfter
        unpartitioned = BucketPartition(
            keySpace,
            staleAfter: staleAfter,
            configuration: configuration
        )
        partitionConfiguration = nil
    }

    /// Creates independently loaded partitions with configurable snapshot freshness.
    ///
    /// Partitioning is part of the bucket's structure. The configuration closure
    /// receives each selected partition once and builds that partition's source
    /// capabilities lazily. The first access to a partition traps if its
    /// configuration contains more than one ``RemoteSource``.
    /// Each partition tracks freshness independently using the same duration.
    /// Cached snapshot loads expose unvalidated local data before refreshing
    /// remotely; only successful full remote loads restart the interval.
    /// Expiration starts no work itself. Entry loads have no freshness interval.
    ///
    /// - Parameters:
    ///   - keySpace: The key shape shared by every partition.
    ///   - partition: The stable type identifying independent snapshots.
    ///   - staleAfter: How long each successful full remote snapshot remains
    ///     fresh. Must be nonnegative. `nil` preserves unlimited cache reuse;
    ///     zero requires remote validation on every cached snapshot load.
    ///     Local-only partitions ignore freshness.
    ///   - configuration: The sources for one selected partition.
    public convenience init<Partition>(
        _ keySpace: Space,
        partitionedBy _: Partition.Type,
        staleAfter: Duration? = nil,
        @BucketBuilder<Space> configuration:
            @escaping @MainActor @Sendable (Partition) -> BucketConfiguration<Space, Operation>
    ) where Scope == PartitionedScope<Partition> {
        self.init(keySpace, staleAfter: staleAfter, partitionConfiguration: configuration)
    }

    private init(
        _ keySpace: Space,
        staleAfter: Duration?,
        partitionConfiguration: @escaping @MainActor @Sendable (Scope.Partition) ->
            BucketConfiguration<Space, Operation>
    ) {
        precondition(
            staleAfter.map { $0 >= .zero } ?? true,
            "staleAfter must be nonnegative."
        )
        self.keySpace = keySpace
        self.staleAfter = staleAfter
        unpartitioned = nil
        self.partitionConfiguration = partitionConfiguration
    }

    // Work around a Swift 6.3 EarlyPerfInliner crash in the synthesized
    // deinitializer for this generic observable type. This stays nonisolated
    // because it performs no actor-isolated cleanup.
    @_optimize(none)
    deinit {}
}

public extension Bucket where Scope == UnpartitionedBucketScope {
    /// The bucket's complete loading state.
    var state: LoadState {
        storage.state
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

    /// The latest collection load, entry load, mutation, persistence, or invalidation error.
    ///
    /// Starting source work or a mutation clears the previous error. A cached
    /// memory hit preserves it. A reset clears it on success. Continuation
    /// failures appear in ``nextPageError``.
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

    /// The current observable result or reset, initially `.reset`.
    ///
    /// An established snapshot remains a success when an operation fails. Read
    /// ``error`` for the operation failure. A collection load can publish a
    /// failure when no snapshot is available.
    var latest: Update<Space.Snapshot> { storage.latest }

    /// Loads and atomically publishes the complete snapshot.
    ///
    /// A cached load returns established memory immediately unless configured
    /// freshness requires remote validation. Stale values remain readable while
    /// the shared refresh completes. Otherwise it shares active work or tries
    /// local sources before remote. A cached-then-remote load publishes available
    /// cache data and returns after its required remote phase. A remote load
    /// waits for mutations and resets, then supersedes active reads and starts
    /// directly at the remote source. Writable local sources persist remote
    /// results in declaration order before publication. Cached-then-remote
    /// loads and cached loads without fresh memory also wait for mutations and
    /// resets.
    /// Loading a bucket with no local or remote source emits a runtime warning.
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

    /// Runs an operation in the bucket's mutation queue.
    ///
    /// The configured ``Mutate`` handler receives the operation and can
    /// optimistically edit the snapshot. Buckets without a custom handler
    /// accept a ``MutationAction`` closure. A failure rolls back owned edits.
    /// Reset cancels queued mutations and revokes a running handler's ability
    /// to publish or persist edits while it finishes its request.
    func mutate(_ operation: Operation) async throws {
        try await storage.mutate(operation)
    }

    /// Forgets the current snapshot, pagination state, and writable local
    /// snapshots, returning the bucket to its initial state.
    ///
    /// Reset differs from an established empty indexed snapshot: reset makes
    /// ``isLoaded`` false, while a loaded `[]` is a known successful snapshot.
    /// Reset revokes ownership from earlier work and clears observable memory
    /// before suspending. Writable local sources receive `nil` in
    /// declaration order, after already executing local writes finish. A
    /// failure restores the previous observable state, attempts to restore
    /// local persistence, and becomes ``error``. Caller cancellation restores
    /// state only while this reset still owns it and does not become ``error``.
    ///
    /// - Throws: An error raised by a writable ``LocalSource``.
    func reset() async throws {
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
    /// Concurrent calls for the same cursor share one source operation. Page
    /// work waits for pending mutations and resets before capturing its base
    /// snapshot; a later mutation supersedes an active page.
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
    /// and in-flight work. Accessed partitions remain retained until this
    /// bucket is released; reset clears a partition's data without evicting it.
    /// Use bounded query identities for long-lived buckets.
    ///
    /// - Parameter partition: The stable identity of the list to select.
    subscript(partition: Scope.Partition) -> BucketPartition<Space, Operation> {
        if let existing = partitionStorage[partition] {
            return existing
        }

        guard let partitionConfiguration else {
            preconditionFailure("A partitioned bucket requires a source configuration.")
        }

        let created = BucketPartition(
            keySpace,
            staleAfter: staleAfter,
            configuration: partitionConfiguration(partition)
        )
        partitionStorage[partition] = created
        return created
    }
}

extension Bucket where Scope == UnpartitionedBucketScope {
    var storage: BucketPartition<Space, Operation> {
        guard let unpartitioned else {
            preconditionFailure("An unpartitioned bucket requires atomic storage.")
        }
        return unpartitioned
    }
}


extension Bucket: AsyncSequence where Scope == UnpartitionedBucketScope {
    /// The result or reset emitted by this bucket.
    public typealias Element = Update<Space.Snapshot>
    /// An independent iterator over this bucket's outcomes.
    public typealias AsyncIterator = BucketPartition<Space, Operation>.AsyncIterator

    /// Observes outcomes without starting a load.
    ///
    /// Initial unavailability is silent. Each iterator has an independent,
    /// unbounded buffer and retains the bucket until cancelled or released.
    nonisolated public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(observation: makeSourceObservation(self))
    }
}


extension Bucket: CompositionResetSource where Scope == UnpartitionedBucketScope {
    var compositionResetRevision: UInt { storage.compositionResetRevision }
}

public extension Bucket where Scope == UnpartitionedBucketScope {
    /// Loads one indexed entry, persisting a replacement only when it is already present.
    ///
    /// `.cached` returns an existing entry; other policies fetch remotely. Missing
    /// entries are returned without insertion. See ``BucketPartition/load(id:using:)``.
    /// - Parameters:
    ///   - id: The index to load.
    ///   - policy: The entry loading policy. Defaults to `.cached`.
    /// - Returns: The existing or fetched entry.
    /// - Throws: A source, persistence, configuration, identity, or cancellation error.
    @discardableResult
    func load<ID, Value>(id: ID, using policy: LoadPolicy = .cached) async throws -> Value
    where Space == IndexedKey<ID, Value> {
        try await storage.load(id: id, using: policy)
    }
}
