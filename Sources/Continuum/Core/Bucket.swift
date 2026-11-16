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

/// Owns one observable snapshot and its source operations.
///
/// Select an independently loaded bucket with ``PartitionedBucket/subscript(_:)``.
@MainActor
@Observable
public final class Bucket<Space: ContinuumKeySpace, Operation: Sendable>: Sendable {
    private enum LoadOrigin {
        case local
        case remote

        var debugName: String {
            switch self {
            case .local:
                "local"
            case .remote:
                "remote"
            }
        }
    }

    private enum PaginationCheckpoint {
        case unavailable
        case available(RemotePageContinuation<Space>?)

        var continuation: RemotePageContinuation<Space>? {
            guard case .available(let continuation) = self else {
                return nil
            }
            return continuation
        }
    }

    private struct LoadOutcome {
        let snapshot: Space.Snapshot
        let origin: LoadOrigin
        let nextPage: RemotePageContinuation<Space>?

        init(
            snapshot: Space.Snapshot,
            origin: LoadOrigin,
            nextPage: RemotePageContinuation<Space>? = nil
        ) {
            self.snapshot = snapshot
            self.origin = origin
            self.nextPage = nextPage
        }
    }

    private struct InFlight {
        let epoch: UInt
        let generation: UInt
        let operationID: String
        let task: Task<LoadOutcome, any Error>
        var continuesToRemote = false
    }

    private struct NextPageFlight {
        let epoch: UInt
        let generation: UInt
        let identifier: UInt
        let operationID: String
        let task: Task<RemoteSnapshot<Space>, any Error>
    }

    private struct CompletedNextPageFlight {
        let generation: UInt
        let identifier: UInt
        let result: Result<Space.Snapshot, any Error>
    }

    private struct QueuedMutation {
        let kind: BucketMutationKind
        let operationID: String
        let task: Task<Void, any Error>
    }

    /// The key that establishes this bucket's identity and snapshot behavior.
    public let keySpace: Space

    private var loading = false
    private var operationError: (any Error)?
    private var pageError: (any Error)?

    private var latestUpdate: Update<Space.Snapshot> = .reset
    private(set) var resetRevision: UInt = 0

    @ObservationIgnored
    private let localSources: [LocalSource<Space>]

    @ObservationIgnored
    private let remoteSource: RemoteSource<Space>?

    @ObservationIgnored
    private let mutationHandler: Mutate<Operation, Space>?

    @ObservationIgnored
    private let staleAfter: Duration?

    @ObservationIgnored
    private let now: @MainActor @Sendable () -> ContinuousClock.Instant

    @ObservationIgnored
    private var lastRemoteLoad: ContinuousClock.Instant?

    @ObservationIgnored
    private let persistence: LocalPersistenceCoordinator<Space>

    @ObservationIgnored
    private let logIdentity: BucketLogIdentity

    @ObservationIgnored
    private var entryTasks: [Space.Input: Task<Space.Value, any Error>] = [:]

    @ObservationIgnored
    private var inFlight: InFlight?

    @ObservationIgnored
    private var completedLoad: Result<LoadOutcome, any Error>?

    private var paginationCheckpoint = PaginationCheckpoint.unavailable

    @ObservationIgnored
    private var nextPageInFlight: NextPageFlight?

    @ObservationIgnored
    private var completedNextPageFlight: CompletedNextPageFlight?

    @ObservationIgnored
    private var nextPageIdentifier: UInt = 0

    @ObservationIgnored
    private var generation: UInt = 0

    @ObservationIgnored
    private var mutationEpoch: UInt = 0

    @ObservationIgnored
    private var mutationTail: Task<Void, any Error>?

    @ObservationIgnored
    private var mutationTasks: [UInt: Task<Void, any Error>] = [:]

    @ObservationIgnored
    private var runningMutation: UInt?

    @ObservationIgnored
    private var mutationIdentifier: UInt = 0

    @ObservationIgnored
    private var invalidationTasks: [Task<Void, Never>] = []

    /// Creates an observable bucket without loading sources.
    ///
    /// A bucket without loading sources can run remote-only mutation actions.
    /// Supply a ``LocalSource`` or ``RemoteSource`` to establish a snapshot.
    ///
    /// - Parameter keySpace: The key whose atomic snapshot this bucket owns.
    public convenience init(_ keySpace: Space)
    where Operation == MutationAction<Space> {
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
    ) {
        self.init(keySpace, staleAfter: staleAfter, configuration: configuration())
    }

    init(
        _ keySpace: Space,
        staleAfter: Duration? = nil,
        now: @escaping @MainActor @Sendable () -> ContinuousClock.Instant = { .now },
        configuration: BucketConfiguration<Space, Operation>
    ) {
        precondition(
            staleAfter.map { $0 >= .zero } ?? true,
            "staleAfter must be nonnegative."
        )
        precondition(
            configuration.remoteSources.count <= 1,
            "A data bucket accepts at most one RemoteSource."
        )
        precondition(
            configuration.mutations.count <= 1,
            "A data bucket accepts at most one Mutate handler."
        )

        let localSources = configuration.localSources
        let logIdentity = BucketLogIdentity(
            namespace: keySpace.namespace,
            version: keySpace.version
        )
        self.keySpace = keySpace
        self.staleAfter = staleAfter
        self.now = now
        self.localSources = localSources
        self.logIdentity = logIdentity
        remoteSource = configuration.remoteSources.first
        mutationHandler = configuration.mutations.first
        persistence = LocalPersistenceCoordinator(
            sources: localSources,
            logIdentity: logIdentity
        )

        continuumDebug(
            .bucketConfigured(
                logIdentity,
                localSources: localSources.count,
                writableSources: localSources.filter {
                    $0.persistence != nil
                }.count,
                hasRemote: remoteSource != nil,
                paginated: remoteSource?.isPaginated == true,
                invalidationSignals:
                    configuration.invalidationSignals.count
            )
        )

        for signal in configuration.invalidationSignals {
            let task = Task { [weak self] in
                await signal.observation(
                    {
                        await self?.invalidate()
                    },
                    { error in
                        self?.recordInvalidationError(error)
                    }
                )
            }
            invalidationTasks.append(task)
        }
    }

    deinit {
        for task in entryTasks.values { task.cancel() }
        inFlight?.task.cancel()
        nextPageInFlight?.task.cancel()
        for task in mutationTasks.values {
            task.cancel()
        }
        for task in invalidationTasks {
            task.cancel()
        }
    }

    /// The bucket's complete loading state.
    public var state: LoadState {
        LoadState(isLoading: loading, isLoaded: isLoaded, error: operationError)
    }

    /// Whether a full snapshot load is active for this bucket.
    public var isLoading: Bool {
        loading
    }

    /// Whether the bucket has established a complete snapshot.
    ///
    /// A successful empty indexed snapshot sets this property to `true`.
    public var isLoaded: Bool {
        snapshot != nil
    }

    /// The latest collection load, entry load, mutation, persistence, or invalidation error.
    ///
    /// A collection load retains the previous error until it settles. A cached
    /// memory hit preserves it. An entry fetch, mutation, or reset clears it.
    /// Continuation failures appear in ``nextPageError``.
    public var error: (any Error)? {
        operationError
    }

    /// Returns the current value for one input without starting a load.
    ///
    /// - Parameter input: The value to read from the atomic snapshot.
    public func value(for input: Space.Input) -> Space.Value? {
        guard let snapshot = snapshot else { return nil }
        return keySpace.value(for: input, in: snapshot)
    }

    /// Returns one value from the current snapshot.
    public subscript(input: Space.Input) -> Space.Value? {
        value(for: input)
    }

    /// The stored inputs in insertion order.
    public var keys: [Space.Input] {
        guard let snapshot else { return [] }
        return keySpace.inputs(in: snapshot)
    }

    /// The stored values in insertion order.
    public var values: [Space.Value] {
        guard let snapshot else { return [] }
        return keySpace.values(in: snapshot)
    }

    /// The stored key-value pairs in insertion order.
    ///
    /// The returned array is an observation-tracked snapshot.
    public var elements: [(key: Space.Input, value: Space.Value)] {
        guard let snapshot else { return [] }
        return zip(
            keySpace.inputs(in: snapshot),
            keySpace.values(in: snapshot)
        ).map { key, value in
            (key: key, value: value)
        }
    }

    /// The number of values in the current snapshot.
    public var count: Int {
        values.count
    }

    /// Whether the current snapshot contains no values.
    ///
    /// Use ``isLoaded`` to distinguish a successful empty snapshot from a
    /// bucket that has not established a snapshot.
    public var isEmpty: Bool {
        values.isEmpty
    }

    /// The current observable result or reset, initially `.reset`.
    ///
    /// An established snapshot remains a success when an operation fails. Read
    /// ``error`` for the operation failure. A collection load can publish a
    /// failure when no snapshot is available. Starting a retry preserves that
    /// outcome until the load publishes a new result or the partition is reset.
    public var latest: Update<Space.Snapshot> { latestUpdate }

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
    public func load(
        using policy: LoadPolicy = .cached
    ) async throws -> Space.Snapshot {
        try await ContinuumLogContext.withOperation {
            continuumDebug(
                .loadRequested(
                    logIdentity,
                    policy: policy,
                    established: isLoaded
                )
            )
            if localSources.isEmpty, remoteSource == nil {
                continuumWarning(
                    .loadRequestedWithoutSources(
                        logIdentity,
                        policy: policy,
                        established: isLoaded
                    )
                )
            }

            switch policy {
            case .cached:
                return try await loadCached()
            case .cachedThenRemote:
                return try await loadCachedThenRemote()
            case .remote:
                return try await loadRemote()
            }
        }
    }

    func load(in context: SourceLoadContext) async {
        // The bucket publishes operation failures for observers.
        _ = try? await load(using: context.policy)
    }

    /// Runs an operation in this bucket's mutation queue.
    ///
    /// The handler can publish and persist an optimistic edit before calling
    /// a service. A failure rolls back owned edits. Reset cancels queued
    /// mutations and revokes ownership from a running handler, which can
    /// finish its request without publishing to the bucket.
    public func mutate(_ operation: Operation) async throws {
        let action: MutationAction<Space>
        if let handler = mutationHandler?.operation {
            action = { mutation in try await handler(operation, mutation) }
        } else if let direct = operation as? MutationAction<Space> {
            action = direct
        } else {
            throw ContinuumError.missingMutationHandler(namespace: keySpace.namespace)
        }
        let queued = enqueue(kind: .mutate) { [self] epoch in
            try await performMutation(epoch: epoch, action)
        }
        try await finish(queued)
    }

    /// Clears the snapshot, error, freshness, pagination, and writable local data.
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
    /// - Throws: An error raised by a writable ``LocalSource``. Cancellation
    ///   and source-generation checks prevent superseded work from publishing.
    public func reset() async throws {
        try Task.checkCancellation()
        let queued = ContinuumLogContext.withOperation {
            let previousUpdate = latestUpdate
            let previousRemoteLoad = lastRemoteLoad
            let previousPagination = paginationCheckpoint

            logSupersededWork(replacement: BucketMutationKind.resetLocal.rawValue)
            supersedeMutations()
            supersedeSourceWork()
            let ownership = MutationOwnership(epoch: mutationEpoch, generation: generation)
            publishMutation(nil)
            clearPagination()

            return enqueue(kind: .resetLocal) { [self] _ in
                do {
                    try requireCurrent(ownership)
                    try await persistence.persist(nil)
                    try requireCurrent(ownership)
                } catch {
                    guard ownsState(ownership) else { throw CancellationError() }
                    let failure: any Error = Task.isCancelled ? CancellationError() : error
                    await rollback(
                        to: previousUpdate,
                        after: failure,
                        ownership: ownership,
                        lastRemoteLoad: previousRemoteLoad,
                        restoring: previousPagination
                    )
                    throw failure
                }
            }
        }
        try await finish(queued)
    }
}

extension Bucket: UpdateSource, LoadableSource, CompositionResetSource {}

public extension Bucket where Space.Input == SingletonInput {
    /// The current singleton value without starting a load.
    var value: Space.Value? {
        value(for: .shared)
    }
}

public extension Bucket {
    /// The current next-page loading state.
    var pagination: PaginationState {
        PaginationState(isLoading: isLoadingNextPage, hasNextPage: hasNextPage, error: nextPageError)
    }

    /// Whether a continuation page is currently loading.
    var isLoadingNextPage: Bool {
        access(keyPath: \.isLoadingNextPage)
        return nextPageInFlight != nil
    }

    /// Whether the latest remote page supplied another cursor.
    var hasNextPage: Bool {
        paginationCheckpoint.continuation != nil
    }

    /// The latest next-page error, or `nil` after success, refresh, or reset.
    var nextPageError: (any Error)? {
        pageError
    }

    /// Loads and merges the next remote page.
    ///
    /// Concurrent calls for the same cursor share one source operation. Page
    /// work waits for pending mutations and resets before capturing its base
    /// snapshot; a later mutation supersedes an active page.
    /// Array pages append using the key's ordered-index semantics. A
    /// A remote source with nested accumulation uses the incoming snapshot as its
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
    ///   the next page.
    @discardableResult
    func loadNext() async throws -> Space.Snapshot {
        try await ContinuumLogContext.withOperation {
            continuumDebug(.paginationRequested(logIdentity))
            return try await loadNextPage()
        }
    }
}

private extension Bucket {
    var snapshot: Space.Snapshot? { latestUpdate.snapshot }

    private func publishFailure(_ error: any Error) {
        loading = false
        operationError = error
        latestUpdate = snapshot.map { .result(.success($0)) } ?? .result(.failure(error))
    }

    var currentOperationID: String {
        ContinuumLogContext.operationID
            ?? ContinuumLogContext.makeIdentifier()
    }

    func snapshotCount(_ snapshot: Space.Snapshot) -> Int {
        keySpace.values(in: snapshot).count
    }

    func logSupersededWork(replacement: String) {
        let replacementOperationID = currentOperationID
        if let inFlight {
            ContinuumLogContext.withOperationID(inFlight.operationID) {
                continuumDebug(
                    .operationSuperseded(
                        logIdentity,
                        operation: "load",
                        replacement: replacement,
                        replacementOperationID: replacementOperationID
                    )
                )
            }
        }
        if let nextPageInFlight {
            ContinuumLogContext.withOperationID(nextPageInFlight.operationID) {
                continuumDebug(
                    .operationSuperseded(
                        logIdentity,
                        operation: "next-page",
                        replacement: replacement,
                        replacementOperationID: replacementOperationID
                    )
                )
            }
        }
    }

    private struct MutationOwnership {
        let epoch: UInt
        let generation: UInt
    }

    private func ownsState(_ ownership: MutationOwnership) -> Bool {
        mutationEpoch == ownership.epoch && generation == ownership.generation
    }

    private func requireCurrent(_ ownership: MutationOwnership) throws {
        try Task.checkCancellation()
        guard ownsState(ownership) else { throw CancellationError() }
    }

    private func supersedeMutations() {
        mutationEpoch &+= 1
        for (identifier, task) in mutationTasks where identifier != runningMutation {
            task.cancel()
        }
        mutationTasks.removeAll()
        mutationTail = nil
        runningMutation = nil
    }

    private func finishMutation(_ identifier: UInt) {
        mutationTasks[identifier] = nil
        if runningMutation == identifier { runningMutation = nil }
        if mutationIdentifier == identifier { mutationTail = nil }
    }

    func waitForMutations() async throws {
        let epoch = mutationEpoch
        try Task.checkCancellation()
        while let pending = mutationTail {
            _ = await pending.result
            try Task.checkCancellation()
            guard mutationEpoch == epoch else { throw CancellationError() }
        }
    }

    private func enqueue(
        kind: BucketMutationKind,
        operation: @escaping @MainActor (UInt) async throws -> Void
    ) -> QueuedMutation {
        ContinuumLogContext.withOperation {
            let epoch = mutationEpoch
            let predecessor = mutationTail
            continuumDebug(
                .mutationRequested(
                    logIdentity,
                    kind: kind,
                    queued: predecessor != nil
                )
            )

            mutationIdentifier &+= 1
            let identifier = mutationIdentifier
            let task = Task<Void, any Error> { @MainActor [weak self] in
                defer { self?.finishMutation(identifier) }
                if let predecessor {
                    _ = await predecessor.result
                }
                guard let self, mutationEpoch == epoch else {
                    throw CancellationError()
                }

                // Reset has already cleared memory and owns its cancellation rollback.
                if kind != .resetLocal { try Task.checkCancellation() }
                if kind == .mutate { runningMutation = identifier }
                try await operation(epoch)
            }
            mutationTail = task
            mutationTasks[identifier] = task
            return QueuedMutation(kind: kind, operationID: currentOperationID, task: task)
        }
    }

    private func finish(_ queued: QueuedMutation) async throws {
        try await ContinuumLogContext.withOperationID(queued.operationID) {
            let task = queued.task
            let kind = queued.kind
            do {
                try await withTaskCancellationHandler {
                    try await task.value
                } onCancel: {
                    task.cancel()
                }
                continuumDebug(
                    .mutationCompleted(
                        logIdentity,
                        kind: kind,
                        count: snapshot.map(snapshotCount) ?? 0,
                        established: isLoaded
                    )
                )
            } catch is CancellationError {
                continuumDebug(
                    .operationCancelled(
                        logIdentity,
                        operation: kind.rawValue
                    )
                )
                throw CancellationError()
            } catch {
                continuumDebug(
                    .operationFailed(
                        logIdentity,
                        operation: kind.rawValue,
                        error: error
                    )
                )
                throw error
            }
        }
    }

    private func performMutation(
        epoch: UInt,
        _ operation: (Mutation<Space>) async throws -> Void
    ) async throws {
        logSupersededWork(replacement: BucketMutationKind.mutate.rawValue)
        supersedeSourceWork()
        loading = false
        operationError = nil
        let ownership = MutationOwnership(epoch: epoch, generation: generation)
        let previousUpdate = latestUpdate
        let previousRemoteLoad = lastRemoteLoad
        var published = false

        let mutation = Mutation(
            keySpace: keySpace,
            read: { [weak self] in self?.snapshot },
            ownsState: { [weak self] in self?.ownsState(ownership) == true },
            publish: { [weak self] snapshot in
                guard let self else { throw CancellationError() }
                try requireCurrent(ownership)
                published = true
                let normalized = snapshot.map(keySpace.normalized)
                publishMutation(normalized)
                try await persistence.persist(normalized)
            },
            remoteStarted: { [logIdentity] in
                continuumDebug(.remoteMutationStarted(logIdentity, kind: .mutate))
            }
        )

        do {
            try await operation(mutation)
            try mutation.finish()
            try Task.checkCancellation()
        } catch {
            mutation.invalidate()
            let failure: any Error = Task.isCancelled ? CancellationError() : error
            guard ownsState(ownership) else { throw failure }
            if published {
                await rollback(
                    to: previousUpdate,
                    after: failure,
                    ownership: ownership,
                    lastRemoteLoad: previousRemoteLoad,
                    restoring: nil
                )
            } else if !(failure is CancellationError) {
                operationError = failure
            }
            throw failure
        }
    }

    private func replaceEntry(_ value: Space.Value, ownership: MutationOwnership) async throws {
        try requireCurrent(ownership)
        let id = keySpace.input(for: value)
        guard let previous = snapshot, keySpace.value(for: id, in: previous) != nil else { return }
        // A pending page captured an older collection. Keep its cursor for retry.
        cancelNextPageWork()
        let updated = keySpace.normalized(keySpace.storing(value, in: previous))
        do {
            try await persistence.persist(updated)
            try requireCurrent(ownership)
            latestUpdate = .result(.success(updated))
        } catch {
            if ownsState(ownership) {
                try? await persistence.persist(previous, restoring: true)
            }
            throw error
        }
    }

    private func publishMutation(_ snapshot: Space.Snapshot?) {
        loading = false
        operationError = nil
        if let snapshot {
            latestUpdate = .result(.success(snapshot))
        } else {
            lastRemoteLoad = nil
            // An explicit reset also invalidates derived entries when this bucket is empty.
            resetRevision &+= 1
            latestUpdate = .reset
        }
    }

    private func rollback(
        to previousUpdate: Update<Space.Snapshot>,
        after error: any Error,
        ownership: MutationOwnership,
        lastRemoteLoad: ContinuousClock.Instant?,
        restoring paginationCheckpoint: PaginationCheckpoint?
    ) async {
        guard ownsState(ownership) else { return }

        self.lastRemoteLoad = lastRemoteLoad
        loading = false
        operationError = error is CancellationError ? nil : error
        if let paginationCheckpoint {
            self.paginationCheckpoint = paginationCheckpoint
            pageError = nil
        }
        latestUpdate = previousUpdate
        try? await persistence.persist(previousUpdate.snapshot, restoring: true)
    }

    func invalidate() async {
        await ContinuumLogContext.withOperation {
            continuumDebug(.invalidationReceived(logIdentity))
            do {
                try await reset()
            } catch is CancellationError {
                return
            } catch {
                // The failed reset records its error in observable state. Keep
                // the event sequence alive so a later invalidation can retry.
            }
        }
    }

    func recordInvalidationError(_ error: any Error) {
        guard (error is CancellationError) == false else { return }
        ContinuumLogContext.withOperation {
            continuumDebug(
                .invalidationStreamFailed(
                    logIdentity,
                    error: error
                )
            )
            operationError = error
        }
    }

    func loadCached() async throws -> Space.Snapshot {
        if isLoaded == false || requiresRemoteValidation {
            try await waitForMutations()
        }
        if requiresRemoteValidation {
            return try await loadCachedThenRemote()
        }
        if isLoaded, let snapshot {
            continuumDebug(
                .loadReturnedMemory(
                    logIdentity,
                    count: snapshotCount(snapshot)
                )
            )
            return snapshot
        }

        if let inFlight {
            continuumDebug(
                .loadJoined(
                    logIdentity,
                    activeOperationID: inFlight.operationID
                )
            )
            return try await resolve(inFlight).snapshot
        }

        let flight = makeLoadFlight(using: .cached)
        return try await resolve(flight).snapshot
    }

    private var requiresRemoteValidation: Bool {
        guard let staleAfter, remoteSource != nil else { return false }
        guard let lastRemoteLoad else { return true }
        return lastRemoteLoad.duration(to: now()) >= staleAfter
    }

    func loadCachedThenRemote() async throws -> Space.Snapshot {
        try await waitForMutations()
        guard let remoteSource else {
            throw missingRemoteSource()
        }

        if let nextPageInFlight {
            let replacementOperationID = currentOperationID
            ContinuumLogContext.withOperationID(nextPageInFlight.operationID) {
                continuumDebug(
                    .operationSuperseded(
                        logIdentity,
                        operation: "next-page",
                        replacement: "cached-then-remote-load",
                        replacementOperationID: replacementOperationID
                    )
                )
            }
        }
        cancelNextPageWork()

        if let current = inFlight {
            continuumDebug(
                .loadJoined(
                    logIdentity,
                    activeOperationID: current.operationID
                )
            )
            inFlight?.continuesToRemote = true
            return try await resolve(current).snapshot
        }

        let flight = makeLoadFlight(using: .cachedThenRemote, remoteSource: remoteSource)
        return try await resolve(flight).snapshot
    }

    func loadRemote() async throws -> Space.Snapshot {
        guard let remoteSource else {
            throw missingRemoteSource()
        }

        let epoch = mutationEpoch
        repeat {
            try await waitForMutations()
            guard mutationEpoch == epoch else { throw CancellationError() }
        } while mutationTail != nil
        logSupersededWork(replacement: "remote-load")
        supersedeSourceWork()
        let flight = makeLoadFlight(using: .remote, remoteSource: remoteSource)
        return try await resolve(flight).snapshot
    }

    private func makeLoadFlight(
        using policy: LoadPolicy,
        remoteSource: RemoteSource<Space>? = nil
    ) -> InFlight {
        let generation = nextGeneration()
        let shouldReadLocal = policy == .cached || (policy == .cachedThenRemote && !isLoaded)
        let localSources = localSources
        let remoteSource = remoteSource ?? self.remoteSource
        let keySpace = keySpace
        let persistence = persistence
        let logIdentity = logIdentity
        let operationID = currentOperationID
        let task = Task { [weak self] in
            if shouldReadLocal,
               let snapshot = try await Self.firstLocalSnapshot(
                   from: localSources,
                   keySpace: keySpace,
                   persistence: persistence,
                   logIdentity: logIdentity
               ) {
                if policy == .cached {
                    return LoadOutcome(snapshot: snapshot, origin: .local)
                }
                self?.publishCached(snapshot, generation: generation)
            }

            guard let remoteSource else {
                throw ContinuumError.missingRemoteSource(namespace: keySpace.namespace)
            }
            return try await Self.remoteOutcome(
                from: remoteSource,
                keySpace: keySpace,
                persistence: persistence,
                logIdentity: logIdentity
            )
        }
        let flight = InFlight(epoch: mutationEpoch, generation: generation, operationID: operationID, task: task)
        inFlight = flight
        loading = true
        return flight
    }

    private static func firstLocalSnapshot(
        from sources: [LocalSource<Space>],
        keySpace: Space,
        persistence: LocalPersistenceCoordinator<Space>,
        logIdentity: BucketLogIdentity
    ) async throws -> Space.Snapshot? {
        for (offset, source) in sources.enumerated() {
            let index = offset + 1
            continuumDebug(
                .localSourceStarted(
                    logIdentity,
                    index: index,
                    total: sources.count
                )
            )
            try Task.checkCancellation()
            if let snapshot = try await persistence.read(source.operation) {
                try Task.checkCancellation()
                continuumDebug(
                    .localSourceHit(
                        logIdentity,
                        index: index,
                        count: keySpace.values(in: snapshot).count
                    )
                )
                return keySpace.normalized(snapshot)
            }
            continuumDebug(
                .localSourceMissed(
                    logIdentity,
                    index: index
                )
            )
        }

        return nil
    }

    private static func remoteOutcome(
        from source: RemoteSource<Space>,
        keySpace: Space,
        persistence: LocalPersistenceCoordinator<Space>,
        logIdentity: BucketLogIdentity
    ) async throws -> LoadOutcome {
        continuumDebug(.remoteSourceStarted(logIdentity))
        try Task.checkCancellation()
        let remote = try await source.operation()
        try Task.checkCancellation()
        let snapshot = keySpace.normalized(remote.snapshot)
        try await persistence.persist(snapshot)
        try Task.checkCancellation()
        return LoadOutcome(
            snapshot: snapshot,
            origin: .remote,
            nextPage: remote.nextPage
        )
    }

    private func resolve(_ initialFlight: InFlight) async throws -> LoadOutcome {
        var flight = initialFlight
        while true {
            let result = await flight.task.result
            guard mutationEpoch == initialFlight.epoch else { throw CancellationError() }
            guard generation == flight.generation else {
                if let replacement = completedLoad {
                    return try replacement.get()
                }
                guard let replacement = inFlight else { throw CancellationError() }
                flight = replacement
                continue
            }

            if case .success(let outcome) = result, outcome.origin == .local,
               inFlight?.continuesToRemote == true, let remoteSource {
                publishCached(outcome.snapshot, generation: flight.generation)
                flight = ContinuumLogContext.withOperationID(flight.operationID) {
                    makeLoadFlight(using: .remote, remoteSource: remoteSource)
                }
                continue
            }

            if inFlight?.generation == flight.generation {
                finishLoad(result, flight: flight)
            }
            return try result.get()
        }
    }

    func missingRemoteSource() -> ContinuumError {
        let error = ContinuumError.missingRemoteSource(
            namespace: keySpace.namespace
        )

        if inFlight == nil {
            publishFailure(error)
        }

        continuumDebug(
            .operationFailed(
                logIdentity,
                operation: "load",
                error: error
            )
        )
        return error
    }

    func nextGeneration() -> UInt {
        for task in entryTasks.values { task.cancel() }
        entryTasks.removeAll()
        completedLoad = nil
        generation &+= 1
        return generation
    }

    func supersedeSourceWork() {
        inFlight?.task.cancel()
        _ = nextGeneration()
        inFlight = nil
        cancelNextPageWork()
    }

    func publishCached(_ snapshot: Space.Snapshot, generation: UInt) {
        guard self.generation == generation,
              inFlight?.generation == generation else {
            return
        }

        latestUpdate = .result(.success(snapshot))
        if let operationID = inFlight?.operationID {
            ContinuumLogContext.withOperationID(operationID) {
                continuumDebug(
                    .cachedSnapshotPublished(
                        logIdentity,
                        count: snapshotCount(snapshot)
                    )
                )
            }
        }
    }

    private func finishLoad(_ result: Result<LoadOutcome, any Error>, flight: InFlight) {
        inFlight = nil
        completedLoad = result

        ContinuumLogContext.withOperationID(flight.operationID) {
            switch result {
            case .success(let outcome):
                if outcome.origin == .remote {
                    lastRemoteLoad = now()
                    paginationCheckpoint = remoteSource?.isPaginated == true ? .available(outcome.nextPage) : .unavailable
                    pageError = nil
                }
                loading = false
                operationError = nil
                latestUpdate = .result(.success(outcome.snapshot))
                continuumDebug(.loadCompleted(
                    logIdentity,
                    origin: outcome.origin.debugName,
                    count: snapshotCount(outcome.snapshot),
                    hasNextPage: outcome.nextPage != nil
                ))
            case .failure(let error):
                publishFailure(error)
                logLoadFailure(error, operation: "load")
            }
        }
    }

    func logLoadFailure(_ error: any Error, operation: String) {
        if error is CancellationError {
            continuumDebug(.operationCancelled(logIdentity, operation: operation))
        } else {
            continuumDebug(.operationFailed(logIdentity, operation: operation, error: error))
        }
    }

    func cancelNextPageWork() {
        nextPageInFlight?.task.cancel()
        setNextPageFlight(nil)
        completedNextPageFlight = nil
        pageError = nil
    }

    // Keep the task accessible to deinit while observing changes to its presence.
    private func setNextPageFlight(_ flight: NextPageFlight?) {
        withMutation(keyPath: \.isLoadingNextPage) {
            nextPageInFlight = flight
        }
    }

    func clearPagination() {
        paginationCheckpoint = .unavailable
    }
}

private extension Bucket {
    func loadNextPage() async throws -> Space.Snapshot {
        try await waitForMutations()
        guard remoteSource?.isPaginated == true else {
            let error = ContinuumError.missingPaginatedRemoteSource(
                namespace: keySpace.namespace
            )
            recordPaginationError(error)
            throw error
        }

        if let current = inFlight {
            continuumDebug(
                .paginationWaitingForLoad(
                    logIdentity,
                    activeOperationID: current.operationID
                )
            )
            _ = try await resolve(current)
            try await waitForMutations()
        }

        if let current = nextPageInFlight {
            continuumDebug(
                .paginationJoined(
                    logIdentity,
                    activeOperationID: current.operationID
                )
            )
            return try await resolveNextPage(current)
        }

        guard case .available(let continuation) = paginationCheckpoint,
              let establishedSnapshot = snapshot else {
            let error = ContinuumError.initialPageNotLoaded(
                namespace: keySpace.namespace
            )
            recordPaginationError(error)
            throw error
        }

        guard let continuation else {
            continuumDebug(
                .paginationExhausted(
                    logIdentity,
                    count: snapshotCount(establishedSnapshot)
                )
            )
            return establishedSnapshot
        }

        let flight = makeNextPageFlight(
            continuation,
            appendingTo: establishedSnapshot
        )
        return try await resolveNextPage(flight)
    }

    private func makeNextPageFlight(
        _ continuation: RemotePageContinuation<Space>,
        appendingTo establishedSnapshot: Space.Snapshot
    ) -> NextPageFlight {
        nextPageIdentifier &+= 1
        let keySpace = keySpace
        let persistence = persistence
        let operationID = currentOperationID
        let task = Task {
            try Task.checkCancellation()
            let page = try await continuation.operation()
            try Task.checkCancellation()
            let merged = keySpace.normalized(
                continuation.merge(
                    establishedSnapshot,
                    page.snapshot
                )
            )
            try await persistence.persist(merged)
            try Task.checkCancellation()
            return RemoteSnapshot(
                snapshot: merged,
                nextPage: page.nextPage
            )
        }
        let flight = NextPageFlight(
            epoch: mutationEpoch,
            generation: generation,
            identifier: nextPageIdentifier,
            operationID: operationID,
            task: task
        )
        setNextPageFlight(flight)
        completedNextPageFlight = nil
        pageError = nil
        return flight
    }

    private func resolveNextPage(_ initialFlight: NextPageFlight) async throws -> Space.Snapshot {
        var flight = initialFlight
        while true {
            let result = await flight.task.result
            guard mutationEpoch == initialFlight.epoch else { throw CancellationError() }
            if let completed = completedNextPageReplacement(for: flight) {
                return try completed.get()
            }
            guard generation == flight.generation else {
                guard let replacement = nextPageInFlight else { throw CancellationError() }
                flight = replacement
                continue
            }

            guard nextPageInFlight?.identifier == flight.identifier else {
                _ = try result.get()
                guard let snapshot else { throw CancellationError() }
                return snapshot
            }

            setNextPageFlight(nil)
            completedNextPageFlight = CompletedNextPageFlight(
                generation: flight.generation,
                identifier: flight.identifier,
                result: result.map { $0.snapshot }
            )
            ContinuumLogContext.withOperationID(flight.operationID) {
                switch result {
                case .success(let page):
                    paginationCheckpoint = .available(page.nextPage)
                    pageError = nil
                    latestUpdate = .result(.success(page.snapshot))
                    continuumDebug(.paginationCompleted(
                        logIdentity, count: snapshotCount(page.snapshot), hasNextPage: page.nextPage != nil
                    ))
                case .failure(let error):
                    pageError = error
                    logLoadFailure(error, operation: "next-page")
                }
            }
            return try result.get().snapshot
        }
    }

    private func completedNextPageReplacement(
        for flight: NextPageFlight
    ) -> Result<
        Space.Snapshot,
        any Error
    >? {
        guard let completedNextPageFlight,
              completedNextPageFlight.generation == flight.generation,
              completedNextPageFlight.identifier == flight.identifier else {
            return nil
        }
        return completedNextPageFlight.result
    }

    func recordPaginationError(_ error: any Error) {
        continuumDebug(.operationFailed(logIdentity, operation: "next-page", error: error))
        pageError = error
    }
}


extension Bucket: AsyncSequence {
    /// The result or reset emitted by this bucket.
    public typealias Element = Update<Space.Snapshot>

    /// Observes outcomes without starting a load.
    ///
    /// Each iterator receives the current result if one exists. Initial
    /// unavailability is silent. A reset clears a previous result while the
    /// bucket remains unavailable. Changes may coalesce. Each iterator has
    /// an independent, unbounded buffer and retains its bucket until cancelled
    /// or released.
    nonisolated public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(observation: makeSourceObservation(self))
    }

    /// An independent subscription to a bucket or selected partition.
    nonisolated public struct AsyncIterator: AsyncIteratorProtocol {
        private var iterator: AsyncStream<Element>.Iterator
        private let lifetime: BucketObservationLifetime

        nonisolated init(
            observation: (stream: AsyncStream<Element>, cancel: @Sendable () -> Void)
        ) {
            iterator = observation.stream.makeAsyncIterator()
            lifetime = BucketObservationLifetime(observation.cancel)
        }

        /// Waits for an outcome, returning nil after cancellation.
        @concurrent public mutating func next() async -> Element? {
            await iterator.next()
        }
    }
}

public extension Bucket {
    /// Loads one entry, replacing and persisting it only if already in the collection.
    ///
    /// `.cached` returns an existing entry; other policies fetch remotely. Fetches
    /// for the same ID share work except `.remote`, which replaces earlier work.
    /// Missing entries are returned without insertion
    /// or persistence. Ordering, pagination, and the collection's availability are
    /// preserved. Entry failures are reported through ``error`` and thrown to the caller.
    /// Resets and superseding collection operations cancel pending entry loads.
    ///
    /// - Parameters:
    ///   - id: The index to load.
    ///   - policy: Whether an existing entry may satisfy the request. Defaults to `.cached`.
    /// - Returns: The existing or fetched entry.
    /// - Throws: A source or persistence error, `CancellationError`, or
    ///   ``ContinuumError`` for a missing loader or mismatched index.
    @discardableResult
    func load<ID, Value>(id: ID, using policy: LoadPolicy = .cached) async throws -> Value
    where Space == IndexedKey<ID, Value> {
        try Task.checkCancellation()
        if policy == .cached, let value = self[id] { return value }
        let task: Task<Value, any Error>
        if policy.replacesActiveLoad { entryTasks[id]?.cancel() }
        if !policy.replacesActiveLoad, let existing = entryTasks[id] {
            task = existing
        } else {
            task = Task { @MainActor [weak self] in
                try Task.checkCancellation()
                guard let self else { throw CancellationError() }
                try await waitForMutations()
                if let flight = inFlight { _ = try await resolve(flight) }
                try Task.checkCancellation()
                if policy == .cached, let value = self[id] { return value }
                let generation = generation
                if operationError != nil { operationError = nil }
                do {
                    guard let loader = remoteSource?.loadEntry else {
                        throw ContinuumError.missingEntrySource(namespace: keySpace.namespace)
                    }
                    let wasMember = self[id] != nil
                    let value = try await loader(id)
                    try Task.checkCancellation()
                    guard self.generation == generation else { throw CancellationError() }
                    guard keySpace.input(for: value) == id else {
                        throw ContinuumError.mismatchedEntryIdentity(namespace: keySpace.namespace)
                    }
                    if wasMember {
                        let queued = enqueue(kind: .loadEntry) { [self] epoch in
                            try await replaceEntry(value, ownership: .init(epoch: epoch, generation: generation))
                        }
                        try await finish(queued)
                    }
                    try Task.checkCancellation()
                    guard self.generation == generation else { throw CancellationError() }
                    return value
                } catch {
                    if self.generation == generation, !Task.isCancelled, !(error is CancellationError) {
                        operationError = error
                    }
                    throw error
                }
            }
            entryTasks[id] = task
        }
        defer { if entryTasks[id] == task { entryTasks[id] = nil } }
        let value = try await task.value
        try Task.checkCancellation()
        return value
    }
}
