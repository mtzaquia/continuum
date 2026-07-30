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

/// An observable view over one atomic data-bucket partition.
///
/// A partition owns one complete snapshot, loading state, and coalesced source
/// operation. Select it from a partitioned ``Bucket`` with a partition value.
/// An unpartitioned bucket forwards the same API directly.
///
/// The view is main actor-isolated. Source operations are asynchronous, and a
/// successful snapshot is published to observation-tracked readers on the main
/// actor.
@Observable
public final class BucketPartition<Space: ContinuumKeySpace> {
    private enum StoredSnapshot {
        case absent
        case present(Space.Snapshot)
    }

    private enum StreamPublication {
        case result
        case reset
    }

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

    private enum Mutation: Sendable {
        case store(Space.Value)
        case remove(Space.Input)
        case reset

        var logKind: BucketMutationKind {
            switch self {
            case .store:
                .store
            case .remove:
                .remove
            case .reset:
                .resetLocal
            }
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
        let generation: UInt
        let operationID: String
        let task: Task<LoadOutcome, any Error>
    }

    private struct CompletedFlight {
        let generation: UInt
        let result: Result<LoadOutcome, any Error>
    }

    private struct NextPageFlight {
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

    /// The key that establishes this bucket's identity and snapshot behavior.
    public let keySpace: Space

    private var storedSnapshot: StoredSnapshot = .absent
    private var loadState = LoadState()
    private var paginationState = PaginationState()
    private var streamVersion: UInt = 0

    @ObservationIgnored
    private var resetStreamVersion: UInt?

    @ObservationIgnored
    private let localSources: [LocalSource<Space>]

    @ObservationIgnored
    private let remoteSource: RemoteSource<Space>?

    @ObservationIgnored
    private let persistence: LocalPersistenceCoordinator<Space>

    @ObservationIgnored
    private let logIdentity: BucketLogIdentity

    @ObservationIgnored
    private var inFlight: InFlight?

    @ObservationIgnored
    private var completedFlight: CompletedFlight?

    @ObservationIgnored
    private var paginationCheckpoint = PaginationCheckpoint.unavailable

    @ObservationIgnored
    private var nextPageInFlight: NextPageFlight?

    @ObservationIgnored
    private var completedNextPageFlight: CompletedNextPageFlight?

    @ObservationIgnored
    private var nextPageIdentifier: UInt = 0

    @ObservationIgnored
    private var remoteContinuationGeneration: UInt?

    @ObservationIgnored
    private var generation: UInt = 0

    @ObservationIgnored
    private var mutationEpoch: UInt = 0

    @ObservationIgnored
    private var mutationTail: Task<Void, any Error>?

    @ObservationIgnored
    private var mutationTasks: [UInt: Task<Void, any Error>] = [:]

    @ObservationIgnored
    private var mutationIdentifier: UInt = 0

    @ObservationIgnored
    private var invalidationTasks: [Task<Void, Never>] = []

    init(
        _ keySpace: Space,
        configuration: BucketConfiguration<Space> = BucketConfiguration()
    ) {
        precondition(
            configuration.remoteSources.count <= 1,
            "A data bucket accepts at most one RemoteSource."
        )

        let localSources = configuration.localSources
        let logIdentity = BucketLogIdentity(
            namespace: keySpace.namespace,
            version: keySpace.version
        )
        self.keySpace = keySpace
        self.localSources = localSources
        self.logIdentity = logIdentity
        remoteSource = configuration.remoteSources.first
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
        loadState
    }

    /// Whether source work is currently active for this bucket.
    public var isLoading: Bool {
        loadState.isLoading
    }

    /// Whether the bucket has established a complete snapshot.
    ///
    /// A successful empty indexed snapshot sets this property to `true`.
    public var isLoaded: Bool {
        loadState.isLoaded
    }

    /// The latest snapshot-loading, mutation, persistence, or invalidation error.
    ///
    /// Starting another load or mutation clears the previous error. A reset
    /// clears it on success. Continuation failures appear in
    /// ``nextPageError`` instead.
    public var error: (any Error)? {
        loadState.error
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

    /// Creates a sequence of snapshot results and reset transitions.
    ///
    /// The sequence immediately emits an established snapshot or current error.
    /// An untouched or loading bucket without a snapshot remains silent, while
    /// an established empty snapshot emits a successful empty value. Reset emits
    /// ``BucketUpdate/reset`` only while the bucket remains unavailable; a
    /// replacement available before observation resumes emits its result directly.
    ///
    /// Creating the sequence does not start a load. Each call creates an
    /// independent observation that ends when iteration is cancelled.
    public func updates() -> AsyncStream<BucketUpdate<Space.Snapshot>> {
        let baselineVersion = streamVersion

        return AsyncStream(
            bufferingPolicy: .unbounded
        ) { continuation in
            let producer = Task { @MainActor [weak self] in
                guard let self else {
                    continuation.finish()
                    return
                }

                let (changes, changeContinuation) = AsyncStream<Void>.makeStream(
                    bufferingPolicy: .bufferingNewest(1)
                )
                var changeIterator = changes.makeAsyncIterator()
                var handledVersion = baselineVersion
                var needsInitialEvaluation = true

                defer {
                    changeContinuation.finish()
                    continuation.finish()
                }

                while Task.isCancelled == false {
                    let version = withObservationTracking {
                        self.streamVersion
                    } onChange: {
                        changeContinuation.yield()
                    }

                    guard needsInitialEvaluation
                            || version != handledVersion else {
                        guard await changeIterator.next() != nil else { return }
                        continue
                    }

                    needsInitialEvaluation = false
                    handledVersion = version

                    if self.resetStreamVersion == version,
                       self.currentStreamResult == nil {
                        await Task.yield()
                        guard self.streamVersion == version else {
                            continue
                        }
                    }

                    let update: BucketUpdate<Space.Snapshot>? =
                        if let result = self.currentStreamResult {
                            .result(result)
                        } else if self.resetStreamVersion == version,
                                  version != baselineVersion {
                            .reset
                        } else {
                            nil
                        }

                    if let update,
                       case .terminated = continuation.yield(update) {
                        return
                    }

                    guard await changeIterator.next() != nil else { return }
                }
            }

            continuation.onTermination = { @Sendable _ in
                producer.cancel()
            }
        }
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
    public func load(
        using policy: LoadPolicy = .cached
    ) async throws -> Space.Snapshot {
        try await ContinuumLogContext.withOperation {
            continuumDebug(
                .loadRequested(
                    logIdentity,
                    policy: policy,
                    established: loadState.isLoaded
                )
            )
            if localSources.isEmpty, remoteSource == nil {
                continuumWarning(
                    .loadRequestedWithoutSources(
                        logIdentity,
                        policy: policy,
                        established: loadState.isLoaded
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
    public func store(_ value: Space.Value) async throws {
        try await enqueue(.store(value))
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
    public func remove(_ input: Space.Input) async throws {
        try await enqueue(.remove(input))
    }

    /// Forgets the current snapshot, pagination state, and writable local
    /// snapshots, returning the partition to its initial state.
    ///
    /// Reset differs from an established empty indexed snapshot: reset makes
    /// ``isLoaded`` false, while a loaded `[]` is a known successful snapshot.
    /// Observable memory resets before writable local sources receive `nil` in
    /// declaration order. A failure restores the previous observable state,
    /// attempts to restore local persistence, and becomes ``error``.
    ///
    /// - Throws: An error raised by a writable ``LocalSource``. Cancellation
    ///   and source-generation checks prevent superseded work from publishing.
    public func reset() async throws {
        try await ContinuumLogContext.withOperation {
            continuumDebug(
                .mutationRequested(
                    logIdentity,
                    kind: .resetLocal,
                    queued: false
                )
            )
            mutationEpoch &+= 1
            for task in mutationTasks.values {
                task.cancel()
            }
            mutationTasks.removeAll()
            mutationTail = nil

            do {
                try await perform(.reset, epoch: mutationEpoch)
                continuumDebug(
                    .mutationCompleted(
                        logIdentity,
                        kind: .resetLocal,
                        count: snapshot.map(snapshotCount) ?? 0,
                        established: loadState.isLoaded
                    )
                )
            } catch is CancellationError {
                continuumDebug(
                    .operationCancelled(
                        logIdentity,
                        operation: BucketMutationKind.resetLocal.rawValue
                    )
                )
                throw CancellationError()
            } catch {
                continuumDebug(
                    .operationFailed(
                        logIdentity,
                        operation: BucketMutationKind.resetLocal.rawValue,
                        error: error
                    )
                )
                throw error
            }
        }
    }

    /// Schedules a reset without waiting for persistence to finish.
    ///
    /// This compatibility overload is deprecated because it cannot report a
    /// persistence failure. Use ``reset()`` with `try await` to observe
    /// completion and errors.
    @available(
        *,
        deprecated,
        message: "Use try await reset() to observe completion and errors."
    )
    public func reset() {
        Task { @MainActor [weak self] in
            try? await self?.reset()
        }
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
    public func reset(including _: ResetScope) async throws {
        try await reset()
    }
}

public extension BucketPartition where Space.Input == SingletonInput {
    /// The current singleton value without starting a load.
    var value: Space.Value? {
        value(for: .shared)
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
        try await remove(.shared)
    }
}

public extension BucketPartition {
    /// The current next-page loading state.
    var pagination: PaginationState {
        paginationState
    }

    /// Whether a continuation page is currently loading.
    var isLoadingNextPage: Bool {
        paginationState.isLoading
    }

    /// Whether the latest remote page supplied another cursor.
    var hasNextPage: Bool {
        paginationState.hasNextPage
    }

    /// The latest next-page error, or `nil` after success, refresh, or reset.
    var nextPageError: (any Error)? {
        paginationState.error
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
        try await ContinuumLogContext.withOperation {
            continuumDebug(.paginationRequested(logIdentity))
            return try await loadNextPage()
        }
    }
}

private extension BucketPartition {
    var snapshot: Space.Snapshot? {
        switch storedSnapshot {
        case .absent:
            nil
        case .present(let snapshot):
            .some(snapshot)
        }
    }

    var currentStreamResult: Result<Space.Snapshot, any Error>? {
        if let error = loadState.error {
            return .failure(error)
        }
        guard loadState.isLoaded, let snapshot else {
            return nil
        }
        return .success(snapshot)
    }

    private func publishStreamState(
        _ publication: StreamPublication = .result,
        changes: () -> Void
    ) {
        changes()
        let version = streamVersion &+ 1

        if case .reset = publication {
            resetStreamVersion = version
        }

        streamVersion = version
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

    private func enqueue(_ mutation: Mutation) async throws {
        try await ContinuumLogContext.withOperation {
            let epoch = mutationEpoch
            let predecessor = mutationTail
            let kind = mutation.logKind
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
                if let predecessor {
                    _ = await predecessor.result
                }
                try Task.checkCancellation()

                guard let self, mutationEpoch == epoch else {
                    throw CancellationError()
                }

                try await perform(mutation, epoch: epoch)
            }
            mutationTail = task
            mutationTasks[identifier] = task

            defer {
                mutationTasks[identifier] = nil
                if mutationIdentifier == identifier {
                    mutationTail = nil
                }
            }

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
                        established: loadState.isLoaded
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

    private func perform(_ mutation: Mutation, epoch: UInt) async throws {
        logSupersededWork(replacement: mutation.logKind.rawValue)
        supersedeSourceWork()
        loadState = LoadState(isLoaded: loadState.isLoaded)
        let mutationGeneration = generation
        let previousSnapshot = snapshot
        let previousPaginationCheckpoint = paginationCheckpoint
        let resetsPagination: Bool
        let publishesReset: Bool

        do {
            let updated: Space.Snapshot?

            switch mutation {
            case .store(let submittedValue):
                updated = keySpace.normalized(
                    keySpace.storing(
                        submittedValue,
                        in: previousSnapshot
                    )
                )
                resetsPagination = false
                publishesReset = false
            case .remove(let input):
                updated = keySpace.removing(input, from: previousSnapshot)
                    .map(keySpace.normalized)
                resetsPagination = false
                publishesReset = previousSnapshot != nil && updated == nil
            case .reset:
                updated = nil
                resetsPagination = true
                publishesReset = true
            }

            publishMutation(
                updated,
                publication: publishesReset ? .reset : .result
            )
            if resetsPagination {
                clearPagination()
            }
            try await persistence.persist(updated)
            try Task.checkCancellation()

            guard mutationEpoch == epoch,
                  generation == mutationGeneration else {
                throw CancellationError()
            }

            switch mutation {
            case .store(let submittedValue):
                guard let store = remoteSource?.store else { break }

                continuumDebug(
                    .remoteMutationStarted(
                        logIdentity,
                        kind: .store
                    )
                )
                let authoritativeValue = try await store.operation(submittedValue)
                try Task.checkCancellation()

                let submittedInput = keySpace.input(for: submittedValue)
                let authoritativeInput = keySpace.input(for: authoritativeValue)
                let baseSnapshot = if submittedInput == authoritativeInput {
                    updated
                } else {
                    keySpace.removing(submittedInput, from: updated)
                }
                let reconciled = keySpace.normalized(
                    keySpace.storing(authoritativeValue, in: baseSnapshot)
                )

                publishMutation(reconciled)
                try await persistence.persist(reconciled)
                try Task.checkCancellation()

                guard mutationEpoch == epoch,
                      generation == mutationGeneration else {
                    throw CancellationError()
                }
            case .remove(let input):
                guard let remove = remoteSource?.remove else { break }

                continuumDebug(
                    .remoteMutationStarted(
                        logIdentity,
                        kind: .remove
                    )
                )
                try await remove.operation(input)
                try Task.checkCancellation()
            case .reset:
                break
            }
        } catch {
            if (error is CancellationError) == false {
                await rollback(
                    to: previousSnapshot,
                    after: error,
                    epoch: epoch,
                    generation: mutationGeneration,
                    restoring: resetsPagination
                        ? previousPaginationCheckpoint
                        : nil
                )
            }
            throw error
        }
    }

    private func publishMutation(
        _ snapshot: Space.Snapshot?,
        publication: StreamPublication = .result
    ) {
        publishStreamState(publication) {
            if let snapshot {
                storedSnapshot = .present(snapshot)
                loadState = LoadState(isLoaded: true)
            } else {
                storedSnapshot = .absent
                loadState = LoadState()
            }
        }
    }

    private func rollback(
        to snapshot: Space.Snapshot?,
        after error: any Error,
        epoch: UInt,
        generation: UInt,
        restoring paginationCheckpoint: PaginationCheckpoint?
    ) async {
        guard mutationEpoch == epoch,
              self.generation == generation else {
            return
        }

        publishStreamState {
            if let snapshot {
                storedSnapshot = .present(snapshot)
            } else {
                storedSnapshot = .absent
            }
            loadState = LoadState(
                isLoaded: snapshot != nil,
                error: error
            )
            if let paginationCheckpoint {
                self.paginationCheckpoint = paginationCheckpoint
                paginationState = PaginationState(
                    hasNextPage: paginationCheckpoint.continuation != nil
                )
            }
        }
        try? await persistence.persist(snapshot)
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
            publishStreamState {
                loadState = LoadState(
                    isLoaded: loadState.isLoaded,
                    error: error
                )
            }
        }
    }

    func loadCached() async throws -> Space.Snapshot {
        if loadState.isLoaded, let snapshot {
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

        let flight = makeCachedFlight()
        return try await resolve(flight).snapshot
    }

    func loadCachedThenRemote() async throws -> Space.Snapshot {
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
            remoteContinuationGeneration = current.generation
            return try await resolve(current).snapshot
        }

        let flight = makeCachedThenRemoteFlight(remoteSource)
        return try await resolve(flight).snapshot
    }

    func loadRemote() async throws -> Space.Snapshot {
        guard let remoteSource else {
            throw missingRemoteSource()
        }

        logSupersededWork(replacement: "remote-load")
        supersedeSourceWork()
        let flight = makeRemoteFlight(remoteSource)
        return try await resolve(flight).snapshot
    }

    private func makeCachedFlight() -> InFlight {
        let generation = nextGeneration()
        let localSources = localSources
        let remoteSource = remoteSource
        let namespace = keySpace.namespace
        let keySpace = keySpace
        let persistence = persistence
        let logIdentity = logIdentity
        let operationID = currentOperationID
        let task = Task {
            if let snapshot = try await Self.firstLocalSnapshot(
                from: localSources,
                keySpace: keySpace,
                logIdentity: logIdentity
            ) {
                return LoadOutcome(
                    snapshot: snapshot,
                    origin: .local
                )
            }

            guard let remoteSource else {
                throw ContinuumError.missingRemoteSource(namespace: namespace)
            }

            return try await Self.remoteOutcome(
                from: remoteSource,
                keySpace: keySpace,
                persistence: persistence,
                logIdentity: logIdentity
            )
        }
        let flight = InFlight(
            generation: generation,
            operationID: operationID,
            task: task
        )
        start(flight)
        return flight
    }

    private func makeCachedThenRemoteFlight(
        _ remoteSource: RemoteSource<Space>
    ) -> InFlight {
        let shouldLoadLocalSources = loadState.isLoaded == false
        let generation = nextGeneration()
        let localSources = localSources
        let keySpace = keySpace
        let persistence = persistence
        let logIdentity = logIdentity
        let operationID = currentOperationID
        let task = Task { [weak self] in
            if shouldLoadLocalSources,
               let snapshot = try await Self.firstLocalSnapshot(
                   from: localSources,
                   keySpace: keySpace,
                   logIdentity: logIdentity
               ) {
                self?.publishCached(
                    snapshot,
                    generation: generation
                )
            }

            return try await Self.remoteOutcome(
                from: remoteSource,
                keySpace: keySpace,
                persistence: persistence,
                logIdentity: logIdentity
            )
        }
        let flight = InFlight(
            generation: generation,
            operationID: operationID,
            task: task
        )
        start(flight)
        return flight
    }

    private func makeRemoteFlight(
        _ remoteSource: RemoteSource<Space>
    ) -> InFlight {
        let generation = nextGeneration()
        let keySpace = keySpace
        let persistence = persistence
        let logIdentity = logIdentity
        let operationID = currentOperationID
        let task = Task {
            try await Self.remoteOutcome(
                from: remoteSource,
                keySpace: keySpace,
                persistence: persistence,
                logIdentity: logIdentity
            )
        }
        let flight = InFlight(
            generation: generation,
            operationID: operationID,
            task: task
        )
        start(flight)
        return flight
    }

    private static func firstLocalSnapshot(
        from sources: [LocalSource<Space>],
        keySpace: Space,
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
            if let snapshot = try await source.operation() {
                try Task.checkCancellation()
                continuumDebug(
                    .localSourceHit(
                        logIdentity,
                        index: index,
                        count: keySpace.values(in: snapshot).count
                    )
                )
                return snapshot
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

    private func resolve(
        _ initialFlight: InFlight
    ) async throws -> LoadOutcome {
        var flight = initialFlight

        while true {
            do {
                let outcome = try await flight.task.value

                guard generation == flight.generation else {
                    if let replacement = completedReplacement(
                        for: flight.generation
                    ) {
                        return try replacement.get()
                    }
                    if let replacement = inFlight {
                        flight = replacement
                        continue
                    }
                    throw CancellationError()
                }

                let normalized = LoadOutcome(
                    snapshot: keySpace.normalized(outcome.snapshot),
                    origin: outcome.origin,
                    nextPage: outcome.nextPage
                )

                if normalized.origin == .local,
                   remoteContinuationGeneration == flight.generation,
                   let remoteSource {
                    publishCached(
                        normalized.snapshot,
                        generation: flight.generation
                    )
                    remoteContinuationGeneration = nil
                    flight = ContinuumLogContext.withOperationID(
                        flight.operationID
                    ) {
                        makeRemoteFlight(remoteSource)
                    }
                    continue
                }

                publish(normalized, generation: flight.generation)
                return normalized
            } catch {
                guard generation == flight.generation else {
                    if let replacement = completedReplacement(
                        for: flight.generation
                    ) {
                        return try replacement.get()
                    }
                    if let replacement = inFlight {
                        flight = replacement
                        continue
                    }
                    throw CancellationError()
                }

                finish(with: error, generation: flight.generation)
                throw error
            }
        }
    }

    private func completedReplacement(
        for requestedGeneration: UInt
    ) -> Result<LoadOutcome, any Error>? {
        guard let completedFlight else {
            return nil
        }

        if completedFlight.generation == requestedGeneration
            || completedFlight.generation == generation {
            return completedFlight.result
        }

        return nil
    }

    func missingRemoteSource() -> ContinuumError {
        let error = ContinuumError.missingRemoteSource(
            namespace: keySpace.namespace
        )

        if inFlight == nil {
            publishStreamState {
                loadState = LoadState(
                    isLoaded: loadState.isLoaded,
                    error: error
                )
            }
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

    private func start(_ flight: InFlight) {
        inFlight = flight
        completedFlight = nil
        loadState = LoadState(
            isLoading: true,
            isLoaded: loadState.isLoaded
        )
    }

    func nextGeneration() -> UInt {
        generation &+= 1
        return generation
    }

    func supersedeSourceWork() {
        inFlight?.task.cancel()
        nextPageInFlight?.task.cancel()
        _ = nextGeneration()
        inFlight = nil
        completedFlight = nil
        remoteContinuationGeneration = nil
        nextPageInFlight = nil
        completedNextPageFlight = nil
        paginationState = PaginationState(
            hasNextPage: paginationCheckpoint.continuation != nil
        )
    }

    func publishCached(_ snapshot: Space.Snapshot, generation: UInt) {
        guard self.generation == generation,
              inFlight?.generation == generation else {
            return
        }

        publishStreamState {
            storedSnapshot = .present(keySpace.normalized(snapshot))
            loadState = LoadState(
                isLoading: true,
                isLoaded: true
            )
        }
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

    private func publish(_ outcome: LoadOutcome, generation: UInt) {
        guard self.generation == generation else { return }

        if inFlight?.generation == generation {
            let operationID = inFlight?.operationID
            publishStreamState {
                storedSnapshot = .present(outcome.snapshot)
                inFlight = nil
                remoteContinuationGeneration = nil
                completedFlight = CompletedFlight(
                    generation: generation,
                    result: .success(outcome)
                )
                loadState = LoadState(isLoaded: true)

                if outcome.origin == .remote {
                    paginationCheckpoint = if remoteSource?.isPaginated == true {
                        .available(outcome.nextPage)
                    } else {
                        .unavailable
                    }
                    paginationState = PaginationState(
                        hasNextPage: outcome.nextPage != nil
                    )
                }
            }

            if let operationID {
                ContinuumLogContext.withOperationID(operationID) {
                    continuumDebug(
                        .loadCompleted(
                            logIdentity,
                            origin: outcome.origin.debugName,
                            count: snapshotCount(outcome.snapshot),
                            hasNextPage: outcome.nextPage != nil
                        )
                    )
                }
            }
        }
    }

    func finish(with error: any Error, generation: UInt) {
        guard self.generation == generation,
              inFlight?.generation == generation else {
            return
        }

        let operationID = inFlight?.operationID
        publishStreamState {
            inFlight = nil
            remoteContinuationGeneration = nil
            completedFlight = CompletedFlight(
                generation: generation,
                result: .failure(error)
            )
            loadState = LoadState(
                isLoaded: loadState.isLoaded,
                error: error
            )
        }
        if let operationID {
            ContinuumLogContext.withOperationID(operationID) {
                if error is CancellationError {
                    continuumDebug(
                        .operationCancelled(
                            logIdentity,
                            operation: "load"
                        )
                    )
                } else {
                    continuumDebug(
                        .operationFailed(
                            logIdentity,
                            operation: "load",
                            error: error
                        )
                    )
                }
            }
        }
    }

    func cancelNextPageWork() {
        nextPageInFlight?.task.cancel()
        nextPageInFlight = nil
        completedNextPageFlight = nil
        paginationState = PaginationState(
            hasNextPage: paginationCheckpoint.continuation != nil
        )
    }

    func clearPagination() {
        paginationCheckpoint = .unavailable
        nextPageInFlight = nil
        completedNextPageFlight = nil
        paginationState = PaginationState()
    }
}

private extension BucketPartition {
    func loadNextPage() async throws -> Space.Snapshot {
        guard remoteSource?.isPaginated == true else {
            let error = ContinuumError.missingPaginatedRemoteSource(
                namespace: keySpace.namespace
            )
            recordPaginationError(error)
            continuumDebug(
                .operationFailed(
                    logIdentity,
                    operation: "next-page",
                    error: error
                )
            )
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

        guard case .available(let continuation) = paginationCheckpoint else {
            let error = ContinuumError.initialPageNotLoaded(
                namespace: keySpace.namespace
            )
            recordPaginationError(error)
            continuumDebug(
                .operationFailed(
                    logIdentity,
                    operation: "next-page",
                    error: error
                )
            )
            throw error
        }

        guard let establishedSnapshot = snapshot else {
            let error = ContinuumError.initialPageNotLoaded(
                namespace: keySpace.namespace
            )
            recordPaginationError(error)
            continuumDebug(
                .operationFailed(
                    logIdentity,
                    operation: "next-page",
                    error: error
                )
            )
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
            generation: generation,
            identifier: nextPageIdentifier,
            operationID: operationID,
            task: task
        )
        nextPageInFlight = flight
        completedNextPageFlight = nil
        paginationState = PaginationState(
            isLoading: true,
            hasNextPage: true
        )
        return flight
    }

    private func resolveNextPage(
        _ initialFlight: NextPageFlight
    ) async throws -> Space.Snapshot {
        var flight = initialFlight

        while true {
            do {
                let page = try await flight.task.value

                guard generation == flight.generation else {
                    if let replacement = completedNextPageReplacement(
                        for: flight
                    ) {
                        return try replacement.get()
                    }
                    if let replacement = nextPageInFlight {
                        flight = replacement
                        continue
                    }
                    throw CancellationError()
                }

                if let completed = completedNextPageReplacement(for: flight) {
                    return try completed.get()
                }

                guard nextPageInFlight?.identifier == flight.identifier else {
                    guard let snapshot else {
                        throw CancellationError()
                    }
                    return snapshot
                }

                let operationID = flight.operationID
                publishStreamState {
                    storedSnapshot = .present(page.snapshot)
                    paginationCheckpoint = .available(page.nextPage)
                    nextPageInFlight = nil
                    completedNextPageFlight = CompletedNextPageFlight(
                        generation: flight.generation,
                        identifier: flight.identifier,
                        result: .success(page.snapshot)
                    )
                    paginationState = PaginationState(
                        hasNextPage: page.nextPage != nil
                    )
                }
                ContinuumLogContext.withOperationID(operationID) {
                    continuumDebug(
                        .paginationCompleted(
                            logIdentity,
                            count: snapshotCount(page.snapshot),
                            hasNextPage: page.nextPage != nil
                        )
                    )
                }
                return page.snapshot
            } catch {
                guard generation == flight.generation else {
                    if let replacement = completedNextPageReplacement(
                        for: flight
                    ) {
                        return try replacement.get()
                    }
                    if let replacement = nextPageInFlight {
                        flight = replacement
                        continue
                    }
                    throw CancellationError()
                }

                if let completed = completedNextPageReplacement(for: flight) {
                    return try completed.get()
                }

                guard nextPageInFlight?.identifier == flight.identifier else {
                    throw error
                }

                let operationID = flight.operationID
                nextPageInFlight = nil
                completedNextPageFlight = CompletedNextPageFlight(
                    generation: flight.generation,
                    identifier: flight.identifier,
                    result: .failure(error)
                )
                paginationState = PaginationState(
                    hasNextPage: paginationCheckpoint.continuation != nil,
                    error: error
                )
                ContinuumLogContext.withOperationID(operationID) {
                    if error is CancellationError {
                        continuumDebug(
                            .operationCancelled(
                                logIdentity,
                                operation: "next-page"
                            )
                        )
                    } else {
                        continuumDebug(
                            .operationFailed(
                                logIdentity,
                                operation: "next-page",
                                error: error
                            )
                        )
                    }
                }
                throw error
            }
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
        paginationState = PaginationState(
            hasNextPage: paginationCheckpoint.continuation != nil,
            error: error
        )
    }
}
