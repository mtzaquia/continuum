import Continuum
import Testing

@Suite("Operation ordering", .timeLimit(.minutes(1)))
struct OperationOrderingTests {
    @Test("An obsolete disk write completes before reset clears persistence")
    func resetFencesPersistence() async throws {
        let disk = OrderedDisk()
        let bucket = Bucket(Key<Int>("ordering.reset")) {
            LocalSource { await disk.value } persist: { await disk.write($0) }
            RemoteSource { 1 }
        }
        await disk.write(9)
        try await bucket.load()
        let load = Task { try await bucket.load(using: .remote) }
        await disk.writeGate.started()
        let reset = Task { try await bucket.reset() }
        while bucket.isLoaded { await Task.yield() }
        #expect(await disk.writes == [9])
        await disk.writeGate.finish(())
        try await reset.value
        await #expect(throws: CancellationError.self) { try await load.value }
        #expect(bucket.value == nil)
        #expect(await disk.value == nil)
        #expect(await disk.writes == [9, 1, nil])
    }

    @Test("A cached read waits for reset to finish clearing disk")
    func cachedReadWaitsForReset() async throws {
        let disk = OrderedDisk(blockReset: true)
        let bucket = Bucket(Key<Int>("ordering.cached-reset")) {
            LocalSource { await disk.value } persist: { await disk.write($0) }
            RemoteSource { 7 }
        }
        await disk.write(9)
        try await bucket.load()
        let reset = Task { try await bucket.reset() }
        await disk.writeGate.started()
        let cached = Task { try await bucket.load() }
        await disk.writeGate.finish(())
        try await reset.value
        #expect(try await cached.value == 7)
        #expect(bucket.value == 7)
    }

    @Test("A forced refresh waits for an earlier mutation and reads its server result")
    func forcedRefreshWaitsForMutation() async throws {
        let gate = OperationGate<Int>()
        let disk = OrderedDisk()
        let (refreshRequested, requested) = AsyncStream<Void>.makeStream()
        let bucket = Bucket(Key<Int>("ordering.store-refresh")) {
            LocalSource { 0 } persist: { await disk.write($0) }
            RemoteSource { await disk.value ?? -1 }
        }
        try await bucket.load()
        let mutation = Task {
            try await bucket.mutate { mutation in
                try await mutation.update { _ in 4 } remote: {
                    let response = await gate.wait()
                    try Task.checkCancellation()
                    return .updated(response)
                }
            }
        }
        await gate.started()
        let refresh = Task {
            requested.yield(())
            return try await bucket.load(using: .remote)
        }
        var requests = refreshRequested.makeAsyncIterator()
        _ = await requests.next()
        #expect(bucket.value == 4)
        await gate.finish(2)
        try await mutation.value
        #expect(try await refresh.value == 2)
        #expect(bucket.value == 2)
        #expect(bucket.error == nil)
        #expect(await disk.writes == [4, 2, 2])
    }

    @Test("A load from before reset cannot adopt the next session's snapshot")
    func resetFencesReadReplacement() async throws {
        let oldSource = OperationGate<Int>()
        let source = SessionSource(old: oldSource)
        let bucket = Bucket(Key<Int>("ordering.session-load")) {
            RemoteSource { await source.load() }
        }
        let old = Task { try await bucket.load(using: .remote) }
        await oldSource.started()

        try await bucket.reset()
        await source.switchSession()
        #expect(try await bucket.load(using: .remote) == 2)

        await oldSource.finish(1)
        await #expect(throws: CancellationError.self) { try await old.value }
        #expect(bucket.value == 2)
    }

    @Test("Reset prevents queued mutations from starting and lets an active remote-only handler finish")
    func resetDiscardsQueuedMutations() async throws {
        let endpoint = OperationGate<Void>()
        let (queued, requested) = AsyncStream<Void>.makeStream()
        let bucket = Bucket(Key<Int>("ordering.reset-queued"))
        var startedQueuedMutation = false

        let running = Task {
            try await bucket.mutate { _ in
                await endpoint.wait()
                try Task.checkCancellation()
            }
        }
        await endpoint.started()
        let pending = Task {
            requested.yield(())
            try await bucket.mutate { _ in startedQueuedMutation = true }
        }
        var requests = queued.makeAsyncIterator()
        _ = await requests.next()

        try await bucket.reset()
        await endpoint.finish(())
        try await running.value
        await #expect(throws: CancellationError.self) { try await pending.value }
        #expect(!startedQueuedMutation)
    }

    @Test("Cancellation rolls back memory and disk")
    func cancellationRestoresState() async throws {
        let gate = OperationGate<Int>()
        let disk = OrderedDisk()
        let bucket = Bucket(Key<Int>("ordering.cancel")) {
            LocalSource { nil } persist: { await disk.write($0) }
            RemoteSource { 9 }
        }
        try await bucket.load()
        let mutation = Task {
            try await bucket.mutate { mutation in
                try await mutation.update { _ in 4 } remote: {
                    _ = await gate.wait()
                    try Task.checkCancellation()
                    return .updated(2)
                }
            }
        }
        await gate.started()
        mutation.cancel()
        await gate.finish(0)
        await #expect(throws: CancellationError.self) { try await mutation.value }
        #expect(bucket.value == 9)
        #expect(bucket.error == nil)
        #expect(await disk.writes == [9, 4, 9])
    }

    @Test("Pagination waits for store reconciliation before capturing its base")
    func paginationWaitsForStore() async throws {
        let gate = OperationGate<Int>()
        let pageEntered = OperationGate<Void>()
        let bucket = Bucket(IndexedKey<Int, Int>("ordering.store-page", indexedBy: { $0 })) {
            RemoteSource {
                Load { Page(values: [1], next: 1) }
                NextPage { (_: Int) in
                    await pageEntered.finish(())
                    return Page(values: [3], next: Optional<Int>.none)
                }
            }
        }
        try await bucket.load()
        let mutation = Task {
            try await bucket.mutate { mutation in
                try await mutation.update { $0 + [2] } remote: {
                    .updated([1, await gate.wait()])
                }
            }
        }
        await gate.started()
        let page = Task { try await bucket.loadNext() }
        // Yield to the page caller while the store remains suspended.
        await Task.yield()
        #expect(await pageEntered.isFinished == false)
        await gate.finish(20)
        try await mutation.value
        #expect(try await page.value == [1, 20, 3])
        #expect(bucket.values == [1, 20, 3])
        #expect(bucket.hasNextPage == false)
    }

    @Test("A queued mutation waits for cancelled mutation cleanup")
    func queuedMutationFollowsRollback() async throws {
        let gate = OperationGate<Void>()
        let disk = OrderedDisk()
        let bucket = Bucket(Key<Int>("ordering.cancel-queued")) {
            LocalSource { nil } persist: { await disk.write($0) }
            RemoteSource { 9 }
        }
        try await bucket.load()
        let first = Task {
            try await bucket.mutate { mutation in
                try await mutation.update { _ in 4 } remote: {
                    await gate.wait()
                    try Task.checkCancellation()
                    return .updated(4)
                }
            }
        }
        await gate.started()
        let second = Task {
            try await bucket.mutate { mutation in
                try await mutation.update { _ in 5 } remote: {
                    .updated(5)
                }
            }
        }
        first.cancel()
        await gate.finish(())
        await #expect(throws: CancellationError.self) { try await first.value }
        try await second.value
        #expect(bucket.value == 5)
        #expect(await disk.writes == [9, 4, 9, 5, 5])
    }

    @Test("A cancelled reset restores persistence and the pagination checkpoint")
    func cancelledResetRestoresPagination() async throws {
        let disk = PageDisk()
        let bucket = Bucket(IndexedKey<Int, Int>("ordering.cancel-reset", indexedBy: { $0 })) {
            LocalSource { nil } persist: { await disk.write($0) }
            RemoteSource {
                Load { Page(values: [1], next: 1) }
                NextPage { (_: Int) in Page(values: [2], next: Optional<Int>.none) }
            }
        }
        try await bucket.load()
        let reset = Task { try await bucket.reset() }
        await disk.resetGate.started()
        reset.cancel()
        await disk.resetGate.finish(())
        await #expect(throws: CancellationError.self) { try await reset.value }
        #expect(bucket.values == [1])
        #expect(bucket.error == nil)
        #expect(bucket.hasNextPage)
        #expect(await disk.writes == [[1], nil, [1]])
        #expect(try await bucket.loadNext() == [1, 2])
    }

    @Test("Cached-then-remote waits for a pending mutation")
    func refreshWaitsForMutation() async throws {
        let gate = OperationGate<Int>()
        let bucket = Bucket(Key<Int>("ordering.cached-remote")) {
            LocalSource { 0 }
            RemoteSource { 9 }
        }
        try await bucket.load()
        let mutation = Task {
            try await bucket.mutate { mutation in
                try await mutation.update { _ in 4 } remote: {
                    .updated(await gate.wait())
                }
            }
        }
        await gate.started()
        let refresh = Task { try await bucket.load(using: .cachedThenRemote) }
        await Task.yield()
        await gate.finish(2)
        try await mutation.value
        #expect(try await refresh.value == 9)
        #expect(bucket.value == 9)
    }

    @Test("A partition retry preserves its failure until it settles", arguments: [false, true])
    func retryPreservesStreamFailure(retryFails: Bool) async throws {
        enum Failure: Error { case initial, retry }
        let gate = OperationGate<Result<Int, Failure>>()
        let bucket = Bucket(Key<Int>("ordering.retry"), partitionedBy: String.self) { _ in
            RemoteSource { try await gate.wait().get() }
        }
        let partition = bucket["selected"]
        let composition = Composition { Input(partition) } transform: { $0 }
        let firstLoad = Task { await composition.load(using: .remote) }
        await gate.started()
        await gate.finish(.failure(.initial))
        await firstLoad.value

        var updates = composition.makeAsyncIterator()
        guard case .result(.failure)? = await updates.next() else {
            Issue.record("Expected the initial failure")
            return
        }

        let retry = Task { await composition.load(using: .remote) }
        await gate.started()
        #expect(partition.isLoading)
        #expect(partition.error == nil)
        if case .result(.failure(let error)) = partition.latest {
            #expect(error as? Failure == .initial)
            var joinedDuringRetry = partition.makeAsyncIterator()
            if case .result(.failure(let replayed))? = await joinedDuringRetry.next() {
                #expect(replayed as? Failure == .initial)
            } else {
                Issue.record("Expected a new partition subscriber to receive the previous failure")
            }
        } else {
            Issue.record("Expected the partition to retain its failure during retry")
        }
        let retainedError = composition.error as? CompositionError
        #expect(retainedError?.errors.first as? Failure == .initial)

        await gate.finish(retryFails ? .failure(.retry) : .success(9))
        await retry.value
        switch await updates.next() {
        case .result(.failure(let error as CompositionError))? where retryFails:
            #expect(error.errors.first as? Failure == .retry)
        case .result(.success(9))? where !retryFails:
            #expect(composition.error == nil)
        default:
            Issue.record("Expected the retry outcome without an intervening reset")
            return
        }

        var resetUpdates = composition.makeAsyncIterator()
        _ = await resetUpdates.next()
        try await partition.reset()
        guard case .reset? = await resetUpdates.next() else {
            Issue.record("Expected an explicit reset to clear the retry outcome")
            return
        }
        #expect(partition.error == nil)
        #expect(composition.error == nil)
    }
}

private actor PageDisk {
    private(set) var writes: [[Int]?] = []
    let resetGate = OperationGate<Void>()

    func write(_ value: [Int]?) async {
        if value == nil { await resetGate.wait() }
        writes.append(value)
    }
}

private actor SessionSource {
    let old: OperationGate<Int>
    private var newSession = false

    init(old: OperationGate<Int>) { self.old = old }

    func switchSession() { newSession = true }

    func load() async -> Int {
        if newSession { return 2 }
        return await old.wait()
    }
}

private actor OrderedDisk {
    private(set) var value: Int?
    private(set) var writes: [Int?] = []
    let writeGate = OperationGate<Void>()
    let blockReset: Bool

    init(blockReset: Bool = false) { self.blockReset = blockReset }

    func write(_ value: Int?) async {
        if blockReset ? value == nil : value == 1 { await writeGate.wait() }
        self.value = value
        writes.append(value)
    }
}

private actor OperationGate<Value: Sendable> {
    private var pending: CheckedContinuation<Value, Never>?
    private var ready: [CheckedContinuation<Void, Never>] = []
    private(set) var isFinished = false

    func wait() async -> Value {
        await withCheckedContinuation { continuation in
            pending = continuation
            ready.forEach { $0.resume() }
            ready.removeAll()
        }
    }

    func started() async {
        if pending != nil { return }
        await withCheckedContinuation { ready.append($0) }
    }

    func finish(_ value: Value) {
        isFinished = true
        pending?.resume(returning: value)
        pending = nil
    }
}
