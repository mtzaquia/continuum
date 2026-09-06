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
        try await bucket.store(9)
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
        try await bucket.store(9)
        let reset = Task { try await bucket.reset() }
        await disk.writeGate.started()
        let cached = Task { try await bucket.load() }
        await disk.writeGate.finish(())
        try await reset.value
        #expect(try await cached.value == 7)
        #expect(bucket.value == 7)
    }

    @Test("A superseded store cannot overwrite a refresh or its persistence")
    func refreshSupersedesStore() async throws {
        let gate = OperationGate<Int>()
        let disk = OrderedDisk()
        let bucket = Bucket(Key<Int>("ordering.store-refresh")) {
            LocalSource { nil } persist: { await disk.write($0) }
            RemoteSource {
                Load { 9 }
                Store { _ in await gate.wait() }
            }
        }
        let mutation = Task { try await bucket.store(4) }
        await gate.started()
        try await bucket.load(using: .remote)
        await gate.finish(2)
        await #expect(throws: CancellationError.self) { try await mutation.value }
        #expect(bucket.value == 9)
        #expect(bucket.error == nil)
        #expect(await disk.writes == [4, 9])
    }

    @Test("Cancellation rolls back memory and disk")
    func cancellationRestoresState() async throws {
        let gate = OperationGate<Int>()
        let disk = OrderedDisk()
        let bucket = Bucket(Key<Int>("ordering.cancel")) {
            LocalSource { nil } persist: { await disk.write($0) }
            RemoteSource {
                Load { 9 }
                Store { _ in
                    _ = await gate.wait()
                    try Task.checkCancellation()
                    return 2
                }
            }
        }
        try await bucket.load()
        let mutation = Task { try await bucket.store(4) }
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
                Store { _ in await gate.wait() }
            }
        }
        try await bucket.load()
        let mutation = Task { try await bucket.store(2) }
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
            RemoteSource {
                Load { 9 }
                Store { value in
                    if value == 4 { await gate.wait() }
                    try Task.checkCancellation()
                    return value
                }
            }
        }
        try await bucket.load()
        let first = Task { try await bucket.store(4) }
        await gate.started()
        let second = Task { try await bucket.store(5) }
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
            RemoteSource {
                Load { 9 }
                Store { _ in await gate.wait() }
            }
        }
        let mutation = Task { try await bucket.store(4) }
        await gate.started()
        let refresh = Task { try await bucket.load(using: .cachedThenRemote) }
        await Task.yield()
        await gate.finish(2)
        try await mutation.value
        #expect(try await refresh.value == 9)
        #expect(bucket.value == 9)
    }

    @Test("A new subscription during retry does not replay a cleared error")
    func retryClearsStreamFailure() async throws {
        let gate = OperationGate<Int>()
        enum Failure: Error { case expected }
        let bucket = Bucket(Key<Int>("ordering.retry")) {
            RemoteSource {
                Load { await gate.wait() }
                Store<Int> { _ in throw Failure.expected }
            }
        }
        await #expect(throws: Failure.self) { try await bucket.store(1) }
        let refresh = Task { try await bucket.load(using: .remote) }
        await gate.started()
        #expect(bucket.error == nil)
        // This is the same current state that the public stream observes.
        guard case .reset = bucket._latestUpdateForObservation() else {
            await gate.finish(9)
            _ = try await refresh.value
            Issue.record("Expected unavailable state after clearing the failure")
            return
        }
        let consumer = Task {
            var iterator = bucket.updates().makeAsyncIterator()
            return await iterator.next()
        }
        await Task.yield()
        await gate.finish(9)
        _ = try await refresh.value
        guard case .result(.success(9))? = await consumer.value else {
            Issue.record("Expected the refreshed result, not the old error")
            return
        }
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
