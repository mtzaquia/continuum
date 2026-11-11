@testable import Continuum
import Testing

@Suite("Snapshot freshness", .timeLimit(.minutes(1)))
struct FreshnessTests {
    @Test("Original initializer signatures remain usable as function values")
    func originalInitializerReferences() async throws {
        typealias Configuration = BucketConfiguration<Key<Int>>
        let makeBucket: @MainActor (Key<Int>, () -> Configuration) -> ValueBucket<Int> = Bucket.init
        let makePartitioned: @MainActor (
            Key<Int>,
            String.Type,
            @escaping @MainActor @Sendable (String) -> Configuration
        ) -> PartitionedValueBucket<String, Int> = Bucket.init
        let service = Service()
        let configuration = Configuration(remoteSources: [
            RemoteSource { try await service.load() },
        ])

        let bucket = makeBucket(Key<Int>("freshness.original"), { configuration })
        #expect(try await bucket.load() == 1)
        #expect(try await bucket.load() == 1)

        let partitions = makePartitioned(
            Key<Int>("freshness.original.partitions"), String.self, { _ in configuration }
        )
        #expect(try await partitions["first"].load() == 2)
        #expect(try await partitions["first"].load() == 2)
    }

    @Test("Cached loads reuse fresh memory and refresh at the exact boundary")
    func freshnessBoundary() async throws {
        let time = TestTime()
        let service = Service()
        let bucket = makePartition(time: time) {
            RemoteSource { try await service.load() }
        }

        #expect(try await bucket.load() == 1)
        time.advance(by: .seconds(9))
        #expect(try await bucket.load() == 1)
        #expect(service.calls == 1)
        time.advance(by: .seconds(1))
        #expect(bucket.value == 1)
        #expect(bucket.isLoaded)
        #expect(!bucket.isLoading)
        #expect(service.calls == 1)
        #expect(try await bucket.load() == 2)
        time.advance(by: .seconds(9))
        #expect(try await bucket.load() == 2)
        #expect(service.calls == 2)
    }

    @Test("Omitting freshness preserves unlimited memory and local cache reuse")
    func unlimited() async throws {
        let time = TestTime()
        let service = Service()
        let remote = makePartition(time: time, staleAfter: nil) {
            RemoteSource { try await service.load() }
        }
        #expect(try await remote.load() == 1)
        time.advance(by: .seconds(1_000_000))
        #expect(try await remote.load() == 1)
        #expect(service.calls == 1)

        let local = Bucket(Key<Int>("freshness.unlimited")) {
            LocalSource { 7 }
            RemoteSource { try await service.load() }
        }
        #expect(try await local.load() == 7)
        #expect(service.calls == 1)
    }

    @Test("Zero freshness validates every cached load through the public API")
    func zeroDuration() async throws {
        let service = Service()
        let bucket = Bucket(Key<Int>("freshness.zero"), staleAfter: .zero) {
            RemoteSource { try await service.load() }
        }
        #expect(try await bucket.load() == 1)
        #expect(try await bucket.load() == 2)
    }

    @Test("Local-only and in-memory configurations ignore freshness")
    func localOnly() async throws {
        let local = Bucket(Key<Int>("freshness.local"), staleAfter: .zero) {
            LocalSource { 7 }
        }
        #expect(try await local.load() == 7)
        #expect(try await local.load() == 7)
        let memory = Bucket(Key<Int>("freshness.memory"), staleAfter: .zero) {}
        try await memory.store(9)
        #expect(try await memory.load() == 9)
    }

    @Test("Unvalidated local data stays readable while the first cached load validates remotely")
    func localValidation() async throws {
        let gate = RemoteGate()
        let service = Service()
        let bucket = Bucket(Key<Int>("freshness.disk"), staleAfter: .seconds(10)) {
            LocalSource { 7 } persist: { try await service.persist($0) }
            RemoteSource { try await gate.load() }
        }
        let loading = Task { try await bucket.load() }
        await until { gate.pending != nil }
        #expect(bucket.value == 7)
        #expect(bucket.isLoaded)
        #expect(bucket.isLoading)
        gate.finish(with: .success(8))
        #expect(try await loading.value == 8)
        #expect(service.writes == [8])
        #expect(try await bucket.load() == 8)
        #expect(gate.calls == 1)
    }

    @Test("Stale callers share a refresh while keeping old values loaded")
    func sharedRefresh() async throws {
        let time = TestTime()
        let gate = RemoteGate()
        let bucket = makePartition(time: time) {
            RemoteSource { try await gate.load() }
        }
        let initial = Task { try await bucket.load() }
        await until { gate.pending != nil }
        gate.finish(with: .success(1))
        _ = try await initial.value
        time.advance(by: .seconds(10))

        let first = Task { try await bucket.load() }
        await until { gate.pending != nil }
        let second = Task { try await bucket.load() }
        #expect(bucket.value == 1)
        #expect(bucket.isLoaded)
        #expect(bucket.isLoading)
        gate.finish(with: .success(2))
        #expect(try await first.value == 2)
        #expect(try await second.value == 2)
        #expect(gate.calls == 2)
    }

    @Test("Failed stale refreshes preserve data and retry on the next cached load")
    func failedRefresh() async throws {
        let time = TestTime()
        let service = Service()
        let bucket = makePartition(time: time) {
            RemoteSource { try await service.load() }
        }
        try await bucket.load()
        time.advance(by: .seconds(10))
        service.failLoad = true
        await #expect(throws: Failure.self) { try await bucket.load() }
        #expect(bucket.value == 1)
        #expect(bucket.isLoaded)
        #expect(!bucket.isLoading)
        #expect(bucket.error is Failure)
        if case .result(.success(1)) = bucket.latest {} else {
            Issue.record("Expected the retained snapshot as the latest outcome")
        }
        service.failLoad = false
        #expect(try await bucket.load() == 3)
        #expect(bucket.error == nil)
    }

    @Test("Persistence must succeed before freshness restarts")
    func persistenceFailure() async throws {
        let time = TestTime()
        let service = Service()
        let bucket = makePartition(time: time) {
            LocalSource { nil } persist: { try await service.persist($0) }
            RemoteSource { try await service.load() }
        }
        try await bucket.load()
        time.advance(by: .seconds(10))
        service.failWrites = true
        await #expect(throws: Failure.self) { try await bucket.load() }
        #expect(bucket.value == 1)
        service.failWrites = false
        #expect(try await bucket.load() == 3)
        #expect(service.writes == [1, 3])
    }

    @Test("Freshness begins after full snapshot persistence completes")
    func freshnessAfterPersistence() async throws {
        let time = TestTime()
        let gate = RemoteGate()
        let service = Service()
        let bucket = makePartition(time: time) {
            LocalSource { nil } persist: { _ in _ = try await gate.load() }
            RemoteSource { try await service.load() }
        }
        let initial = Task { try await bucket.load() }
        await until { gate.pending != nil }
        time.advance(by: .seconds(20))
        gate.finish(with: .success(0))
        _ = try await initial.value
        time.advance(by: .seconds(9))
        #expect(try await bucket.load() == 1)
        #expect(service.calls == 1)
    }

    @Test("Explicit refresh policies still reach remote and renew freshness")
    func explicitPolicies() async throws {
        let time = TestTime()
        let service = Service()
        let bucket = makePartition(time: time) {
            RemoteSource { try await service.load() }
        }
        try await bucket.load()
        time.advance(by: .seconds(5))
        #expect(try await bucket.load(using: .cachedThenRemote) == 2)
        time.advance(by: .seconds(5))
        #expect(try await bucket.load() == 2)
        #expect(try await bucket.load(using: .remote) == 3)
        time.advance(by: .seconds(9))
        #expect(try await bucket.load() == 3)
    }

    @Test("A failed explicit refresh does not extend the prior freshness interval")
    func failedExplicitRefresh() async throws {
        let time = TestTime()
        let service = Service()
        let bucket = makePartition(time: time) {
            RemoteSource { try await service.load() }
        }
        try await bucket.load()
        time.advance(by: .seconds(5))
        service.failLoad = true
        await #expect(throws: Failure.self) { try await bucket.load(using: .remote) }
        #expect(try await bucket.load() == 1)
        #expect(bucket.error is Failure)
        service.failLoad = false
        time.advance(by: .seconds(5))
        #expect(try await bucket.load() == 3)
    }

    @Test("Reset clears freshness and failed reset restores the original interval")
    func resetFreshness() async throws {
        let time = TestTime()
        let service = Service()
        let bucket = makePartition(time: time) {
            LocalSource { 7 } persist: { try await service.persist($0) }
            RemoteSource { try await service.load() }
        }
        try await bucket.load()
        time.advance(by: .seconds(5))
        service.failWrites = true
        await #expect(throws: Failure.self) { try await bucket.reset() }
        service.failWrites = false
        #expect(try await bucket.load() == 1)
        time.advance(by: .seconds(5))
        #expect(try await bucket.load() == 2)
        try await bucket.reset()
        #expect(!bucket.isLoaded)
        // The constant local fixture still returns data, which needs validation.
        #expect(try await bucket.load() == 3)
        #expect(service.writes == [1, 2, nil, 3])
    }

    @Test("Each partition owns its own freshness interval")
    func independentIntervals() async throws {
        let time = TestTime()
        let service = Service()
        let first = makePartition(time: time) {
            RemoteSource { try await service.load() }
        }
        let second = makePartition(time: time) {
            RemoteSource { try await service.load() }
        }
        try await first.load()
        time.advance(by: .seconds(5))
        try await second.load()
        time.advance(by: .seconds(5))
        #expect(try await first.load() == 3)
        #expect(try await second.load() == 2)
        time.advance(by: .seconds(5))
        #expect(try await first.load() == 3)
        #expect(try await second.load() == 4)
    }

    @Test("The public partition initializer forwards freshness to each selected partition")
    func partitionedAPI() async throws {
        let service = Service()
        let bucket = Bucket(
            Key<Int>("freshness.partitions"),
            partitionedBy: String.self,
            staleAfter: .zero
        ) { _ in
            RemoteSource { try await service.load() }
        }
        #expect(try await bucket["first"].load() == 1)
        #expect(try await bucket["second"].load() == 2)
        #expect(try await bucket["first"].load() == 3)
        #expect(bucket["second"].value == 2)
        #expect(bucket["first"] === bucket["first"])
    }

    @Test("Mutations, entry loads, and subsequent pages do not renew collection freshness")
    func partialUpdates() async throws {
        let time = TestTime()
        let service = Service()
        let key = IndexedKey<Int, Int>("freshness.pages", indexedBy: { $0 })
        let bucket = BucketPartition(
            key,
            staleAfter: .seconds(10),
            now: { time.instant },
            configuration: BucketConfiguration(remoteSources: [
                RemoteSource {
                    Load { Page(values: [try await service.load()], next: 1) }
                    NextPage { (_: Int) in Page(values: [2], next: Optional<Int>.none) }
                    LoadEntry { (id: Int) in id }
                    Store { (value: Int) in value }
                    Remove { (_: Int) in }
                },
            ])
        )
        try await bucket.load()
        time.advance(by: .seconds(2))
        try await bucket.store(3)
        time.advance(by: .seconds(2))
        try await bucket.remove(3)
        time.advance(by: .seconds(2))
        #expect(try await bucket.load(id: 1, using: .remote) == 1)
        time.advance(by: .seconds(2))
        #expect(try await bucket.loadNext() == [1, 2])
        time.advance(by: .seconds(2))
        #expect(try await bucket.load() == [2])
        #expect(service.calls == 2)
    }

    @Test("Stale cached loading waits for an optimistic mutation to finish")
    func waitsForMutation() async throws {
        let time = TestTime()
        let gate = RemoteGate()
        let service = Service()
        let bucket = makePartition(time: time) {
            RemoteSource {
                Load { try await service.load() }
                Store { _ in try await gate.load() }
            }
        }
        try await bucket.load()
        time.advance(by: .seconds(10))
        let mutation = Task { try await bucket.store(7) }
        await until { gate.pending != nil }
        let cached = Task { try await bucket.load() }
        #expect(bucket.value == 7)
        gate.finish(with: .success(8))
        try await mutation.value
        #expect(try await cached.value == 2)
        #expect(bucket.value == 2)
    }

    private func makePartition(
        time: TestTime,
        staleAfter: Duration? = .seconds(10),
        @BucketBuilder<Key<Int>> configuration: () -> BucketConfiguration<Key<Int>>
    ) -> BucketPartition<Key<Int>> {
        BucketPartition(
            Key<Int>("freshness.test"),
            staleAfter: staleAfter,
            now: { time.instant },
            configuration: configuration()
        )
    }

    private func until(_ predicate: () -> Bool) async {
        while !predicate() { await Task.yield() }
    }
}

private final class TestTime {
    var instant = ContinuousClock.now

    func advance(by duration: Duration) {
        instant = instant.advanced(by: duration)
    }
}

private enum Failure: Error { case failed }

private final class Service {
    var calls = 0
    var failLoad = false
    var failWrites = false
    var writes: [Int?] = []

    func load() throws -> Int {
        calls += 1
        if failLoad { throw Failure.failed }
        return calls
    }

    func persist(_ snapshot: Int?) throws {
        if failWrites { throw Failure.failed }
        writes.append(snapshot)
    }
}

private final class RemoteGate {
    var calls = 0
    var pending: CheckedContinuation<Int, any Error>?

    func load() async throws -> Int {
        calls += 1
        return try await withCheckedThrowingContinuation { pending = $0 }
    }

    func finish(with result: Result<Int, any Error>) {
        let continuation = pending
        pending = nil
        continuation?.resume(with: result)
    }
}
