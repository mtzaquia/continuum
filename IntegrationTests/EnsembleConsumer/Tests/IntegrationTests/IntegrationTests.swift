import Continuum
import Ensemble
import Observation
import Testing

@MainActor @Observable
final class Source: UpdateSource {
    var update: Update<Int> = .reset
    var latest: Update<Int> { update }
}

enum Failure: Error { case test }

@Suite(.timeLimit(.minutes(1)))
struct IntegrationTests {
    @MainActor @Test func directBucketBinding() async throws {
        let bucket = Bucket(Key<Int>("ensemble.direct")) {
            LocalSource { 7 }
            RemoteSource { throw Failure.test }
        }
        let model = Consumer(bucket) { _ = try? await bucket.load(using: .remote) }
        let data = model.data
        var received = model.updates.makeAsyncIterator()
        data.connect()
        for _ in 0..<10 { await Task.yield() }
        #expect(data.phase.kind == .empty)
        try await bucket.load()
        if case .result(.success(7))? = await received.next() {} else {
            Issue.record("The initial snapshot must be the first update")
        }
        if case .available(7) = data.latestValue {} else { Issue.record("Expected direct bucket value") }

        await data.reload()
        if case .result(.success(7))? = await received.next() {} else {
            Issue.record("A failed refresh must deliver the retained value to Ensemble")
        }
        #expect(bucket.error is Failure)
        if case .success = data.phase {} else { Issue.record("The refresh must leave ViewData successful") }
        #expect(try await bucket.load() == 7)
        #expect(bucket.error is Failure)

        let lateModel = Consumer(bucket)
        let lateData = lateModel.data
        var lateUpdates = lateModel.updates.makeAsyncIterator()
        lateData.connect()
        if case .result(.success(7))? = await lateUpdates.next() {} else {
            Issue.record("A late binding must receive the retained value")
        }
        if case .available(7) = lateData.latestValue {} else { Issue.record("Expected late ViewData to show data") }

        try await bucket.reset()
        guard case .reset? = await received.next() else {
            Issue.record("Expected reset from direct bucket iteration")
            return
        }
        if case .unavailable = data.latestValue {} else { Issue.record("Expected reset to clear data") }
        await data.reload()
        if case .result(.failure)? = await received.next() {} else {
            Issue.record("A refresh with no snapshot must still deliver failure")
        }
        if case .failure = data.phase {} else { Issue.record("Expected failure without a snapshot") }
    }

    @MainActor @Test func retainedBucketInComposition() async throws {
        let bucket = Bucket(Key<Int>("ensemble.required")) {
            LocalSource { 7 }
            RemoteSource { throw Failure.test }
        }
        try await bucket.load()
        let sibling = Source()
        sibling.update = .result(.success(1))
        let composition = Composition {
            Input(bucket)
            Input(sibling)
        } transform: { $0 + $1 }
        let model = Consumer(composition) { await composition.load(using: .remote) }
        let data = model.data
        var received = model.updates.makeAsyncIterator()
        data.connect()
        _ = await received.next()
        await data.reload()
        #expect(bucket.error is Failure)
        let aggregate = composition.error as? CompositionError
        #expect(aggregate?.errors.count == 1)
        #expect(aggregate?.errors.first is Failure)
        sibling.update = .result(.success(2))
        while let update = await received.next() {
            guard case .result(.success(let value)) = update else {
                Issue.record("The composed binding must stay successful after the refresh")
                return
            }
            if value == 9 { break }
        }
        if case .available(9) = data.latestValue {} else { Issue.record("Expected the updated composition") }
    }

    @MainActor @Test func relationshipResetFailureAndRetry() async throws {
        let service = RelationshipService()
        let names = Bucket(IndexedKey<Int, NamedEntry>("ensemble.names")) {
            RemoteSource(
                { [NamedEntry]() },
                loadEntry: { id in NamedEntry(id: id, name: try await service.name(id)) }
            )
        }
        let posts = Bucket(Key<[Int]>("ensemble.posts")) { LocalSource { [1] } }
        let child = Composition {
            Input(posts).resolving(\.self, from: names)
        } transform: { $0.related[1]?.name ?? "" }
        let composition = Composition { Input(child) } transform: { $0 }
        let model = Consumer(composition)
        let data = model.data
        var received = model.updates.makeAsyncIterator()
        data.connect()

        await composition.load()
        while let update = await received.next() {
            if case .result(.success("name-1")) = update { break }
        }
        if case .available("name-1") = data.latestValue {} else { Issue.record("Expected resolved name") }

        try await names.reset()
        while let update = await received.next() {
            if case .reset = update { break }
        }
        if case .unavailable = data.latestValue {} else { Issue.record("Reset relationships must clear retained data") }

        await service.setFailure(true)
        await composition.load()
        while let update = await received.next() {
            if case .result(.failure) = update { break }
        }
        if case .failure = data.phase {} else { Issue.record("Expected failure phase") }

        await service.setFailure(false)
        await composition.load()
        while let update = await received.next() {
            if case .result(.success("name-1")) = update { break }
        }
        if case .available("name-1") = data.latestValue {} else { Issue.record("Relationship retry must recover") }
    }

    @MainActor @Test func bindingResetFailureAndRetry() async {
        let a = Source()
        let b = Source()
        let inner = Composition {
            Input(a)
            Input(b)
        } transform: { $0 + $1 }
        let middle = Composition {
            Input(inner)
        } transform: { $0 + 0 }
        let composition = Composition {
            Input(middle)
        } transform: { $0 + 0 }
        let model = Consumer(composition) {
            a.update = .result(.success(10))
            b.update = .result(.success(20))
        }
        let data = model.data
        var received = model.updates.makeAsyncIterator()
        data.connect()
        for _ in 0..<20 { await Task.yield() }
        #expect(data.phase.kind == .empty)
        await composition.load()
        #expect(data.phase.kind == .empty)
        await data.reload()
        // Wait for the observed values to reach the binding.
        while let update = await received.next() {
            if case .result(.success(30)) = update { break }
        }
        if case .available(30) = data.latestValue {} else { Issue.record("Expected loaded data") }
        a.update = .reset
        b.update = .result(.failure(Failure.test))
        while let update = await received.next() {
            if case .reset = update { break }
        }
        while let update = await received.next() {
            if case .result(.failure) = update { break }
        }
        if case .unavailable = data.latestValue {} else { Issue.record("Failure must not retain reset data") }
        if case .failure = data.phase {} else { Issue.record("Expected failure phase") }
        await data.reload()
        while let update = await received.next() {
            if case .result(.success(30)) = update { break }
        }
        if case .available(30) = data.latestValue {} else { Issue.record("Retry must recover") }
    }
}

@MainActor
private final class Consumer<Source: AsyncSequence, Value: Sendable> where Source.Element == Update<Value> {
    let source: Source
    let refresh: @MainActor () async -> Void
    let updates: AsyncStream<Update<Value>>
    private let observed: AsyncStream<Update<Value>>.Continuation

    @ViewDataSource(
        make: { (model: Self) in model.source },
        refresh: { model, _ in await model.refresh() },
        receive: { model, update, sink in
            switch update {
            case .result(let result): sink.receive(result)
            case .reset: sink.reset()
            }
            model.observed.yield(update)
        }
    )
    var data: ViewData<Value>

    init(_ source: Source, refresh: @escaping @MainActor () async -> Void = {}) {
        self.source = source
        self.refresh = refresh
        (updates, observed) = AsyncStream.makeStream()
    }
}

private actor RelationshipService {
    private var fails = false
    func setFailure(_ fails: Bool) { self.fails = fails }
    func name(_ id: Int) throws -> String {
        if fails { throw Failure.test }
        return "name-\(id)"
    }
}

private nonisolated struct NamedEntry: Identifiable, Sendable { let id: Int; let name: String }

extension IntegrationTests {
    @MainActor @Test func partitionRetryKeepsViewDataLoading() async throws {
        let remote = RetryRemote()
        let buckets = PartitionedBucket(Key<Int>("ensemble.partition.retry"), partitionedBy: String.self) { _ in
            RemoteSource { try await remote.load() }
        }
        let partition = buckets["selected"]
        let composition = Composition { Input(partition) } transform: { $0 }

        await composition.load(using: .remote)
        let model = Consumer(composition) { await composition.load(using: .remote) }
        let data = model.data
        var received = model.updates.makeAsyncIterator()
        data.connect()
        guard case .result(.failure)? = await received.next() else {
            Issue.record("Expected the initial partition failure")
            return
        }
        #expect(data.phase.kind == .failure)

        let reload = Task { await data.reload() }
        await remote.waitForRetry()
        #expect(partition.isLoading)
        #expect(data.phase.kind == .loading)

        await remote.completeRetry()
        await reload.value
        guard case .result(.success(9))? = await received.next() else {
            Issue.record("Expected success without a reset or repeated failure during the retry")
            return
        }
        #expect(data.phase.kind == .success)
        #expect(composition.error == nil)

        try await partition.reset()
        var update: Update<Int>?
        repeat { update = await received.next() } while isSuccess(update)
        guard case .reset? = update else {
            Issue.record("Expected a reset after clearing the partition")
            return
        }
        #expect(data.phase.kind == .empty)
    }

    private func isSuccess(_ update: Update<Int>?) -> Bool {
        if case .result(.success)? = update { return true }
        return false
    }
}

private actor RetryRemote {
    private var firstLoad = true
    private var retry: CheckedContinuation<Int, Never>?
    private var started: CheckedContinuation<Void, Never>?

    func load() async throws -> Int {
        if firstLoad {
            firstLoad = false
            throw Failure.test
        }
        return await withCheckedContinuation { continuation in
            retry = continuation
            started?.resume()
            started = nil
        }
    }

    func waitForRetry() async {
        if retry != nil { return }
        await withCheckedContinuation { started = $0 }
    }

    func completeRetry() {
        retry?.resume(returning: 9)
        retry = nil
    }
}
