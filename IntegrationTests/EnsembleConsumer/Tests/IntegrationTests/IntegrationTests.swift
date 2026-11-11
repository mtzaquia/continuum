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
            RemoteSource { throw Failure.test }
        }
        let model = Consumer(bucket) { _ = try? await bucket.load(using: .remote) }
        let data = model.data
        var received = model.updates.makeAsyncIterator()
        data.connect()
        for _ in 0..<10 { await Task.yield() }
        #expect(data.phase.kind == .empty)
        try await bucket.store(7)
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
            RemoteSource { throw Failure.test }
        }
        try await bucket.store(7)
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
            RemoteSource {
                Load { [NamedEntry]() }
                LoadEntry { id in NamedEntry(id: id, name: try await service.name(id)) }
            }
        }
        let child = Composition {
            Input { [1] }.resolving(\.self, from: names)
        } transform: { _, names in names[1]?.name ?? "" }
        let composition = Composition {
            Input(child)
        } transform: { $0 }
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
        await service.setFailure(true)
        await composition.load(using: .remote)
        var sawReset = false
        while let update = await received.next() {
            if case .reset = update { sawReset = true }
            if case .result(.failure) = update { break }
        }
        #expect(sawReset)
        if case .unavailable = data.latestValue {} else { Issue.record("Reset relationships must clear retained data") }
        await service.setFailure(false)
        await composition.load(using: .remote)
        while let update = await received.next() {
            if case .result(.success("name-1")) = update { break }
        }
        if case .available("name-1") = data.latestValue {} else { Issue.record("Relationship retry must recover") }
    }

    @MainActor @Test func bindingResetFailureAndRetry() async {
        let a = Source()
        let b = Source()
        let inner = Composition {
            Input(a) { _ in a.update = .result(.success(10)) }
            Input(b) { _ in b.update = .result(.success(20)) }
        } transform: { $0 + $1 }
        let middle = Composition {
            Input(inner)
        } transform: { $0 + 0 }
        let composition = Composition {
            Input(middle)
        } transform: { $0 + 0 }
        let model = Consumer(composition) { await composition.load(using: .remote) }
        let data = model.data
        var received = model.updates.makeAsyncIterator()
        data.connect()
        for _ in 0..<20 { await Task.yield() }
        #expect(data.phase.kind == .empty)
        await composition.load()
        // Loading can publish intermediate outcomes; wait for the final success.
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
        refresh: { model in await model.refresh() },
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
