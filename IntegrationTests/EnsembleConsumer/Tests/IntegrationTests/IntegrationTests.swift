import Continuum
import Ensemble
import Observation
import Testing

@MainActor @Observable
final class Source: UpdateSource {
    var update: Update<Int> = .reset
    func _latestUpdateForObservation() -> Update<Int> { update }
}

enum Failure: Error { case test }

@Suite(.timeLimit(.minutes(1)))
struct IntegrationTests {
    @MainActor @Test func directBucketBinding() async throws {
        let bucket = Bucket(Key<Int>("ensemble.direct"))
        let context = ViewDataContext()
        let data = ViewData<Int>()
        let (updates, observed) = AsyncStream<Update<Int>>.makeStream()
        var received = updates.makeAsyncIterator()
        context.bind({ bucket }, to: data) { update, sink in
            switch update {
            case .result(let result): sink.receive(result)
            case .reset: sink.reset()
            }
            observed.yield(update)
        }
        for _ in 0..<10 { await Task.yield() }
        if case .loading = data.phase {} else { Issue.record("Initial silence must preserve loading") }
        try await bucket.store(7)
        _ = await received.next()
        if case .available(7) = data.latestValue {} else { Issue.record("Expected direct bucket value") }
        try await bucket.reset()
        guard case .reset? = await received.next() else {
            Issue.record("Expected reset from direct bucket iteration")
            return
        }
        if case .unavailable = data.latestValue {} else { Issue.record("Expected reset to clear data") }
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
            Input(child) { policy in await child.load(using: policy) }
        } transform: { $0 }
        let context = ViewDataContext()
        let data = ViewData<String>()
        let (updates, observed) = AsyncStream<Update<String>>.makeStream()
        var received = updates.makeAsyncIterator()
        context.bind({ composition }, to: data) { update, sink in
            switch update {
            case .result(let result): sink.receive(result)
            case .reset: sink.reset()
            }
            observed.yield(update)
        }
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
            Input(inner) { policy in await inner.load(using: policy) }
        } transform: { $0 + 0 }
        let composition = Composition {
            Input(middle) { policy in await middle.load(using: policy) }
        } transform: { $0 + 0 }
        let context = ViewDataContext()
        let data = ViewData<Int>()
        let (updates, observed) = AsyncStream<Update<Int>>.makeStream()
        var received = updates.makeAsyncIterator()
        context.bind({ composition }, to: data, reload: .refresh {
            Task { await composition.load(using: .remote) }
        }) { update, sink in
            switch update {
            case .result(let result): sink.receive(result)
            case .reset: sink.reset()
            }
            observed.yield(update)
        }
        for _ in 0..<20 { await Task.yield() }
        if case .loading = data.phase {} else { Issue.record("Initial silence must preserve loading") }
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
        context.reload(data)
        while let update = await received.next() {
            if case .result(.success(30)) = update { break }
        }
        if case .available(30) = data.latestValue {} else { Issue.record("Retry must recover") }
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
