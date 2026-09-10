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
