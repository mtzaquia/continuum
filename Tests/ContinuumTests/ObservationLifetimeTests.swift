import Continuum
import Observation
import Testing

@Suite("Observation lifetime", .timeLimit(.minutes(1)))
struct ObservationLifetimeTests {
    @Test("Accessed partitions live until their owning bucket is released")
    func partitionRetention() {
        var bucket: PartitionedValueBucket<Int, Int>? = Bucket(
            Key<Int>("lifetime.partition"), partitionedBy: Int.self
        ) { _ in }
        weak var selected = bucket?[42]
        #expect(selected != nil)
        #expect(selected === bucket?[42])
        bucket = nil
        #expect(selected == nil)
    }

    @Test("Cancelling iteration releases the observed source")
    func cancellationReleasesSource() async {
        let (released, release) = AsyncStream<Void>.makeStream()
        let (started, start) = AsyncStream<Void>.makeStream()
        let consumer = Task {
            let composition = Composition {
                Input(LifetimeSource(release: release))
            } transform: { $0 }
            for await _ in composition { start.yield(()) }
        }
        var starts = started.makeAsyncIterator()
        _ = await starts.next()
        consumer.cancel()
        await consumer.value
        var releases = released.makeAsyncIterator()
        #expect(await releases.next() != nil)
    }

    @Test("A slow consumer retains each result already emitted by observation")
    func slowConsumerBuffersResults() async throws {
        let bucket = Bucket(Key<Int>("lifetime.buffer")) { LocalSource { 1 } }
        try await bucket.load()
        let (evaluated, acknowledgement) = AsyncStream<Int>.makeStream()
        let composition = Composition { Input(bucket) } transform: { value in
            acknowledgement.yield(value)
            return value
        }
        var updates = composition.makeAsyncIterator()
        if case .result(.success(1))? = await updates.next() {} else {
            Issue.record("Expected the initial snapshot")
        }
        var acknowledgements = evaluated.makeAsyncIterator()
        #expect(await acknowledgements.next() == 1)
        try await bucket.put(2)
        #expect(await acknowledgements.next() == 2)
        try await bucket.put(3)
        #expect(await acknowledgements.next() == 3)

        for expected in [2, 3] {
            guard case .result(.success(let value))? = await updates.next() else {
                Issue.record("Expected a buffered snapshot")
                return
            }
            #expect(value == expected)
        }
    }
}

@MainActor
@Observable
private final class LifetimeSource: UpdateSource {
    let release: AsyncStream<Void>.Continuation
    var latest: Update<Int> { .result(.success(1)) }

    init(release: AsyncStream<Void>.Continuation) { self.release = release }

    deinit {
        release.yield(())
        release.finish()
    }
}
