import Continuum
import Testing

@Suite("Observation lifetime", .timeLimit(.minutes(1)))
struct ObservationLifetimeTests {
    @Test("Accessed partitions live until their owning bucket is released")
    func partitionRetention() {
        var bucket: PartitionedDataBucket<Int, Int>? = Bucket(
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
        let stream = bucketUpdates(observing: LifetimeSource(release: release)) { $0 }
        let (started, start) = AsyncStream<Void>.makeStream()
        let consumer = Task {
            for await _ in stream { start.yield(()) }
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
        let bucket = Bucket(Key<Int>("lifetime.buffer"))
        try await bucket.store(1)
        let (evaluated, acknowledgement) = AsyncStream<Int>.makeStream()
        let stream = bucketUpdates(observing: bucket) { value in
            acknowledgement.yield(value)
            return value
        }
        var acknowledgements = evaluated.makeAsyncIterator()
        #expect(await acknowledgements.next() == 1)
        try await bucket.store(2)
        #expect(await acknowledgements.next() == 2)
        try await bucket.store(3)
        #expect(await acknowledgements.next() == 3)

        let consumer = Task {
            var values: [Int] = []
            for await update in stream {
                if case .result(.success(let value)) = update { values.append(value) }
                if values.count == 3 { return values }
            }
            return values
        }
        #expect(await consumer.value == [1, 2, 3])
        // Explicit cancellation of iteration terminates the observation even
        // while the stream value is retained by its caller.
        let cleanup = Task {
            for await _ in stream {}
        }
        cleanup.cancel()
        await cleanup.value
    }
}

@MainActor
private final class LifetimeSource: BucketUpdateSource {
    let release: AsyncStream<Void>.Continuation

    init(release: AsyncStream<Void>.Continuation) { self.release = release }

    func _latestUpdateForObservation() -> BucketUpdate<Int> { .result(.success(1)) }

    deinit {
        release.yield(())
        release.finish()
    }
}
