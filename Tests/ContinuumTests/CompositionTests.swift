import Continuum
import Observation
import Testing

@Suite("Bucket composition", .timeLimit(.minutes(1)))
struct CompositionTests {
    @Test("Concrete bucket values, partitions and observable values reach the transform")
    func concreteInputs() async throws {
        let user = Bucket(Key<String>("composition.user"))
        let favorites = Bucket(IndexedKey<Int, Favorite>("composition.favorites", indexedBy: \.id))
        let partitioned = Bucket(Key<Int>("composition.partition"), partitionedBy: String.self) { _ in }
        let preferences = Preferences()
        try await user.store("Sam")
        try await partitioned["selected"].store(42)
        let composition = Composition {
            Input(user)
            Input(favorites).optional()
            Input(partitioned["selected"])
            Input { preferences.language }
        } transform: { user, favorites, partition, language in
            // These annotations prove that no wrapper reaches the transform.
            let name: String = user
            let items: [Favorite]? = favorites
            let count: Int = partition
            return "\(name):\(items?.count ?? 0):\(count):\(language)"
        }
        #expect(success(composition.latest) == "Sam:0:42:en")
        var iterator = composition.makeAsyncIterator()
        #expect(success(await iterator.next()) == "Sam:0:42:en")
        preferences.language = "nl"
        #expect(success(await iterator.next()) == "Sam:0:42:nl")
        try await favorites.store(Favorite(id: 1))
        #expect(success(await iterator.next()) == "Sam:1:42:nl")
        try await favorites.reset()
        #expect(success(await iterator.next()) == "Sam:0:42:nl")
    }

    @Test("Initial unavailable compositions are silent and recover after reset")
    func initialSilence() async {
        let source = Source<Int>()
        let composition = Composition { Input(source) } transform: { $0 + 1 }
        let first = Task {
            var iterator = composition.makeAsyncIterator()
            return await iterator.next()
        }
        for _ in 0..<10 { await Task.yield() }
        source.update = .result(.success(1))
        #expect(success(await first.value) == 2)
        var ongoing = composition.makeAsyncIterator()
        #expect(success(await ongoing.next()) == 2)
        source.update = .reset
        #expect(isReset(await ongoing.next()))
        source.update = .result(.success(3))
        #expect(success(await ongoing.next()) == 4)
    }

    @Test("Required failures recover; optional failures contribute nil")
    func failures() async {
        let required = Source<Int>(.result(.success(1)))
        let optional = Source<Int>(.result(.failure(TestError.failed)))
        let composition = Composition {
            Input(required)
            Input(optional).optional()
        } transform: { $0 + ($1 ?? 0) }
        var iterator = composition.makeAsyncIterator()
        #expect(success(await iterator.next()) == 1)
        required.update = .result(.failure(TestError.failed))
        #expect(isFailure(await iterator.next()))
        required.update = .result(.success(2))
        #expect(success(await iterator.next()) == 2)
    }

    @Test("Reset clears retained presentation data before an accompanying failure")
    func resetBeforeFailure() async {
        let a = Source<Int>(.result(.success(1)))
        let b = Source<Int>(.result(.success(2)))
        let composition = Composition {
            Input(a)
            Input(b)
        } transform: { $0 + $1 }
        var iterator = composition.makeAsyncIterator()
        #expect(success(await iterator.next()) == 3)
        a.update = .reset
        b.update = .result(.failure(TestError.failed))
        #expect(isReset(await iterator.next()))
        #expect(isFailure(await iterator.next()))
        a.update = .result(.success(4))
        #expect(isFailure(await iterator.next()))
        b.update = .result(.success(5))
        #expect(success(await iterator.next()) == 9)
    }

    @Test("Builder branches preserve typed slots, including multiple optional inputs", arguments: [false, true])
    func branches(flag: Bool) {
        let composition = Composition {
            if flag {
                Input { 1 }
                Input { "first" }
            } else {
                Input { 2 }
                Input { "second" }
            }
            if flag {
                Input { true }
                Input { 3 }
            }
        } transform: { number, text, enabled, extra in
            let optionalBool: Bool? = enabled
            let optionalInt: Int? = extra
            return "\(number + 1):\(text.uppercased()):\(optionalBool == nil):\(optionalInt == nil)"
        }
        #expect(success(composition.latest) == (flag ? "2:FIRST:false:false" : "3:SECOND:true:true"))
    }

    @Test("Loading forwards policies to bucket and observable inputs, while passive inputs stay passive")
    func loading() async {
        let source = Source<Int>()
        let preferences = Preferences()
        var calls: [String] = []
        let composition = Composition {
            Input(source) { policy in
                if case .remote = policy { calls.append("snapshot") }
                source.update = .result(.success(7))
            }
            Input { preferences.language } load: { policy in
                if case .remote = policy { calls.append("value") }
                preferences.language = "nl"
            }
        } transform: { "\($0 + 1):\($1)" }
        #expect(calls.isEmpty)
        await composition.load(using: .remote)
        #expect(calls.sorted() == ["snapshot", "value"])
        #expect(success(composition.latest) == "8:nl")
        #expect(!composition.isLoading)
    }

    @Test("Required loading errors fail output; optional loading errors do not cancel siblings")
    func actionErrors() async {
        let source = Source<Int>(.result(.success(1)))
        var shouldFail = true
        var siblingFinished = false
        let composition = Composition {
            Input(source) { _ in
                if shouldFail { throw TestError.failed }
            }
            Input { 2 } load: { _ in
                await Task.yield()
                siblingFinished = true
                throw TestError.failed
            }
            .optional()
        } transform: { $0 + ($1 ?? 0) }
        await composition.load()
        #expect(isFailure(composition.latest))
        #expect(siblingFinished)
        shouldFail = false
        await composition.load()
        #expect(success(composition.latest) == 1)
    }

    @Test("A thrown transform error is a recoverable outcome")
    func transformError() async {
        let preferences = Preferences()
        let composition = Composition { Input { preferences.language } } transform: { language in
            if language == "en" { throw TestError.failed }
            return language.uppercased()
        }
        var iterator = composition.makeAsyncIterator()
        #expect(isFailure(await iterator.next()))
        preferences.language = "nl"
        #expect(success(await iterator.next()) == "NL")
    }

    @Test("Subscribers receive independent updates and cancel independently")
    func subscribers() async {
        let source = Source<Int>(.result(.success(1)))
        let composition = Composition { Input(source) } transform: { $0 + 1 }
        var a = composition.makeAsyncIterator()
        var b = composition.makeAsyncIterator()
        #expect(success(await a.next()) == 2)
        #expect(success(await b.next()) == 2)
        let cancelled = Task {
            var iterator = composition.makeAsyncIterator()
            _ = await iterator.next()
            return await iterator.next()
        }
        cancelled.cancel()
        #expect(await cancelled.value == nil)
        source.update = .result(.success(2))
        #expect(success(await b.next()) == 3)
    }

    @Test("Bucket loading errors recover when the bucket succeeds outside the composition")
    func bucketLoadRecovery() async throws {
        let bucket = Bucket(Key<Int>("composition.recovery")) {
            RemoteSource { throw TestError.failed }
        }
        let composition = Composition {
            Input(bucket) { policy in try await bucket.load(using: policy) }
        } transform: { $0 + 1 }
        await composition.load(using: .remote)
        #expect(isFailure(composition.latest))
        var iterator = composition.makeAsyncIterator()
        #expect(isFailure(await iterator.next()))
        try await bucket.store(5)
        #expect(success(await iterator.next()) == 6)
    }

    @Test("Overlapping loads retain loading state and ignore obsolete action errors")
    func overlappingLoads() async {
        let (started, start) = AsyncStream<Void>.makeStream()
        let (release, unblock) = AsyncStream<Void>.makeStream()
        var calls = 0
        let composition = Composition {
            Input { 1 } load: { _ in
                calls += 1
                if calls == 1 {
                    start.yield(())
                    for await _ in release { break }
                    throw TestError.failed
                }
            }
        } transform: { $0 + 1 }
        let first = Task { await composition.load() }
        var starts = started.makeAsyncIterator()
        _ = await starts.next()
        #expect(composition.isLoading)
        await composition.load(using: .remote)
        #expect(composition.isLoading)
        unblock.yield(())
        await first.value
        #expect(!composition.isLoading)
        #expect(success(composition.latest) == 2)
    }

    @Test("Cancellation reaches loading actions without failing the composition")
    func cancellation() async {
        let (started, start) = AsyncStream<Void>.makeStream()
        let (blocked, _) = AsyncStream<Void>.makeStream()
        var cancelled = false
        let composition = Composition {
            Input { 1 } load: { _ in
                start.yield(())
                for await _ in blocked {}
                cancelled = Task.isCancelled
                try Task.checkCancellation()
            }
        } transform: { $0 + 1 }
        let loading = Task { await composition.load() }
        var starts = started.makeAsyncIterator()
        _ = await starts.next()
        loading.cancel()
        await loading.value
        #expect(cancelled)
        #expect(!composition.isLoading)
        #expect(success(composition.latest) == 2)
    }

    @Test("Domain nil is a successful concrete value")
    func domainNil() {
        let composition = Composition {
            Input { Optional<Int>.none }
        } transform: { value in value == nil }
        #expect(success(composition.latest) == true)
    }

    @Test("Optional-valued snapshots flatten only when the dependency is optional")
    func flattenedOptional() async throws {
        let bucket = Bucket(Key<Int?>("composition.optional-value"))
        let descriptor = Input(bucket).optional()
        // Infer the overload first, then verify its exact type without context.
        let _: Input<Int?> = descriptor
        let repeated = descriptor.optional()
        let _: Input<Int?> = repeated
        let composition = Composition { descriptor } transform: { value in
            let concrete: Int? = value
            return concrete ?? -1
        }
        #expect(success(composition.latest) == -1)
        var iterator = composition.makeAsyncIterator()
        #expect(success(await iterator.next()) == -1)
        try await bucket.store(Optional<Int>.none)
        #expect(success(await iterator.next()) == -1)
        try await bucket.store(Optional.some(7))
        #expect(success(await iterator.next()) == 7)
        try await bucket.reset()
        #expect(success(await iterator.next()) == -1)
    }

    @Test("Required optional values gate output and fail; relaxed optional values do neither")
    func optionalRequirement() async {
        let source = Source<Int?>()
        let required = Composition { Input(source) } transform: { $0 ?? -1 }
        let relaxed = Composition { Input(source).optional() } transform: { $0 ?? -1 }
        #expect(isReset(required.latest))
        #expect(success(relaxed.latest) == -1)
        var iterator = required.makeAsyncIterator()
        source.update = .result(.success(nil))
        #expect(success(await iterator.next()) == -1)
        source.update = .result(.failure(TestError.failed))
        #expect(isFailure(await iterator.next()))
        var optionalUpdates = relaxed.makeAsyncIterator()
        #expect(success(await optionalUpdates.next()) == -1)
    }

    @Test("Flattened optional inputs retain loading actions and suppress their errors")
    func flattenedLoading() async {
        let preferences = Preferences()
        var calls = 0
        let input = Input { Optional(preferences.language) } load: { policy in
            if case .remote = policy { calls += 1 }
            throw TestError.failed
        }
        let optionalInput = input.optional()
        let _: Input<String?> = optionalInput
        let composition = Composition { optionalInput } transform: { $0 ?? "missing" }
        #expect(success(composition.latest) == "en")
        await composition.load(using: .remote)
        #expect(calls == 1)
        #expect(success(composition.latest) == "missing")
    }

    @Test("Nested compositions preserve reset before failure through multiple levels")
    func nestedReset() async {
        let user = Source<Int>(.result(.success(1)))
        let permissions = Source<Int>(.result(.success(2)))
        let inner = Composition {
            Input(user)
            Input(permissions)
        } transform: { $0 + $1 }
        let middle = Composition { Input(inner) } transform: { $0 * 2 }
        let outer = Composition { Input(middle) } transform: { $0 + 1 }
        var iterator = outer.makeAsyncIterator()
        #expect(success(await iterator.next()) == 7)
        // The inner composition publishes both updates in a single synchronous
        // evaluation; its latest property is already failure when parents read it.
        user.update = .reset
        permissions.update = .result(.failure(TestError.failed))
        #expect(isReset(await iterator.next()))
        #expect(isFailure(await iterator.next()))
        user.update = .result(.success(3))
        permissions.update = .result(.success(4))
        #expect(success(await iterator.next()) == 15)
    }

    @Test("Optional nested compositions consume resets and errors as nil")
    func optionalNestedReset() async {
        let a = Source<Int>(.result(.success(1)))
        let b = Source<Int>(.result(.success(2)))
        let inner = Composition {
            Input(a)
            Input(b)
        } transform: { $0 + $1 }
        let middle = Composition { Input(inner).optional() } transform: { $0 ?? -1 }
        let outer = Composition { Input(middle) } transform: { $0 * 2 }
        var iterator = outer.makeAsyncIterator()
        #expect(success(await iterator.next()) == 6)
        a.update = .reset
        b.update = .result(.failure(TestError.failed))
        #expect(success(await iterator.next()) == -2)
        a.update = .result(.success(4))
        b.update = .result(.success(5))
        #expect(success(await iterator.next()) == 18)
    }

    @Test("Nested loading is explicit and initial unavailability stays silent")
    func nestedLoading() async {
        let source = Source<Int>()
        var policies: [String] = []
        let inner = Composition {
            Input(source) { policy in
                if case .remote = policy { policies.append("remote") }
                source.update = .result(.success(4))
            }
        } transform: { $0 * 2 }
        let passive = Composition { Input(inner) } transform: { $0 + 1 }
        await passive.load(using: .remote)
        #expect(policies.isEmpty)
        #expect(isReset(passive.latest))
        let outer = Composition {
            Input(inner) { policy in await inner.load(using: policy) }
        } transform: { $0 + 1 }
        let first = Task {
            var iterator = outer.makeAsyncIterator()
            return await iterator.next()
        }
        for _ in 0..<10 { await Task.yield() }
        await outer.load(using: .remote)
        #expect(success(await first.value) == 9)
        #expect(success(outer.latest) == 9)
        #expect(policies == ["remote"])
    }

    @Test("New parents start from the current outcome without replaying historical resets")
    func nestedResetBaseline() async {
        let a = Source<Int>(.result(.success(1)))
        let b = Source<Int>(.result(.success(2)))
        let inner = Composition {
            Input(a)
            Input(b)
        } transform: { $0 + $1 }
        var updates = inner.makeAsyncIterator()
        _ = await updates.next()
        a.update = .reset
        b.update = .result(.failure(TestError.failed))
        #expect(isReset(await updates.next()))
        #expect(isFailure(await updates.next()))
        let outer = Composition { Input(inner) } transform: { $0 * 2 }
        var iterator = outer.makeAsyncIterator()
        #expect(isFailure(await iterator.next()))
    }

    @Test("Buckets, partitions and compositions share observable outcomes and direct iteration")
    func uniformSources() async throws {
        let bucket = Bucket(Key<Int>("uniform.bucket"))
        let partitions = Bucket(Key<Int>("uniform.partition"), partitionedBy: String.self) { _ in }
        let partition = partitions["one"]
        #expect(isReset(bucket.latest))
        #expect(isReset(partition.latest))
        let (changes, changed) = AsyncStream<Void>.makeStream()
        withObservationTracking { _ = bucket.latest } onChange: { changed.yield(()) }
        try await bucket.store(3)
        var observed = changes.makeAsyncIterator()
        #expect(await observed.next() != nil)
        try await partition.store(4)
        let composition = Composition { Input(bucket) } transform: { $0 + 2 }
        #expect(success(bucket.latest) == 3)
        #expect(success(partition.latest) == 4)
        #expect(success(try await firstUpdate(bucket)) == 3)
        #expect(success(try await firstUpdate(partition)) == 4)
        #expect(success(try await firstUpdate(composition)) == 5)
    }

    @Test("Direct bucket iteration stays silent initially, emits resets and recovers")
    func directBucketReset() async throws {
        let bucket = Bucket(Key<Int>("uniform.reset"))
        let first = Task { try await firstUpdate(bucket) }
        for _ in 0..<10 { await Task.yield() }
        try await bucket.store(1)
        #expect(success(try await first.value) == 1)
        var iterator = bucket.makeAsyncIterator()
        #expect(success(await iterator.next()) == 1)
        try await bucket.reset()
        #expect(isReset(await iterator.next()))
        #expect(isReset(bucket.latest))
        try await bucket.store(2)
        #expect(success(await iterator.next()) == 2)
    }

    @Test("Bucket errors and recovery agree between latest and direct iteration")
    func directBucketFailure() async throws {
        let bucket = Bucket(Key<Int>("uniform.failure")) {
            RemoteSource { throw TestError.failed }
        }
        try await bucket.store(1)
        var iterator = bucket.makeAsyncIterator()
        _ = await iterator.next()
        do { try await bucket.load(using: .remote) } catch {}
        #expect(isFailure(await iterator.next()))
        #expect(isFailure(bucket.latest))
        try await bucket.store(2)
        #expect(success(await iterator.next()) == 2)
        #expect(success(bucket.latest) == 2)
    }

    @Test("Direct bucket subscribers cancel independently")
    func directSubscribers() async throws {
        let bucket = Bucket(Key<Int>("uniform.subscribers"))
        try await bucket.store(1)
        var other = bucket.makeAsyncIterator()
        #expect(success(await other.next()) == 1)
        let (started, start) = AsyncStream<Void>.makeStream()
        let consumer = Task {
            var iterator = bucket.makeAsyncIterator()
            _ = await iterator.next()
            start.yield(())
            return await iterator.next()
        }
        var starts = started.makeAsyncIterator()
        _ = await starts.next()
        consumer.cancel()
        #expect(await consumer.value == nil)
        try await bucket.store(2)
        #expect(success(await other.next()) == 2)
    }

    @Test("Releasing a direct iterator releases its retained bucket")
    func directIteratorLifetime() async throws {
        var bucket: DataBucket<Int>? = Bucket(Key<Int>("uniform.lifetime"))
        try await bucket?.store(1)
        weak var weakBucket = bucket
        var iterator = bucket?.makeAsyncIterator()
        #expect(success(await iterator?.next()) == 1)
        bucket = nil
        #expect(weakBucket != nil)
        iterator = nil
        for _ in 0..<20 { await Task.yield() }
        #expect(weakBucket == nil)
        weakBucket = nil
    }

    @Test("Failure mapping sees every required failure in declaration order")
    func failureMapping() {
        let first = Source<Int>(.result(.failure(NumberedError(number: 1))))
        let second = Source<Int>(.result(.failure(NumberedError(number: 2))))
        let third = Source<Int>(.result(.failure(NumberedError(number: 3))))
        var numbers: [Int] = []
        let composition = Composition {
            Input(first)
            Input(second).optional()
            Input(third)
        } mapFailures: { errors in
            numbers = errors.compactMap { ($0 as? NumberedError)?.number }
            return TestError.failed
        } transform: { $0 + ($1 ?? 0) + $2 }
        #expect(isFailure(composition.latest))
        #expect(numbers == [1, 3])
    }

    @Test("Releasing an iterator releases its subscription and retained composition")
    func iteratorLifetime() async {
        var source: Source<Int>? = Source(.result(.success(1)))
        var composition: Composition<Int>? = Composition {
            Input(source!)
        } transform: { $0 + 1 }
        weak var weakComposition = composition
        weak var weakSource = source
        var iterator = composition?.makeAsyncIterator()
        #expect(success(await iterator?.next()) == 2)
        composition = nil
        source = nil
        #expect(weakComposition != nil)
        iterator = nil
        for _ in 0..<10 { await Task.yield() }
        #expect(weakComposition == nil)
        #expect(weakSource == nil)
        weakComposition = nil
        weakSource = nil
    }

    @Test("Observation does not retain an abandoned composition or its dependencies")
    func lifetime() async {
        var source: Source<Int>? = Source(.result(.success(1)))
        weak var weakSource = source
        var composition: Composition<Int>? = Composition {
            Input(source!)
        } transform: { $0 + 1 }
        weak var weakComposition = composition
        #expect(success(composition?.latest) == 2)
        composition = nil
        source = nil
        #expect(weakComposition == nil)
        #expect(weakSource == nil)
        weakSource = nil
        weakComposition = nil
    }
}

nonisolated private struct Favorite: Sendable { let id: Int }
private enum TestError: Error { case failed }

@MainActor @Observable
private final class Preferences { var language = "en" }

@MainActor @Observable
private final class Source<T: Sendable>: UpdateSource {
    var update: Update<T>
    init(_ update: Update<T> = .reset) { self.update = update }
    func _latestUpdateForObservation() -> Update<T> { update }
}

private func success<T>(_ update: Update<T>?) -> T? {
    if case .result(.success(let value)) = update { return value }
    return nil
}
private func isReset<T>(_ update: Update<T>?) -> Bool {
    if case .reset = update { return true }
    return false
}
private func isFailure<T>(_ update: Update<T>?) -> Bool {
    if case .result(.failure) = update { return true }
    return false
}

private struct NumberedError: Error { let number: Int }


@concurrent private func firstUpdate<Source: AsyncSequence & Sendable>(_ source: Source) async throws -> Update<Int>?
where Source.Element == Update<Int> {
    for try await update in source { return update }
    return nil
}
