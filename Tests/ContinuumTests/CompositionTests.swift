import Continuum
import Observation
import Testing

@Suite("Bucket composition", .timeLimit(.minutes(1)))
struct CompositionTests {
    @Test("Concrete bucket values, partitions and observable values reach the transform")
    func concreteInputs() async throws {
        let user = Bucket(Key<String>("composition.user")) { LocalSource { "Sam" } }
        let favorites = Bucket(IndexedKey<Int, Favorite>("composition.favorites", indexedBy: \.id)) {
            LocalSource { [Favorite(id: 1)] }
        }
        let partitioned = Bucket(Key<Int>("composition.partition"), partitionedBy: String.self) { _ in LocalSource { 42 } }
        let preferences = Preferences()
        try await user.load()
        try await partitioned["selected"].load()
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
        try await favorites.load()
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

    @Test("Observable update inputs gate, fail, reset, and recover from repository phase changes")
    func observableUpdateAvailability() async {
        let repository = RepositoryState<Int?>()
        let required = Composition { Input(update: { repository.update }) } transform: { $0 ?? -2 }
        let optionalInput: Input<Int?> = Input(update: { repository.update }).optional()
        let optional = Composition { optionalInput } transform: { $0 ?? -1 }
        #expect(isReset(required.latest))
        #expect(success(optional.latest) == -1)
        var requiredUpdates = required.makeAsyncIterator()
        var optionalUpdates = optional.makeAsyncIterator()
        #expect(success(await optionalUpdates.next()) == -1)

        repository.phase = .loaded(nil)
        #expect(success(await requiredUpdates.next()) == -2)
        #expect(success(await optionalUpdates.next()) == -1)
        repository.phase = .loaded(7)
        #expect(success(await requiredUpdates.next()) == 7)
        #expect(success(await optionalUpdates.next()) == 7)
        repository.phase = .failed(TestError.failed)
        #expect(isFailure(await requiredUpdates.next()))
        #expect(success(await optionalUpdates.next()) == -1)
        #expect((optional.error as? CompositionError)?.errors.first is TestError)

        repository.phase = .idle
        #expect(isReset(await requiredUpdates.next()))
        #expect(success(await optionalUpdates.next()) == -1)
        #expect(optional.error == nil)
        repository.phase = .loaded(8)
        #expect(success(await requiredUpdates.next()) == 8)
    }

    @Test("Observable update inputs forward policies and report the source failure once")
    func observableUpdateLoad() async {
        let repository = RepositoryState<Int>()
        var policies: [LoadPolicy] = []
        let composition = Composition {
            Input(update: { repository.update }, load: { policy in
                policies.append(policy)
                switch policy {
                case .cached: break
                case .cachedThenRemote: throw NumberedError(number: 1)
                case .remote:
                    repository.phase = .failed(NumberedError(number: 2))
                    throw NumberedError(number: 3)
                }
            })
        } transform: { $0 + 1 }

        #expect(policies.isEmpty)
        await composition.load(using: .cached)
        #expect(isReset(composition.latest))
        await composition.load(using: .cachedThenRemote)
        let actionFailure = composition.error as? CompositionError
        #expect(actionFailure?.errors.compactMap { ($0 as? NumberedError)?.number } == [1])
        await composition.load(using: .remote)
        let sourceFailure = composition.error as? CompositionError
        #expect(sourceFailure?.errors.compactMap { ($0 as? NumberedError)?.number } == [2])
        #expect(policies == [.cached, .cachedThenRemote, .remote])

        var updates = composition.makeAsyncIterator()
        #expect(isFailure(await updates.next()))
        repository.phase = .loaded(8)
        #expect(success(await updates.next()) == 9)
        #expect(composition.error == nil)
    }

    @Test("Retry keeps an observable query's load failure until success")
    func observableUpdateRetry() async {
        let repository = RepositoryState<Int>()
        let (started, start) = AsyncStream<Void>.makeStream()
        let (release, unblock) = AsyncStream<Void>.makeStream()
        let composition = Composition {
            Input(update: { repository.update }, load: { policy in
                guard policy == .remote else { throw NumberedError(number: 1) }
                start.yield(())
                for await _ in release { break }
                repository.phase = .loaded(8)
            })
        } transform: { $0 + 1 }

        await composition.load()
        var updates = composition.makeAsyncIterator()
        #expect(isFailure(await updates.next()))

        let retry = Task { await composition.load(using: .remote) }
        var starts = started.makeAsyncIterator()
        _ = await starts.next()
        #expect(composition.isLoading)
        #expect(isFailure(composition.latest))
        #expect(((composition.error as? CompositionError)?.errors.first as? NumberedError)?.number == 1)

        unblock.yield(())
        await retry.value
        var update: Update<Int>?
        repeat { update = await updates.next() } while isFailure(update)
        #expect(success(update) == 9)
        #expect(success(composition.latest) == 9)
        #expect(composition.error == nil)
    }

    @Test("Nested compositions preserve an observed reset before an accompanying failure")
    func observableUpdateNestedReset() async {
        let repository = RepositoryState<Int>()
        let permissions = RepositoryState<Int>()
        repository.phase = .loaded(1)
        permissions.phase = .loaded(2)
        let inner = Composition {
            Input(update: { repository.update })
            Input(update: { permissions.update })
        } transform: { $0 + $1 }
        let middle = Composition { Input(inner) } transform: { $0 * 2 }
        let outer = Composition { Input(middle) } transform: { $0 + 1 }
        var updates = outer.makeAsyncIterator()
        #expect(success(await updates.next()) == 7)

        repository.phase = .idle
        permissions.phase = .failed(TestError.failed)
        guard isReset(await updates.next()) else {
            Issue.record("Expected the reset before the failure")
            return
        }
        #expect(isFailure(await updates.next()))
        repository.phase = .loaded(3)
        permissions.phase = .loaded(4)
        #expect(success(await updates.next()) == 15)
    }

    @Test("Observable update inputs read the current outcome after coalesced writes")
    func observableUpdateCoalescing() async {
        let repository = RepositoryState<Int>()
        repository.phase = .loaded(1)
        let composition = Composition { Input(update: { repository.update }) } transform: { $0 }
        var updates = composition.makeAsyncIterator()
        #expect(success(await updates.next()) == 1)

        repository.phase = .idle
        repository.phase = .loaded(2)
        #expect(success(await updates.next()) == 2)
    }

    @Test("Observable array inputs can supply a relationship root")
    func observableUpdateRelationshipRoot() async {
        let repository = RepositoryState<[Favorite]>()
        let composition = Composition {
            Input(update: { repository.update }).resolving(\.id) { id, _ in "favorite-\(id)" }
        } transform: { favorites, names in favorites.compactMap { names[$0.id] } }
        #expect(isReset(composition.latest))
        var updates = composition.makeAsyncIterator()
        repository.phase = .loaded([Favorite(id: 7)])
        #expect(success(await updates.next()) == ["favorite-7"])
        repository.phase = .idle
        #expect(isReset(await updates.next()))
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

    @Test("Bucket and optional partition inputs automatically use the composition load policy",
          arguments: [LoadPolicy.cached, .cachedThenRemote, .remote])
    func automaticBucketLoading(policy: LoadPolicy) async {
        let calls = LocalLoadCalls()
        let bucket = Bucket(Key<Int>("composition.auto")) {
            LocalSource {
                await calls.record("bucket")
                return 1
            }
            RemoteSource { 2 }
        }
        let partitioned = Bucket(Key<String>("composition.auto.partition"), partitionedBy: String.self) { key in
            LocalSource {
                await calls.record(key)
                return "local-\(key)"
            }
            RemoteSource { "remote-\(key)" }
        }
        let composition = Composition {
            Input(bucket)
            Input(partitioned["selected"]).optional()
        } transform: { "\($0):\($1 ?? "none")" }
        #expect(isReset(composition.latest))
        #expect(isReset(bucket.latest))
        #expect(isReset(partitioned["selected"].latest))

        if policy == .cached {
            await composition.load()
        } else {
            await composition.load(using: policy)
        }

        #expect(success(composition.latest) == (policy == .cached ? "1:local-selected" : "2:remote-selected"))
        #expect(await calls.keys == (policy == .remote ? [] : ["bucket", "selected"]))
        #expect(isReset(partitioned["other"].latest))
        #expect(composition.error == nil)
    }

    @Test("An explicit bucket action replaces automatic loading and receives the policy")
    func customLoading() async {
        let source = Bucket(Key<Int>("composition.custom")) {
            LocalSource { 7 }
            RemoteSource { throw TestError.failed }
        }
        let preferences = Preferences()
        var calls: [String] = []
        let composition = Composition {
            Input(source) { policy in
                if case .remote = policy { calls.append("snapshot") }
                try await source.load()
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
        #expect(composition.error == nil)
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
        if case .result(.failure(let error as CompositionError))? = await iterator.next() {
            #expect(error.errors.first is TestError)
        } else { Issue.record("Expected the mapped transform error") }
        #expect((composition.error as? CompositionError)?.errors.first is TestError)
        preferences.language = "nl"
        #expect(success(await iterator.next()) == "NL")
        #expect(composition.error == nil)
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
            LocalSource { 5 }
            RemoteSource { throw TestError.failed }
        }
        let composition = Composition {
            Input(bucket)
        } transform: { $0 + 1 }
        await composition.load(using: .remote)
        #expect(isFailure(composition.latest))
        var iterator = composition.makeAsyncIterator()
        #expect(isFailure(await iterator.next()))
        try await bucket.load()
        while let update = await iterator.next() {
            if let value = success(update) {
                #expect(value == 6)
                return
            }
        }
        Issue.record("Expected the composition to recover after loading the bucket")
    }

    @Test("A failed partition refresh retains its value alongside an observation-only bucket")
    func retainedPartitionAfterCompositionLoad() async throws {
        let bucket = Bucket(Key<Int>("composition.partition"), partitionedBy: String.self) { _ in
            LocalSource { 10 }
            RemoteSource { throw TestError.failed }
        }
        let partition = bucket["selected"]
        let sibling = Bucket(Key<Int>("composition.sibling")) { LocalSource { 1 } }
        try await partition.load()
        try await sibling.load()
        let composition = Composition {
            Input(partition)
            Input(sibling) { _ in }
        } transform: { $0 + $1 }
        var current = composition.makeAsyncIterator()
        #expect(success(await current.next()) == 11)

        await composition.load(using: .remote)
        #expect(partition.error is TestError)
        #expect(sibling.error == nil)
        #expect((composition.error as? CompositionError)?.errors.count == 1)
        #expect((composition.error as? CompositionError)?.errors.first is TestError)
        #expect(success(composition.latest) == 11)
        #expect(success(try await firstUpdate(composition)) == 11)

        try await sibling.put(2)
        // Pending recomputations can repeat the retained success.
        while let update = await current.next() {
            guard let value = success(update) else {
                Issue.record("Expected the retained snapshot to stay available")
                break
            }
            if value == 12 { break }
        }
        #expect(success(composition.latest) == 12)
    }

    @Test("Overlapping loads retain loading state and ignore obsolete action outcomes",
          arguments: [false, true])
    func overlappingLoads(obsoleteLoadFails: Bool) async {
        let (started, start) = AsyncStream<Void>.makeStream()
        let (release, unblock) = AsyncStream<Void>.makeStream()
        let composition = Composition {
            Input { 1 } load: { policy in
                guard policy == .cached else { throw NumberedError(number: 2) }
                start.yield(())
                for await _ in release { break }
                if obsoleteLoadFails { throw NumberedError(number: 1) }
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
        #expect(isFailure(composition.latest))
        #expect(((composition.error as? CompositionError)?.errors.first as? NumberedError)?.number == 2)
    }

    @Test("Cancellation reaches a retry and preserves its previous error")
    func cancellation() async {
        let (started, start) = AsyncStream<Void>.makeStream()
        let (blocked, _) = AsyncStream<Void>.makeStream()
        var cancelled = false
        let composition = Composition {
            Input { 1 } load: { policy in
                guard policy == .remote else { throw TestError.failed }
                start.yield(())
                for await _ in blocked {}
                cancelled = Task.isCancelled
            }
        } transform: { $0 + 1 }
        await composition.load()
        let loading = Task { await composition.load(using: .remote) }
        var starts = started.makeAsyncIterator()
        _ = await starts.next()
        loading.cancel()
        await loading.value
        #expect(cancelled)
        #expect(!composition.isLoading)
        #expect(isFailure(composition.latest))
        #expect((composition.error as? CompositionError)?.errors.first is TestError)
    }

    @Test("Cancelling a top-level load cancels a shared child's action")
    func sharedChildCancellation() async {
        let (started, start) = AsyncStream<Void>.makeStream()
        var calls = 0
        var cancelled = false
        let child = Composition {
            Input { 1 } load: { _ in
                calls += 1
                start.yield(())
                do {
                    try await Task.sleep(for: .seconds(5))
                } catch {
                    cancelled = Task.isCancelled
                    throw error
                }
            }
        } transform: { $0 }
        let parent = Composition {
            Input(child)
            Input(child)
        } transform: { $0 + $1 }

        let loading = Task { await parent.load() }
        var starts = started.makeAsyncIterator()
        _ = await starts.next()
        loading.cancel()
        await loading.value

        #expect(calls == 1)
        #expect(cancelled)
        #expect(!child.isLoading)
        #expect(!parent.isLoading)
        #expect(parent.error == nil)
        #expect(success(parent.latest) == 2)
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
        let bucket = Bucket(Key<Int?>("composition.optional-value")) { LocalSource { .some(nil) } }
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
        try await bucket.load()
        #expect(success(await iterator.next()) == -1)
        try await bucket.put(Optional.some(7))
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

    @Test("Nested inputs forward the policy unless a closure overrides loading",
          arguments: [LoadPolicy.cached, .cachedThenRemote, .remote])
    func nestedLoading(policy: LoadPolicy) async {
        let source = Source<Int>()
        var policies: [LoadPolicy] = []
        let inner = Composition {
            Input(source) { policy in
                policies.append(policy)
                source.update = .result(.success(4))
            }
        } transform: { $0 * 2 }
        let passive = Composition { Input(inner) { _ in } } transform: { $0 + 1 }
        await passive.load(using: policy)
        #expect(policies.isEmpty)
        #expect(isReset(passive.latest))
        let middle = Composition { Input(inner) } transform: { $0 }
        let outer = Composition { Input(middle) } transform: { $0 + 1 }
        let first = Task {
            var iterator = outer.makeAsyncIterator()
            return await iterator.next()
        }
        #expect(isReset(outer.latest))
        await outer.load(using: policy)
        #expect(success(await first.value) == 9)
        #expect(success(outer.latest) == 9)
        await passive.load(using: policy)
        #expect(success(passive.latest) == 9)
        #expect(policies == [policy])

        let custom = Composition {
            Input(inner) { _ in await inner.load(using: .cached) }
        } transform: { $0 }
        await custom.load(using: .remote)
        #expect(policies == [policy, .cached])
    }

    @Test("A child shared across branches loads once for each overlapping or subsequent call")
    func sharedChildLoading() async {
        let (started, start) = AsyncStream<Void>.makeStream()
        let (release, unblock) = AsyncStream<Void>.makeStream()
        var policies: [LoadPolicy] = []
        let child = Composition {
            Input { 1 } load: { policy in
                policies.append(policy)
                if policies.count == 1 {
                    start.yield(())
                    for await _ in release {}
                }
            }
        } transform: { $0 }
        let left = Composition { Input(child) } transform: { $0 }
        let right = Composition { Input(child) } transform: { $0 }
        let parent = Composition {
            Input(left)
            Input(right)
            Input(child).optional()
        } transform: { $0 + $1 + ($2 ?? 0) }

        let first = Task { await parent.load(using: .remote) }
        var starts = started.makeAsyncIterator()
        _ = await starts.next()
        #expect(parent.isLoading)
        #expect(child.isLoading)
        await parent.load(using: .remote)
        #expect(parent.isLoading)
        #expect(child.isLoading)
        unblock.finish()
        await first.value
        #expect(!parent.isLoading)
        #expect(!child.isLoading)
        #expect(policies == [.remote, .remote])

        await parent.load()
        #expect(policies == [.remote, .remote, .cached])
        #expect(success(parent.latest) == 3)
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
        let bucket = Bucket(Key<Int>("uniform.bucket")) { LocalSource { 3 } }
        let partitions = Bucket(Key<Int>("uniform.partition"), partitionedBy: String.self) { _ in LocalSource { 4 } }
        let partition = partitions["one"]
        #expect(isReset(bucket.latest))
        #expect(isReset(partition.latest))
        let (changes, changed) = AsyncStream<Void>.makeStream()
        withObservationTracking { _ = bucket.latest } onChange: { changed.yield(()) }
        try await bucket.load()
        var observed = changes.makeAsyncIterator()
        #expect(await observed.next() != nil)
        try await partition.load()
        let composition = Composition { Input(bucket) } transform: { $0 + 2 }
        #expect(success(bucket.latest) == 3)
        #expect(success(partition.latest) == 4)
        #expect(success(try await firstUpdate(bucket)) == 3)
        #expect(success(try await firstUpdate(partition)) == 4)
        #expect(success(try await firstUpdate(composition)) == 5)
    }

    @Test("Every built-in update source exposes an observable latest outcome")
    func builtInSourceObservation() async throws {
        let bucket = Bucket(Key<Int>("uniform.observed-bucket")) { LocalSource { 1 } }
        #expect(try await observesSourceChange(bucket) {
            try await bucket.load()
        })

        let partitions = Bucket(
            Key<Int>("uniform.observed-partition"),
            partitionedBy: String.self
        ) { _ in LocalSource { 2 } }
        let partition = partitions["one"]
        #expect(try await observesSourceChange(partition) {
            try await partition.load()
        })

        let root = Bucket(Key<Int>("uniform.observed-composition")) { LocalSource { 3 } }
        let composition = Composition { Input(root) } transform: { $0 + 1 }
        #expect(try await observesSourceChange(composition) {
            try await root.load()
        })
    }

    @Test("Direct bucket iteration stays silent initially, emits resets and recovers")
    func directBucketReset() async throws {
        let bucket = Bucket(Key<Int>("uniform.reset")) {
            LocalSource { 1 }
            RemoteSource { 2 }
        }
        let first = Task { try await firstUpdate(bucket) }
        for _ in 0..<10 { await Task.yield() }
        try await bucket.load()
        #expect(success(try await first.value) == 1)
        var iterator = bucket.makeAsyncIterator()
        #expect(success(await iterator.next()) == 1)
        try await bucket.reset()
        #expect(isReset(await iterator.next()))
        #expect(isReset(bucket.latest))
        try await bucket.load(using: .remote)
        #expect(success(await iterator.next()) == 2)
    }

    @Test("A failed refresh replays the retained snapshot and a cached hit preserves the error")
    func directBucketFailure() async throws {
        let bucket = Bucket(Key<Int>("uniform.failure")) {
            LocalSource { 1 }
            RemoteSource { throw TestError.failed }
        }
        try await bucket.load()
        var iterator = bucket.makeAsyncIterator()
        #expect(success(await iterator.next()) == 1)
        await #expect(throws: TestError.self) { try await bucket.load(using: .remote) }
        #expect(success(await iterator.next()) == 1)
        #expect(success(bucket.latest) == 1)
        #expect(bucket.state.error is TestError)
        #expect(try await bucket.load(using: .cached) == 1)
        #expect(bucket.error is TestError)
        #expect(success(try await firstUpdate(bucket)) == 1)
        try await bucket.put(2)
        #expect(success(await iterator.next()) == 2)
        #expect(success(bucket.latest) == 2)
        #expect(bucket.error == nil)
    }

    @Test("Direct bucket subscribers cancel independently")
    func directSubscribers() async throws {
        let bucket = Bucket(Key<Int>("uniform.subscribers")) { LocalSource { 1 } }
        try await bucket.load()
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
        try await bucket.put(2)
        #expect(success(await other.next()) == 2)
    }

    @Test("Releasing a direct iterator releases its retained bucket")
    func directIteratorLifetime() async throws {
        var bucket: ValueBucket<Int>? = Bucket(Key<Int>("uniform.lifetime")) { LocalSource { 1 } }
        try await bucket?.load()
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

    @Test("A single mapping of all input errors supplies latest and error")
    func failureMapping() throws {
        let first = Source<Int>(.result(.failure(NumberedError(number: 1))))
        let second = Source<Int>(.result(.failure(NumberedError(number: 2))))
        let third = Source<Int>(.result(.failure(NumberedError(number: 3))))
        let unavailable = Source<Int>()
        var calls = 0
        let composition = Composition {
            Input(first)
            Input(second).optional()
            Input(unavailable)
            Input(third)
        } mapFailures: { errors in
            calls += 1
            return MappedError(numbers: errors.compactMap { ($0 as? NumberedError)?.number })
        } transform: { first, second, _, third in first + (second ?? 0) + third }
        let mapped = try #require(composition.error as? MappedError)
        #expect(calls == 1)
        #expect(mapped.numbers == [1, 2, 3])
        if case .result(.failure(let error as MappedError)) = composition.latest {
            #expect(error === mapped)
        } else { Issue.record("Expected required failures in latest") }
    }

    @Test("A new operation error updates the shared failure while an input is blocked")
    func failureMappingAfterOperationError() async throws {
        let bucket = Bucket(IndexedKey<Int, Favorite>("composition.shared-error", indexedBy: \.id)) { LocalSource { [Favorite(id: 1)] } }
        try await bucket.load()
        let required = Source<Int>(.result(.failure(NumberedError(number: 1))))
        let composition = Composition {
            Input(required)
            Input(bucket)
        } transform: { $0 + $1.count }
        var updates = composition.makeAsyncIterator()
        #expect(isFailure(await updates.next()))

        await #expect(throws: ContinuumError.self) { try await bucket.load(id: 1, using: .remote) }
        guard case .result(.failure(let published as CompositionError))? = await updates.next() else {
            Issue.record("Expected the updated aggregate failure")
            return
        }
        let current = try #require(composition.error as? CompositionError)
        for error in [published, current] {
            #expect(error.errors.count == 2)
            #expect((error.errors.first as? NumberedError)?.number == 1)
            #expect(error.errors.last is ContinuumError)
        }
    }

    @Test("The default aggregate flattens nested input errors in declaration order")
    func nestedFailureMapping() {
        let first = Source<Int>(.result(.failure(NumberedError(number: 1))))
        let second = Source<Int>(.result(.failure(NumberedError(number: 2))))
        let third = Source<Int>(.result(.failure(NumberedError(number: 3))))
        let child = Composition {
            Input(first)
            Input(second).optional()
        } mapFailures: { _ in TestError.failed } transform: { $0 + ($1 ?? 0) }
        let parent = Composition {
            Input(child).optional()
            Input(third).optional()
        } transform: { ($0 ?? 0) + ($1 ?? 0) }

        #expect(success(parent.latest) == 0)
        let aggregate = parent.error as? CompositionError
        #expect(aggregate?.errors.compactMap { ($0 as? NumberedError)?.number } == [1, 2, 3])
    }

    @Test("An externally failed entry load updates observable composition error without transforming")
    func observesOperationErrors() async throws {
        let bucket = Bucket(IndexedKey<Int, Favorite>("composition.entry-error", indexedBy: \.id)) { LocalSource { [Favorite(id: 1)] } }
        try await bucket.load()
        var transforms = 0
        let composition = Composition { Input(bucket) } transform: { favorites in
            transforms += 1
            return favorites.count
        }
        let (changes, changed) = AsyncStream<Void>.makeStream()
        var observed = changes.makeAsyncIterator()
        withObservationTracking { _ = composition.error } onChange: { changed.yield(()) }

        await #expect(throws: ContinuumError.self) { try await bucket.load(id: 1, using: .remote) }
        _ = await observed.next()
        #expect((composition.error as? CompositionError)?.errors.first is ContinuumError)
        #expect(success(composition.latest) == 1)
        #expect(transforms == 1)

        var updates = composition.makeAsyncIterator()
        #expect(success(await updates.next()) == 1)
        try await bucket.reset()
        #expect(isReset(await updates.next()))
        #expect(composition.error == nil)
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

private actor LocalLoadCalls {
    private(set) var keys: Set<String> = []

    func record(_ key: String) { keys.insert(key) }
}

@MainActor @Observable
private final class Preferences { var language = "en" }

@MainActor @Observable
private final class RepositoryState<Value: Sendable> {
    enum Phase {
        case idle
        case loaded(Value)
        case failed(any Error)
    }

    var phase: Phase = .idle

    var update: Update<Value> {
        switch phase {
        case .idle: .reset
        case .loaded(let value): .result(.success(value))
        case .failed(let error): .result(.failure(error))
        }
    }
}

@MainActor @Observable
private final class Source<T: Sendable>: UpdateSource {
    var update: Update<T>
    var latest: Update<T> { update }
    init(_ update: Update<T> = .reset) { self.update = update }
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
private nonisolated final class MappedError: Error {
    let numbers: [Int]
    init(numbers: [Int]) { self.numbers = numbers }
}


@concurrent private func firstUpdate<Source: AsyncSequence & Sendable>(_ source: Source) async throws -> Update<Int>?
where Source.Element == Update<Int> {
    for try await update in source { return update }
    return nil
}

@MainActor
private func observesSourceChange<Source: UpdateSource>(
    _ source: Source,
    change: @MainActor () async throws -> Void
) async rethrows -> Bool {
    let (events, changed) = AsyncStream<Void>.makeStream()
    withObservationTracking {
        _ = source.latest
    } onChange: {
        changed.yield(())
        changed.finish()
    }
    var iterator = events.makeAsyncIterator()
    try await change()
    return await iterator.next() != nil
}
