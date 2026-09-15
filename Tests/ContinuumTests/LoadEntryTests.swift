import Continuum
import Observation
import Testing

@Suite("Entry loading", .timeLimit(.minutes(1)))
struct LoadEntryTests {
    @Test("Policies replace existing entries in place and persist only collection members")
    func membershipAndPolicies() async throws {
        let service = Service()
        let bucket = makeBucket(service)
        try await bucket.load()
        #expect(try await bucket.load(id: 1) == Entry(1, "old"))
        #expect(await service.calls.isEmpty)
        #expect(try await bucket.load(id: 1, using: .cachedThenRemote) == Entry(1, "new"))
        #expect(bucket.values == [Entry(2, "old"), Entry(1, "new")])
        #expect(await service.writes.last! == bucket.values)
        let writes = await service.writes.count
        #expect(try await bucket.load(id: 3) == Entry(3, "new"))
        #expect(try await bucket.load(id: 3) == Entry(3, "new"))
        #expect(bucket[3] == nil)
        #expect(await service.writes.count == writes)
        #expect(await service.calls == [1, 3, 3])
        #expect(bucket.isLoaded)
    }

    @Test("Fetching an absent entry does not establish a list or persist anything")
    func absentList() async throws {
        let service = Service()
        let bucket = makeBucket(service)
        #expect(try await bucket.load(id: 1) == Entry(1, "new"))
        #expect(!bucket.isLoaded)
        #expect(bucket.values.isEmpty)
        #expect(isReset(bucket.latest))
        #expect(await service.writes.isEmpty)
    }

    @Test("Missing loaders and incorrect identities throw without changing the collection")
    func invalidConfigurationAndIdentity() async throws {
        let bucket = Bucket(IndexedKey<Int, Entry>("entries")) {
            RemoteSource { [Entry(1, "old")] }
        }
        try await bucket.load()
        #expect(try await bucket.load(id: 1) == Entry(1, "old"))
        await #expect(throws: ContinuumError.self) { try await bucket.load(id: 2) }
        let invalid = Bucket(IndexedKey<Int, Entry>("invalid")) {
            RemoteSource {
                Load { [Entry(1, "old")] }
                LoadEntry { (_: Int) in Entry(2, "wrong") }
            }
        }
        try await invalid.load()
        await #expect(throws: ContinuumError.self) { try await invalid.load(id: 1, using: .remote) }
        #expect(invalid.values == [Entry(1, "old")])
        #expect(invalid.error == nil)
    }

    @Test("Fetch and persistence errors preserve the list and its successful outcome")
    func failures() async throws {
        let service = Service()
        let bucket = makeBucket(service)
        try await bucket.load()
        let old = bucket.values
        await service.failFetch(true)
        await #expect(throws: Failure.self) { try await bucket.load(id: 1, using: .remote) }
        await service.failFetch(false)
        await service.failWrite(true)
        await #expect(throws: Failure.self) { try await bucket.load(id: 1, using: .remote) }
        #expect(bucket.values == old)
        #expect(bucket.error == nil)
        #expect(value(bucket.latest) == old)
        #expect(await service.writes.last! == old)
    }

    @Test("Duplicate ID requests share work; different IDs reconcile without losing changes")
    func coalescing() async throws {
        let gate = Gate()
        let bucket = gatedBucket(gate)
        try await bucket.load()
        let first = Task { try await bucket.load(id: 1, using: .cachedThenRemote) }
        await until { gate.calls.count == 1 }
        let duplicate = Task { try await bucket.load(id: 1, using: .cachedThenRemote) }
        let second = Task { try await bucket.load(id: 2, using: .cachedThenRemote) }
        await until { gate.calls.count == 2 }
        #expect(bucket.values == [Entry(1, "old"), Entry(2, "old")])
        gate.finish(2)
        gate.finish(1)
        #expect(try await first.value == Entry(1, "new"))
        #expect(try await duplicate.value == Entry(1, "new"))
        #expect(try await second.value == Entry(2, "new"))
        #expect(gate.calls.sorted() == [1, 2])
        #expect(bucket.values == [Entry(1, "new"), Entry(2, "new")])
    }

    @Test("Remote entry loads supersede same-ID work without cancelling other IDs")
    func remoteReplacement() async throws {
        let gate = Gate()
        let bucket = gatedBucket(gate)
        try await bucket.load()
        let old = Task { try await bucket.load(id: 1, using: .cachedThenRemote) }
        await until { gate.calls.count == 1 }
        let oldContinuation = gate.pending.removeValue(forKey: 1)!
        let other = Task { try await bucket.load(id: 2, using: .remote) }
        await until { gate.calls.count == 2 }
        let replacement = Task { try await bucket.load(id: 1, using: .remote) }
        await until { gate.calls.count == 3 }
        gate.finish(1)
        #expect(try await replacement.value == Entry(1, "new"))
        oldContinuation.resume(returning: Entry(1, "obsolete"))
        await #expect(throws: CancellationError.self) { try await old.value }
        gate.finish(2)
        #expect(try await other.value == Entry(2, "new"))
        #expect(bucket.values == [Entry(1, "new"), Entry(2, "new")])
    }

    @Test("Remote replacement waits for cancelled entry persistence before committing")
    func replacementDuringPersistence() async throws {
        let writes = WriteGate()
        let gate = Gate()
        let bucket = Bucket(IndexedKey<Int, Entry>("replacing-write")) {
            LocalSource { Optional<[Entry]>.none } persist: { try await writes.persist($0) }
            RemoteSource {
                Load { [Entry(1, "old")] }
                LoadEntry { try await gate.fetch($0) }
            }
        }
        try await bucket.load()
        writes.block = true
        let old = Task { try await bucket.load(id: 1, using: .remote) }
        await until { gate.calls.count == 1 }
        gate.finish(1)
        await until { writes.pending != nil }
        let replacement = Task { try await bucket.load(id: 1, using: .remote) }
        await until { writes.cancelled }
        writes.finish()
        await until { gate.calls.count == 2 }
        gate.pending.removeValue(forKey: 1)?.resume(returning: Entry(1, "fresh"))
        #expect(try await replacement.value == Entry(1, "fresh"))
        await #expect(throws: CancellationError.self) { try await old.value }
        #expect(bucket[1] == Entry(1, "fresh"))
        #expect(writes.snapshots.last! == [Entry(1, "fresh")])
    }

    @Test("Removing a collection member retains its latest observed relationship value")
    func retainsLatestObserved() async throws {
        let service = Service()
        let bucket = makeBucket(service)
        let composition = Composition {
            Input { [Post(authorID: 1)] }.resolving(\.authorID, from: bucket)
        } transform: { _, entries in entries }
        await composition.load()
        #expect(value(composition.latest)?[1] == Entry(1, "new"))
        try await bucket.store(Entry(1, "edited"))
        await until { value(composition.latest)?[1] == Entry(1, "edited") }
        var iterator = composition.makeAsyncIterator()
        _ = await iterator.next()
        try await bucket.remove(1)
        #expect(value(try #require(await iterator.next()))?[1] == Entry(1, "edited"))
        await composition.load(using: .cached)
        #expect(value(composition.latest)?[1] == Entry(1, "new"))
    }

    @Test("Collection updates leave entry errors intact until a successful load")
    func explicitErrorRecovery() async throws {
        let service = Service()
        let bucket = makeBucket(service)
        try await bucket.load()
        let composition = Composition {
            Input { [Post(authorID: 1)] }.resolving(\.authorID, from: bucket)
        } transform: { _, entries in entries }
        await composition.load()
        await service.failFetch(true)
        await composition.load(using: .remote)
        var iterator = composition.makeAsyncIterator()
        if case .result(.failure)? = await iterator.next() {} else { Issue.record("Expected failure") }
        try await bucket.store(Entry(2, "Bob"))
        if case .result(.failure)? = await iterator.next() {} else { Issue.record("Bob must not clear Alice's error") }
        try await bucket.store(Entry(1, "Alice"))
        if case .result(.failure)? = await iterator.next() {} else { Issue.record("Stores must not clear load errors") }
        await composition.load(using: .cached)
        #expect(value(composition.latest)?[1] == Entry(1, "Alice"))
    }

    @Test("Reset, removal, and a replacement load reject cancellation-uncooperative entry fetches", arguments: [0, 1, 2])
    func supersession(_ operation: Int) async throws {
        let gate = Gate()
        let bucket = gatedBucket(gate)
        try await bucket.load()
        let pending = Task { try await bucket.load(id: 1, using: .remote) }
        await until { gate.calls.count == 1 }
        switch operation {
        case 0: try await bucket.reset()
        case 1: try await bucket.remove(1)
        default: try await bucket.load(using: .remote)
        }
        let expected = bucket.values
        gate.finish(1)
        await #expect(throws: CancellationError.self) { try await pending.value }
        #expect(bucket.values == expected)
    }

    @Test("Entry refresh preserves pagination and combines with Store and Remove capabilities")
    func pagination() async throws {
        let bucket = Bucket(IndexedKey<Int, Entry>("paged")) {
            RemoteSource {
                Load { Page(value: [Entry(1, "old")], next: 2) }
                NextPage { cursor in Page(value: [Entry(cursor, "page")], next: Optional<Int>.none) }
                LoadEntry { id in Entry(id, "new") }
                Store { (entry: Entry) in entry }
                Remove { (_: Int) in }
            }
        }
        try await bucket.load()
        try await bucket.load(id: 1, using: .remote)
        #expect(bucket.hasNextPage)
        try await bucket.loadNext()
        #expect(bucket.values == [Entry(1, "new"), Entry(2, "page")])
        #expect(!bucket.hasNextPage)
    }

    @Test("Selected indexed partitions support entry loading and relationships")
    func partition() async throws {
        let bucket = Bucket(IndexedKey<Int, Entry>("partitioned"), partitionedBy: String.self) { query in
            RemoteSource {
                Load { [Entry(1, query)] }
                LoadEntry { id in Entry(id, query) }
            }
        }
        let selected = bucket["query"]
        #expect(try await selected.load(id: 1) == Entry(1, "query"))
        let composition = Composition {
            Input { [Post(authorID: 1)] }.resolving(\.authorID, from: selected)
        } transform: { _, entries in entries }
        await composition.load()
        #expect(value(composition.latest)?[1] == Entry(1, "query"))
        #expect(!selected.isLoaded)
    }

    @Test("Relationships combine list entries and fetched values, observe edits, and invalidate on reset")
    func relationships() async throws {
        let service = Service()
        let bucket = makeBucket(service)
        try await bucket.load()
        let composition = Composition {
            Input { [Post(authorID: 1), Post(authorID: 3)] }.resolving(\.authorID, from: bucket)
        } transform: { _, entries in entries }
        await composition.load()
        #expect(value(composition.latest) == [1: Entry(1, "old"), 3: Entry(3, "new")])
        #expect(bucket[3] == nil)
        try await bucket.store(Entry(1, "edited"))
        await until { value(composition.latest)?[1] == Entry(1, "edited") }
        #expect(value(composition.latest)?[3] == Entry(3, "new"))
        try await bucket.reset()
        await until { isReset(composition.latest) }
        await composition.load()
        #expect(value(composition.latest)?.count == 2)
        #expect(!bucket.isLoaded)
        try await bucket.reset()
        await until { isReset(composition.latest) }
    }

    @Test("Relationships can use cached entries without LoadEntry but fail when a fetch needs it")
    func relationshipWithoutLoader() async throws {
        let bucket = Bucket(IndexedKey<Int, Entry>("cached-only"))
        try await bucket.store(Entry(1, "cached"))
        let composition = Composition {
            Input { [Post(authorID: 1)] }.resolving(\.authorID, from: bucket)
        } transform: { _, entries in entries }
        await composition.load()
        #expect(value(composition.latest)?[1] == Entry(1, "cached"))
        await composition.load(using: .remote)
        if case .result(.failure(let error)) = composition.latest {
            guard case ContinuumError.missingEntrySource = error else {
                Issue.record("Expected ContinuumError.missingEntrySource, got \(error)")
                return
            }
        } else { Issue.record("Expected required relationship failure") }
        #expect(bucket[1] == Entry(1, "cached"))
    }

    @Test("Reset waits for an entry write and prevents late publication or persistence resurrection")
    func resetDuringPersistence() async throws {
        let writes = WriteGate()
        let bucket = Bucket(IndexedKey<Int, Entry>("persisting")) {
            LocalSource { Optional<[Entry]>.none } persist: { try await writes.persist($0) }
            RemoteSource {
                Load { [Entry(1, "old")] }
                LoadEntry { id in Entry(id, "new") }
            }
        }
        try await bucket.load()
        writes.block = true
        let load = Task { try await bucket.load(id: 1, using: .remote) }
        await until { writes.pending != nil }
        #expect(bucket[1] == Entry(1, "old"))
        let reset = Task { try await bucket.reset() }
        await until { !bucket.isLoaded }
        writes.finish()
        try await reset.value
        await #expect(throws: CancellationError.self) { try await load.value }
        #expect(bucket.values.isEmpty)
        #expect(writes.snapshots.last! == nil)
    }

    @Test("Entry reconciliation cancels an older page and preserves its cursor for retry")
    func pendingPage() async throws {
        let gate = Gate()
        let bucket = Bucket(IndexedKey<Int, Entry>("pending-page")) {
            RemoteSource {
                Load { Page(value: [Entry(1, "old")], next: 2) }
                NextPage { cursor in
                    Page(value: [try await gate.fetch(cursor)], next: Optional<Int>.none)
                }
                LoadEntry { id in Entry(id, "new") }
            }
        }
        try await bucket.load()
        let page = Task { try await bucket.loadNext() }
        await until { gate.calls.count == 1 }
        try await bucket.load(id: 1, using: .remote)
        #expect(bucket.hasNextPage)
        gate.finish(2)
        await #expect(throws: CancellationError.self) { try await page.value }
        #expect(bucket.values == [Entry(1, "new")])
        let retry = Task { try await bucket.loadNext() }
        await until { gate.calls.count == 2 }
        gate.finish(2)
        try await retry.value
        #expect(bucket.values == [Entry(1, "new"), Entry(2, "new")])
    }

    @Test("Cancelling one entry waiter does not cancel shared work")
    func cancelledWaiter() async throws {
        let gate = Gate()
        let bucket = gatedBucket(gate)
        try await bucket.load()
        let cancelled = Task { try await bucket.load(id: 1, using: .remote) }
        await until { gate.calls.count == 1 }
        cancelled.cancel()
        #expect(try await bucket.load(id: 1) == Entry(1, "old"))
        gate.finish(1)
        await #expect(throws: CancellationError.self) { try await cancelled.value }
        #expect(bucket[1] == Entry(1, "new"))
        #expect(gate.calls == [1])
    }

    @Test("An entry failure fails the relationship without failing the bucket and is retryable")
    func relationshipFailure() async throws {
        let service = Service()
        let bucket = makeBucket(service)
        try await bucket.load()
        let composition = Composition {
            Input { [Post(authorID: 1)] }.resolving(\.authorID, from: bucket)
        } transform: { _, entries in entries }
        await composition.load()
        await service.failFetch(true)
        await composition.load(using: .remote)
        if case .result(.failure) = composition.latest {} else { Issue.record("Expected relationship failure") }
        #expect(bucket.error == nil)
        await service.failFetch(false)
        await composition.load(using: .remote)
        #expect(value(composition.latest)?[1] == Entry(1, "new"))
    }
}

private nonisolated struct Entry: Identifiable, Sendable, Equatable {
    let id: Int
    let name: String
    init(_ id: Int, _ name: String) { self.id = id; self.name = name }
}
private nonisolated struct Post: Sendable { let authorID: Int }
private enum Failure: Error { case failed }
private actor Service {
    var calls: [Int] = []
    var writes: [[Entry]?] = []
    var fetchFails = false
    var writeFails = false
    func fetch(_ id: Int) throws -> Entry {
        calls.append(id)
        if fetchFails { throw Failure.failed }
        return Entry(id, "new")
    }
    func persist(_ entries: [Entry]?) throws {
        writes.append(entries)
        if writeFails { writeFails = false; throw Failure.failed }
    }
    func failFetch(_ enabled: Bool) { fetchFails = enabled }
    func failWrite(_ enabled: Bool) { writeFails = enabled }
}
private func makeBucket(_ service: Service) -> IndexedBucket<Int, Entry> {
    Bucket(IndexedKey<Int, Entry>("entries")) {
        LocalSource { Optional<[Entry]>.none } persist: { try await service.persist($0) }
        RemoteSource {
            Load { [Entry(2, "old"), Entry(1, "old")] }
            LoadEntry { try await service.fetch($0) }
        }
    }
}
@Observable private final class Gate {
    var calls: [Int] = []
    @ObservationIgnored var pending: [Int: CheckedContinuation<Entry, any Error>] = [:]
    func fetch(_ id: Int) async throws -> Entry {
        try await withCheckedThrowingContinuation { continuation in
            pending[id] = continuation
            calls.append(id)
        }
    }
    func finish(_ id: Int) { pending.removeValue(forKey: id)?.resume(returning: Entry(id, "new")) }
}
private func gatedBucket(_ gate: Gate) -> IndexedBucket<Int, Entry> {
    Bucket(IndexedKey<Int, Entry>("gated")) {
        RemoteSource {
            Load { [Entry(1, "old"), Entry(2, "old")] }
            LoadEntry { try await gate.fetch($0) }
        }
    }
}
private func until(_ condition: @escaping @MainActor () -> Bool) async {
    while !Task.isCancelled {
        let (stream, continuation) = AsyncStream<Void>.makeStream()
        let done = withObservationTracking { condition() } onChange: {
            continuation.yield(())
            continuation.finish()
        }
        if done { continuation.finish(); return }
        var iterator = stream.makeAsyncIterator()
        _ = await iterator.next()
    }
}
private func value<T>(_ update: Update<T>) -> T? {
    if case .result(.success(let value)) = update { return value }
    return nil
}
private func isReset<T>(_ update: Update<T>) -> Bool {
    if case .reset = update { return true }
    return false
}

@Observable private final class WriteGate {
    var block = false
    var cancelled = false
    var snapshots: [[Entry]?] = []
    var pending: CheckedContinuation<Void, Never>?
    func persist(_ snapshot: [Entry]?) async throws {
        snapshots.append(snapshot)
        if block {
            block = false
            await withTaskCancellationHandler {
                await withCheckedContinuation { pending = $0 }
            } onCancel: {
                Task { @MainActor in self.cancelled = true }
            }
        }
    }
    func finish() { pending?.resume(); pending = nil }
}
