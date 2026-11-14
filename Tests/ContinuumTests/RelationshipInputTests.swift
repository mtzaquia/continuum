import Continuum
import Observation
import Testing

@Suite("Relationship inputs", .timeLimit(.minutes(1)))
struct RelationshipInputTests {
    @Test("An indexed partition can supply an optional relationship from memory without an entry loader")
    func resolvesFromLocalPartition() async throws {
        let posts = Bucket(Key<[Post]>("relationship.local.posts")) {
            LocalSource { [Post(authorID: 1)] }
            RemoteSource { throw LookupFailure.offline }
        }
        let authors = PartitionedBucket(IndexedKey<Int, Author>("relationship.local.authors"), partitionedBy: Int.self) { _ in
            LocalSource { [Author(id: 1, name: "Local")] }
        }[0]
        try await posts.load()
        try await authors.load()

        let includeAuthors = true
        let screen = Composition {
            if includeAuthors {
                Input(posts, loads: false).resolving(\.authorID, from: authors)
            }
        } transform: { (resolved: ResolvedSnapshot<Post, Int, Author>?) in
            resolved?.related[1]?.name
        }

        await screen.load(using: .remote)
        #expect(snapshot(screen.latest) == "Local")
        #expect(screen.error == nil)
        #expect(posts.error == nil)
    }

    @Test("A missing related entry requires an entry loader")
    func missingEntryLoader() async throws {
        let posts = Bucket(Key<[Post]>("relationship.missing.posts")) {
            LocalSource { [Post(authorID: 2)] }
        }
        let authors = Bucket(IndexedKey<Int, Author>("relationship.missing.authors")) {
            LocalSource { [Author(id: 1, name: "Local")] }
        }
        try await authors.load()
        let screen = Composition {
            Input(posts).resolving(\.authorID, from: authors)
        } transform: { $0 }

        await screen.load()
        let error = try #require(screen.error as? CompositionError)
        guard case .missingEntrySource? = error.errors.first as? ContinuumError else {
            Issue.record("Expected a missing entry loader error")
            return
        }
        guard case .result(.failure) = screen.latest else {
            Issue.record("Expected the unresolved initial pair to fail")
            return
        }
        #expect(authors.values.map(\.id) == [1])
    }

    @Test("Remote root loads reuse local entries and fetch distinct missing IDs in parallel")
    func resolvesMissingEntries() async throws {
        let lookups = EntryLookups()
        let authors = makeAuthors(lookups)
        try await authors.load()
        let roots = RootLoads()
        let posts = Bucket(Key<[Post]>("resolved.posts")) {
            RemoteSource {
                await roots.record()
                return [Post(authorID: 1), Post(authorID: 2), Post(authorID: 3), Post(authorID: 2)]
            }
        }
        let child = Composition {
            Input(posts).resolving(\.authorID, from: authors)
        } transform: { $0 }
        let screen = Composition {
            Input(child)
            Input(posts)
        } transform: { $0.related.count + $1.count }

        let load = Task { await screen.load(using: .remote) }
        await lookups.waitForCalls(2)
        let calls = await lookups.calls
        let rootCalls = await roots.count
        #expect(calls.sorted() == [2, 3])
        #expect(rootCalls == 1)
        #expect(child.isLoading)
        #expect(snapshot(child.latest) == nil)
        await lookups.finish(2, with: .success("Two"))
        await lookups.finish(3, with: .success("Three"))
        await load.value

        let result = try #require(snapshot(child.latest))
        #expect(result.root.map(\.authorID) == [1, 2, 3, 2])
        #expect(result.related[1]?.name == "Local")
        #expect(result.related[2]?.name == "Two")
        #expect(result.related[3]?.name == "Three")
        #expect(authors.values.map(\.id) == [1])
        #expect(snapshot(screen.latest) == 7)

        try await authors.mutate { mutation in
            try await mutation.update { _ in [Author(id: 1, name: "Edited")] }
        }
        await waitUntil { snapshot(child.latest)?.related[1]?.name == "Edited" }
        let callsAfterEdit = await lookups.calls
        #expect(callsAfterEdit.sorted() == [2, 3])
    }

    @Test("A failed page lookup retains the complete feed and retries without moving the cursor")
    func retriesCurrentPage() async throws {
        let lookups = EntryLookups()
        let pages = RootLoads()
        let authors = makeAuthors(lookups)
        try await authors.load()
        let posts = Bucket(Key<[Post]>("resolved.pages")) {
            RemoteSource(
                { Page(values: [Post(authorID: 1)], next: 1) },
                nextPage: NextPage { (_: Int) in
                    await pages.record()
                    return Page(values: [Post(authorID: 2), Post(authorID: 3)], next: nil)
                }
            )
        }
        let screen = Composition {
            Input(posts).resolving(\.authorID, from: authors)
        } transform: { $0.root.count }
        await screen.load()
        #expect(snapshot(screen.latest) == 1)

        #expect(try await posts.loadNext().count == 3)
        await lookups.waitForCalls(2)
        await lookups.finish(2, with: .success("Two"))
        await lookups.finish(3, with: .failure(.offline))
        await waitUntil { screen.error != nil }
        #expect(snapshot(screen.latest) == 1)
        #expect(!posts.hasNextPage)

        try await authors.mutate { mutation in
            try await mutation.update { _ in [Author(id: 1, name: "Edited")] }
        }
        let retry = Task { await screen.load() }
        await lookups.waitForCalls(3)
        #expect(screen.error != nil)
        #expect(screen.isLoading)
        let calls = await lookups.calls
        #expect(calls.sorted() == [2, 3, 3])
        await lookups.finish(3, with: .success("Three"))
        await retry.value
        #expect(snapshot(screen.latest) == 3)
        let pageCalls = await pages.count
        #expect(pageCalls == 1)
        #expect(screen.error == nil)
    }

    @Test("A composition load waits for the current page and keeps lookups for IDs already in flight")
    func pageArrivesDuringLoad() async throws {
        let lookups = EntryLookups()
        let posts = Bucket(Key<[Post]>("resolved.overlap")) {
            RemoteSource(
                { Page(values: [Post(authorID: 2)], next: 1) },
                nextPage: NextPage { (_: Int) in Page(values: [Post(authorID: 3)], next: nil) }
            )
        }
        let authors = makeAuthors(lookups)
        let screen = Composition {
            Input(posts).resolving(\.authorID, from: authors)
        } transform: { $0.root.count }
        let loading = Task { await screen.load() }

        await lookups.waitForCalls(1)
        try await posts.loadNext()
        await lookups.waitForCalls(2)
        await lookups.finish(2, with: .success("Two"))
        #expect(screen.isLoading)
        #expect(snapshot(screen.latest) == nil)
        await lookups.finish(3, with: .success("Three"))
        await loading.value

        #expect(snapshot(screen.latest) == 2)
        #expect(await lookups.calls == [2, 3])
        #expect(!screen.isLoading)
    }

    @Test("Resetting the root fences late relationship results")
    func resetDuringLookup() async throws {
        let lookups = EntryLookups()
        let posts = Bucket(Key<[Post]>("resolved.reset")) { LocalSource { [Post(authorID: 2)] } }
        let authors = makeAuthors(lookups)
        let screen = Composition {
            Input(posts).resolving(\.authorID, from: authors)
        } transform: { $0 }
        let loading = Task { await screen.load() }
        await lookups.waitForCalls(1)
        try await posts.reset()
        await lookups.finish(2, with: .success("Obsolete"))
        await loading.value
        if case .reset = screen.latest {} else { Issue.record("Expected reset after the obsolete lookup returned") }
        #expect(screen.error == nil)
        #expect(!screen.isLoading)
    }

    @Test("Resetting an unloaded related bucket invalidates entries fetched by ID")
    func resetsUnloadedRelatedBucket() async throws {
        let lookups = EntryLookups()
        let posts = Bucket(Key<[Post]>("relationship.reset.posts")) { LocalSource { [Post(authorID: 2)] } }
        let authors = makeAuthors(lookups)
        let screen = Composition {
            Input(posts).resolving(\.authorID, from: authors)
        } transform: { resolved in
            resolved.related[2]?.name ?? "Missing author"
        }

        let initialLoad = Task { await screen.load() }
        await lookups.waitForCalls(1)
        await lookups.finish(2, with: .success("Before reset"))
        await initialLoad.value
        #expect(!authors.isLoaded)
        var updates = screen.makeAsyncIterator()
        let initialUpdate = try #require(await updates.next())
        #expect(snapshot(initialUpdate) == "Before reset")

        try await authors.reset()
        guard case .reset? = await updates.next() else {
            Issue.record("Expected reset after discarding the fetched entry")
            return
        }
        #expect(await lookups.calls == [2])

        let reload = Task { await screen.load() }
        await lookups.waitForCalls(2)
        await lookups.finish(2, with: .success("After reset"))
        await reload.value
        #expect(snapshot(screen.latest) == "After reset")
    }

    private func makeAuthors(_ lookups: EntryLookups) -> IdentifiedBucket<Author> {
        Bucket(IndexedKey("resolved.authors")) {
            LocalSource { [Author(id: 1, name: "Local")] }
            RemoteSource({ [Author]() }, loadEntry: { try await lookups.load($0) })
        }
    }
}

private nonisolated struct Post: Sendable { let authorID: Int }
private nonisolated struct Author: Identifiable, Sendable { let id: Int; let name: String }
private enum LookupFailure: Error { case offline }

private actor RootLoads {
    private(set) var count = 0
    func record() { count += 1 }
}

private actor EntryLookups {
    private(set) var calls: [Int] = []
    private var pending: [Int: CheckedContinuation<Author, any Error>] = [:]
    private var waiting: [(Int, CheckedContinuation<Void, Never>)] = []

    func load(_ id: Int) async throws -> Author {
        try await withCheckedThrowingContinuation { continuation in
            pending[id] = continuation
            calls.append(id)
            waiting.removeAll { count, waiter in
                guard calls.count >= count else { return false }
                waiter.resume()
                return true
            }
        }
    }

    func waitForCalls(_ count: Int) async {
        guard calls.count < count else { return }
        await withCheckedContinuation { waiting.append((count, $0)) }
    }

    func finish(_ id: Int, with result: Result<String, LookupFailure>) {
        pending.removeValue(forKey: id)?.resume(with: result.map { Author(id: id, name: $0) })
    }
}

private func snapshot<Value>(_ update: Update<Value>) -> Value? {
    guard case .result(.success(let value)) = update else { return nil }
    return value
}

private func waitUntil(_ predicate: @escaping @MainActor () -> Bool) async {
    while !Task.isCancelled {
        let (changes, signal) = AsyncStream<Void>.makeStream()
        if withObservationTracking(predicate, onChange: { signal.yield(()) }) { return }
        for await _ in changes { break }
    }
}
