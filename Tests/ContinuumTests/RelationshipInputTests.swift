import Continuum
import Observation
import Testing

@Suite("Relationship inputs", .timeLimit(.minutes(1)))
struct RelationshipInputTests {
    @Test("Initial root loads before native relationships, with duplicate keys coalesced")
    func initialNative() async throws {
        let calls = Calls()
        let posts = Bucket(Key<[Post]>("posts")) {
            RemoteSource {
                await calls.record(0)
                return [Post(id: 1, authorID: 7), Post(id: 2, authorID: 7), Post(id: 3, authorID: 8)]
            }
        }
        let authors = Bucket(Key<String>("authors"), partitionedBy: Int.self) { id in
            RemoteSource { await calls.record(id); return "author-\(id)" }
        }
        let composition = Composition {
            Input(posts) { try await posts.load(using: $0) }.resolving(\.authorID, from: authors)
        } transform: { posts, authors in Pair(posts: posts, authors: authors) }
        #expect(isReset(composition.latest))
        #expect(await calls.values.isEmpty)
        await composition.load()
        let pair = try #require(value(composition.latest))
        #expect(pair.posts.count == 3)
        #expect(pair.authors == [7: "author-7", 8: "author-8"])
        #expect(await calls.values.first == 0)
        #expect(await calls.values.sorted() == [0, 7, 8])
    }

    @Test("One-shot lookup deduplicates concurrent work and waits for every relationship")
    func barrier() async {
        let root = Root()
        let gate = Gate()
        let composition = Composition {
            Input { root.posts } load: { _ in root.posts = [Post(id: 1, authorID: 1), Post(id: 2, authorID: 2), Post(id: 3, authorID: 1)] }
                .resolving(\.authorID) { id, policy in try await gate.resolve(id, policy) }
        } transform: { posts, authors in Pair(posts: posts, authors: authors) }
        #expect(value(composition.latest)?.posts.isEmpty == true)
        let load = Task { await composition.load() }
        await gate.waitForCalls(2)
        #expect(Set(gate.ids) == [1, 2])
        #expect(value(composition.latest)?.posts.isEmpty == true)
        gate.finish(id: 1, value: "one")
        #expect(value(composition.latest)?.posts.isEmpty == true)
        gate.finish(id: 2, value: "two")
        await load.value
        #expect(value(composition.latest)?.authors == [1: "one", 2: "two"])
    }

    @Test("Observation retains continuing keys and discards removed keys")
    func retention() async {
        let root = Root([Post(id: 1, authorID: 1)])
        var ids: [Int] = []
        let composition = Composition {
            Input { root.posts }.resolving(\.authorID) { id, _ in ids.append(id); return "\(id)" }
        } transform: { posts, authors in Pair(posts: posts, authors: authors) }
        await until { value(composition.latest)?.authors == [1: "1"] }
        root.posts = [Post(id: 2, authorID: 1), Post(id: 3, authorID: 2)]
        await until { value(composition.latest)?.posts == root.posts }
        #expect(ids == [1, 2])
        root.posts = [Post(id: 4, authorID: 2)]
        await until { value(composition.latest)?.posts == root.posts }
        #expect(value(composition.latest)?.authors == [2: "2"])
        root.posts = [Post(id: 5, authorID: 1)]
        await until { value(composition.latest)?.posts == root.posts }
        #expect(ids == [1, 2, 1])
    }

    @Test("Native stores, removal, reset, failure, and recovery remain observable")
    func nativeChanges() async throws {
        let fail = FailureSwitch()
        let authors = Bucket(Key<String>("authors"), partitionedBy: Int.self) { _ in
            RemoteSource { if await fail.enabled { throw LookupError.failed }; return "remote" }
        }
        let composition = Composition {
            Input { [Post(id: 1, authorID: 7)] }.resolving(\.authorID, from: authors)
        } transform: { posts, authors in Pair(posts: posts, authors: authors) }
        await until { value(composition.latest)?.authors[7] == "remote" }
        try await authors[7].store("stored")
        await until { value(composition.latest)?.authors[7] == "stored" }
        try await authors[7].remove()
        await until { isReset(composition.latest) }
        try await authors[7].store("recovered")
        await until { value(composition.latest)?.authors[7] == "recovered" }
        try await authors[7].reset()
        await until { isReset(composition.latest) }
        await fail.set(true)
        await composition.load(using: .remote)
        #expect(isFailure(composition.latest))
        await fail.set(false)
        try await authors[7].load(using: .remote)
        await until { value(composition.latest)?.authors[7] == "remote" }
    }

    @Test("Collection-valued partitions preserve their complete snapshot type")
    func collectionPartition() async {
        let groups = Bucket(Key<[String]>("groups"), partitionedBy: Int.self) { id in
            RemoteSource { ["\(id)", "extra"] }
        }
        let composition = Composition {
            Input { [Post(id: 1, authorID: 7)] }.resolving(\.authorID, from: groups)
        } transform: { (_: [Post], groups: [Int: [String]]) in groups }
        await composition.load()
        #expect(value(composition.latest) == [7: ["7", "extra"]])
    }

    @Test("Closure failures retry and each policy reaches root and resolver unchanged", arguments: [LoadPolicy.cached, .cachedThenRemote, .remote])
    func retryAndPolicy(policy: LoadPolicy) async {
        let source = Source<[Post]>()
        var rootPolicies: [String] = []
        var resolverPolicies: [String] = []
        var fail = true
        let composition = Composition {
            Input(source) { policy in
                rootPolicies.append(name(policy))
                source.latest = .result(.success([Post(id: 1, authorID: 1)]))
            }.resolving(\.authorID) { _, policy in
                resolverPolicies.append(name(policy))
                if fail { throw LookupError.failed }
                return "resolved"
            }
        } transform: { posts, authors in Pair(posts: posts, authors: authors) }
        await composition.load(using: policy)
        #expect(isFailure(composition.latest))
        fail = false
        await composition.load(using: policy)
        #expect(value(composition.latest)?.authors == [1: "resolved"])
        #expect(rootPolicies == [name(policy), name(policy)])
        #expect(resolverPolicies == rootPolicies)
    }

    @Test("New root snapshots and refreshes supersede cancellation-uncooperative work")
    func supersession() async {
        let source = Source<[Post]>()
        let gate = Gate()
        var rootID = 1
        let composition = Composition {
            Input(source) { _ in source.latest = .result(.success([Post(id: rootID, authorID: rootID)])) }
                .resolving(\.authorID) { id, policy in try await gate.resolve(id, policy) }
        } transform: { posts, authors in Pair(posts: posts, authors: authors) }
        let old = Task { await composition.load() }
        await gate.waitForCalls(1)
        rootID = 2
        let new = Task { await composition.load(using: .remote) }
        await gate.waitForCalls(2)
        gate.finish(id: 2, value: "new")
        await new.value
        #expect(value(composition.latest)?.posts.first?.id == 2)
        gate.finish(id: 1, value: "obsolete")
        await old.value
        #expect(value(composition.latest)?.authors == [2: "new"])
    }

    @Test("Observed root changes supersede work and never mix the root and dictionary")
    func observedSupersession() async {
        let root = Root([Post(id: 1, authorID: 1)])
        let gate = Gate()
        let composition = Composition {
            Input { root.posts }.resolving(\.authorID) { id, policy in try await gate.resolve(id, policy) }
        } transform: { posts, authors in
            #expect(Set(posts.map(\.authorID)) == Set(authors.keys))
            return Pair(posts: posts, authors: authors)
        }
        await gate.waitForCalls(1)
        root.posts = [Post(id: 2, authorID: 2)]
        await gate.waitForCalls(2)
        gate.finish(id: 1, value: "obsolete")
        gate.finish(id: 2, value: "current")
        await until { value(composition.latest)?.authors == [2: "current"] }
        #expect(value(composition.latest)?.posts == root.posts)
    }

    @Test("Caller cancellation rejects late closure results and allows retry")
    func cancellation() async {
        let source = Source<[Post]>()
        let gate = Gate()
        let composition = Composition {
            Input(source) { _ in source.latest = .result(.success([Post(id: 1, authorID: 1)])) }
                .resolving(\.authorID) { id, policy in try await gate.resolve(id, policy) }
        } transform: { posts, authors in Pair(posts: posts, authors: authors) }
        let load = Task { await composition.load() }
        await gate.waitForCalls(1)
        load.cancel()
        gate.finish(id: 1, value: "cancelled")
        await load.value
        #expect(isReset(composition.latest))
        let retry = Task { await composition.load() }
        await gate.waitForCalls(2)
        gate.finish(id: 1, value: "retried")
        await retry.value
        #expect(value(composition.latest)?.authors == [1: "retried"])
    }

    @Test("Root reset clears the pair and rejects pending resolution")
    func rootReset() async throws {
        let root = Bucket(Key<[Post]>("root"))
        let gate = Gate()
        let composition = Composition {
            Input(root).resolving(\.authorID) { id, policy in try await gate.resolve(id, policy) }
        } transform: { posts, authors in Pair(posts: posts, authors: authors) }
        try await root.store([Post(id: 1, authorID: 1)])
        await gate.waitForCalls(1)
        gate.finish(id: 1, value: "one")
        await until { value(composition.latest) != nil }
        try await root.store([Post(id: 2, authorID: 2)])
        await gate.waitForCalls(2)
        try await root.reset()
        await until { isReset(composition.latest) }
        gate.finish(id: 2, value: "obsolete")
        await composition.load()
        #expect(isReset(composition.latest))
    }

    @Test("Unrelated loading proceeds while relationships wait")
    func unrelatedLoads() async {
        let source = Source<[Post]>()
        let gate = Gate()
        let sibling = Root()
        let composition = Composition {
            Input(source) { _ in source.latest = .result(.success([Post(id: 1, authorID: 1)])) }
                .resolving(\.authorID) { id, policy in try await gate.resolve(id, policy) }
            Input { sibling.posts.count } load: { _ in sibling.posts = [Post(id: 2, authorID: 2)] }
        } transform: { posts, authors, count in Pair(posts: posts, authors: authors.mapValues { "\($0):\(count)" }) }
        let load = Task { await composition.load() }
        await gate.waitForCalls(1)
        await until { sibling.posts.count == 1 }
        gate.finish(id: 1, value: "one")
        await load.value
        #expect(value(composition.latest)?.authors == [1: "one:1"])
    }

    @Test("Builder branches and reused declarations keep independent paired state")
    func branchesAndReuse() async {
        var calls = 0
        let declaration = Input { [Post(id: 1, authorID: 1)] }.resolving(\.authorID) { _, _ in
            calls += 1
            return "\(calls)"
        }
        let a = Composition { declaration } transform: { posts, authors in Pair(posts: posts, authors: authors) }
        let b = Composition {
            if true { declaration }
            Input { 3 }
        } transform: { (posts: [Post]?, authors: [Int: String]?, _: Int) in Pair(posts: posts ?? [], authors: authors ?? [:]) }
        await until { value(a.latest)?.authors.count == 1 && value(b.latest)?.authors.count == 1 }
        #expect(calls == 2)
        #expect(value(a.latest)?.authors != value(b.latest)?.authors)
    }

    @Test("Root failure is a failure without inventing a reset")
    func rootFailure() async throws {
        let source = Source<[Post]>()
        source.latest = .result(.success([Post(id: 1, authorID: 1)]))
        let composition = Composition {
            Input(source).resolving(\.authorID) { _, _ in "one" }
        } transform: { posts, authors in Pair(posts: posts, authors: authors) }
        await until { value(composition.latest) != nil }
        var iterator = composition.makeAsyncIterator()
        _ = await iterator.next()
        source.latest = .result(.failure(LookupError.failed))
        #expect(isFailure(try #require(await iterator.next())))
    }

    @Test("Partition reset history survives recovery and nested observation")
    func nestedResetHistory() async throws {
        let authors = Bucket(Key<String>("authors"), partitionedBy: Int.self) { _ in
            RemoteSource { "one" }
        }
        let child = Composition {
            Input { [Post(id: 1, authorID: 1)] }.resolving(\.authorID, from: authors)
        } transform: { posts, authors in Pair(posts: posts, authors: authors) }
        let parent = Composition { Input(child) } transform: { $0 }
        await until { value(parent.latest) != nil }
        var iterator = parent.makeAsyncIterator()
        _ = await iterator.next()
        try await authors[1].reset()
        try await authors[1].store("two")
        var sawReset = false
        while let update = await iterator.next() {
            if isReset(update) { sawReset = true }
            if value(update)?.authors[1] == "two" { break }
        }
        #expect(sawReset)
    }

    @Test("Native policy selection reuses partition memory or reaches remote", arguments: [LoadPolicy.cached, .cachedThenRemote, .remote])
    func nativePolicies(policy: LoadPolicy) async throws {
        let calls = Calls()
        let authors = Bucket(Key<String>("authors"), partitionedBy: Int.self) { id in
            LocalSource { await calls.record(-id); return "local" }
            RemoteSource { await calls.record(id); return "remote" }
        }
        let source = Source<[Post]>()
        let composition = Composition {
            Input(source) { received in
                #expect(name(received) == name(policy))
                source.latest = .result(.success([Post(id: 1, authorID: 1)]))
            }.resolving(\.authorID, from: authors)
        } transform: { posts, authors in Pair(posts: posts, authors: authors) }
        await composition.load(using: policy)
        switch policy {
        case .cached:
            #expect(await calls.values == [-1])
            #expect(value(composition.latest)?.authors == [1: "local"])
        case .cachedThenRemote:
            #expect(await calls.values == [-1, 1])
            #expect(value(composition.latest)?.authors == [1: "remote"])
        case .remote:
            #expect(await calls.values == [1])
            #expect(value(composition.latest)?.authors == [1: "remote"])
        }
    }

    @Test("A native reset during another lookup invalidates the retained pair")
    func resetDuringResolution() async throws {
        let root = Root([Post(id: 1, authorID: 1)])
        let gate = Gate()
        let authors = Bucket(Key<String>("authors"), partitionedBy: Int.self) { id in
            RemoteSource { try await gate.resolve(id, .cached) }
        }
        let composition = Composition {
            Input { root.posts }.resolving(\.authorID, from: authors)
        } transform: { posts, authors in Pair(posts: posts, authors: authors) }
        await gate.waitForCalls(1)
        gate.finish(id: 1, value: "one")
        await until { value(composition.latest) != nil }
        root.posts.append(Post(id: 2, authorID: 2))
        await gate.waitForCalls(2)
        try await authors[1].reset()
        await until { isReset(composition.latest) }
        gate.finish(id: 2, value: "two")
        try await authors[1].store("recovered")
        await until { value(composition.latest)?.authors == [1: "recovered", 2: "two"] }
    }

    @Test("Loaded optional relationship values remain present dictionary entries")
    func optionalValues() async {
        let composition = Composition {
            Input { [Post(id: 1, authorID: 1)] }.resolving(\.authorID) { _, _ -> String? in nil }
        } transform: { (_: [Post], authors: [Int: String?]) in authors }
        await composition.load()
        #expect(value(composition.latest)?.count == 1)
    }

    @Test("Root reset is observed while its explicit loading action is suspended")
    func resetWhileLoadingRoot() async {
        let source = Source<[Post]>()
        source.latest = .result(.success([Post(id: 1, authorID: 1)]))
        let gate = Gate()
        let composition = Composition {
            Input(source) { policy in _ = try await gate.resolve(0, policy) }
                .resolving(\.authorID) { _, _ in "one" }
        } transform: { posts, authors in Pair(posts: posts, authors: authors) }
        await until { value(composition.latest) != nil }
        let load = Task { await composition.load(using: .remote) }
        await gate.waitForCalls(1)
        source.latest = .reset
        await until { isReset(composition.latest) }
        gate.finish(id: 0, value: "finished")
        await load.value
        #expect(isReset(composition.latest))
    }

    @Test("Sequence-backed array inputs resolve their emitted roots and recover from failure")
    func sequenceRoot() async {
        let (stream, producer) = AsyncStream<Result<[Post], LookupError>>.makeStream()
        var keys: [Int] = []
        let composition = Composition {
            Input(updates: { stream }).resolving(\.authorID) { id, policy in
                #expect(name(policy) == "cached")
                keys.append(id)
                return "\(id)"
            }
        } transform: { posts, authors in Pair(posts: posts, authors: authors) }
        #expect(isReset(composition.latest))
        producer.yield(.success([Post(id: 1, authorID: 1)]))
        await until { value(composition.latest)?.authors == [1: "1"] }
        producer.yield(.failure(.failed))
        await until { isFailure(composition.latest) }
        producer.yield(.success([Post(id: 2, authorID: 2)]))
        await until { value(composition.latest)?.authors == [2: "2"] }
        #expect(keys == [1, 2])
        producer.finish()
    }

    @Test("Releasing the composition releases relationship state and cancels lookups")
    func lifetime() async {
        let gate = Gate()
        var composition: Composition<Pair>? = Composition {
            Input { [Post(id: 1, authorID: 1)] }.resolving(\.authorID) { id, policy in
                try await gate.resolve(id, policy)
            }
        } transform: { posts, authors in Pair(posts: posts, authors: authors) }
        weak var weakComposition = composition
        await gate.waitForCalls(1)
        composition = nil
        #expect(weakComposition == nil)
        gate.finish(id: 1, value: "late")
        await gate.waitForReturns(1)
        #expect(gate.cancelledReturns == 1)
    }
}

private nonisolated struct Post: Sendable, Equatable { let id: Int; let authorID: Int }
private nonisolated struct Pair: Sendable, Equatable { let posts: [Post]; let authors: [Int: String] }
private enum LookupError: Error { case failed }
@Observable private final class Root {
    var posts: [Post]
    init(_ posts: [Post] = []) { self.posts = posts }
}
@Observable private final class Source<T: Sendable>: UpdateSource {
    var latest: Update<T> = .reset
    func _latestUpdateForObservation() -> Update<T> { latest }
}
private actor Calls {
    var values: [Int] = []
    func record(_ id: Int) { values.append(id) }
}
private actor FailureSwitch {
    var enabled = false
    func set(_ value: Bool) { enabled = value }
}
@Observable private final class Gate {
    var ids: [Int] = []
    var returns = 0
    var cancelledReturns = 0
    @ObservationIgnored var continuations: [Int: CheckedContinuation<String, any Error>] = [:]
    func resolve(_ id: Int, _ policy: LoadPolicy) async throws -> String {
        let value = try await withCheckedThrowingContinuation { continuation in
            continuations[id] = continuation
            ids.append(id)
        }
        returns += 1
        if Task.isCancelled { cancelledReturns += 1 }
        return value
    }
    func waitForCalls(_ count: Int) async { await until { self.ids.count >= count } }
    func waitForReturns(_ count: Int) async { await until { self.returns >= count } }
    func finish(id: Int, value: String) { continuations.removeValue(forKey: id)?.resume(returning: value) }
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
private func isReset<T>(_ update: Update<T>) -> Bool { if case .reset = update { return true }; return false }
private func isFailure<T>(_ update: Update<T>) -> Bool { if case .result(.failure) = update { return true }; return false }
private func name(_ policy: LoadPolicy) -> String {
    switch policy { case .cached: "cached"; case .cachedThenRemote: "cachedThenRemote"; case .remote: "remote" }
}
