import Continuum
import Foundation
import Observation
import Testing

@Suite("Data buckets")
struct BucketTests {
    private struct Label: Equatable, Sendable {
        let id: Int
        let text: String
    }

    private struct Run: Equatable, Sendable {
        let id: Int
        let status: String
    }

    private struct Subscription: Equatable, Sendable {
        let plan: String
        var runs: [Run]
    }

    nonisolated private enum Purpose: Hashable, Sendable {
        case buy
        case sell
    }

    private enum TestData {
        static let count = Key<Int>("test.count")
        static let labels = IndexedKey<Int, Label>(
            "test.labels",
            indexedBy: \.id
        )
        static let subscription = Key<Subscription>("test.subscription")
    }

    @Test("A singleton bucket stores and removes its value")
    func singletonMemory() async throws {
        let count = Bucket(TestData.count)

        #expect(count.value == nil)
        #expect(count.isLoaded == false)

        try await count.store(3)

        #expect(count.value == 3)
        #expect(count.isLoaded)

        try await count.remove()

        #expect(count.value == nil)
        #expect(count.isLoaded == false)
    }

    @Test("A stored value participates in observation tracking")
    func observation() async throws {
        let count = Bucket(TestData.count)

        try await confirmation { changed in
            withObservationTracking {
                _ = count.value
            } onChange: {
                changed()
            }

            try await count.store(1)
        }
    }

    @Test("Updates emit an established snapshot immediately")
    func updatesEmitEstablishedSnapshot() async throws {
        let labels = Bucket(TestData.labels)
        let expected = Label(id: 1, text: "one")
        try await labels.store(expected)
        var updates = labels.updates().makeAsyncIterator()

        guard case .result(.success(let snapshot))? = await updates.next() else {
            Issue.record("Expected the established snapshot")
            return
        }
        #expect(snapshot == [expected])
    }

    @Test("Updates emit reset while the bucket remains unavailable")
    func updatesEmitReset() async throws {
        let labels = Bucket(TestData.labels)
        try await labels.store(Label(id: 1, text: "before"))
        var updates = labels.updates().makeAsyncIterator()
        _ = await updates.next()

        try await labels.reset()

        guard case .reset? = await updates.next() else {
            Issue.record("Expected reset")
            return
        }
    }

    @Test("An indexed subscript returns the model")
    func indexedSubscript() async throws {
        let labels = Bucket(TestData.labels)
        let label = Label(id: 7, text: "seven")

        try await labels.store(label)

        #expect(labels[7] == label)
        #expect(labels.value(for: 7) == label)
    }

    @Test("Removing the final indexed value emits loaded empty")
    func indexedRemovalRemainsAvailable() async throws {
        let labels = Bucket(TestData.labels)
        try await labels.store(Label(id: 1, text: "one"))
        var updates = labels.updates().makeAsyncIterator()
        _ = await updates.next()

        try await labels.remove(1)

        #expect(labels.isLoaded)
        #expect(labels.isEmpty)
        guard case .result(.success(let snapshot))? = await updates.next() else {
            Issue.record("Expected a successful empty snapshot")
            return
        }
        #expect(snapshot.isEmpty)
    }

    @Test("The outer key context types a singleton remote source")
    func singletonRemoteSourceInference() async throws {
        let count = Bucket(TestData.count) {
            RemoteSource { 42 }
        }

        let value = try await count.load()

        #expect(value == 42)
        #expect(count.value == 42)
        #expect(count.isLoaded)
    }

    @Test("An indexed source loads the complete snapshot atomically")
    func indexedRemoteSourceInference() async throws {
        let labels = Bucket(TestData.labels) {
            RemoteSource {
                [
                    Label(id: 2, text: "two"),
                    Label(id: 1, text: "one"),
                ]
            }
        }

        let snapshot = try await labels.load()

        #expect(snapshot.map(\.id) == [2, 1])
        #expect(labels.keys == [2, 1])
        #expect(labels[1]?.text == "one")
        #expect(labels.isLoaded)
    }

    @Test("The partition initializer establishes independent snapshots")
    func partitionedSnapshots() async throws {
        let requested = PartitionCounter<Purpose>()
        let labels: PartitionedIndexedBucket<Purpose, Int, Label> = Bucket(
            TestData.labels,
            partitionedBy: Purpose.self
        ) { purpose in
            RemoteSource {
                await requested.increment(purpose)
                switch purpose {
                case .buy:
                    return [Label(id: 1, text: "buy")]
                case .sell:
                    return [Label(id: 2, text: "sell")]
                }
            }
        }

        let buying = labels[.buy]
        let selling = labels[.sell]

        #expect(buying === labels[.buy])
        #expect(buying.isLoaded == false)
        #expect(selling.isLoaded == false)

        _ = try await buying.load()

        #expect(buying.isLoaded)
        #expect(buying[1]?.text == "buy")
        #expect(selling.isLoaded == false)
        #expect(selling.values.isEmpty)

        _ = try await selling.load()

        #expect(selling.isLoaded)
        #expect(selling[2]?.text == "sell")
        #expect(await requested.value(for: .buy) == 1)
        #expect(await requested.value(for: .sell) == 1)
    }

    @Test("Update streams are scoped to a partition lifetime")
    func partitionedUpdates() async throws {
        let labels = Bucket(
            TestData.labels,
            partitionedBy: Purpose.self
        ) { _ in }
        let buying = labels[.buy]
        let selling = labels[.sell]
        var buyingUpdates = buying.updates().makeAsyncIterator()

        #expect(buying === labels[.buy])

        try await buying.store(Label(id: 1, text: "buy"))
        _ = await buyingUpdates.next()
        try await buying.reset()

        guard case .reset? = await buyingUpdates.next() else {
            Issue.record("Expected the selected partition to reset")
            return
        }
        #expect(selling.isLoaded == false)
    }

    @Test("Bucket updates combine any number of successful snapshots")
    func combinedUpdates() async throws {
        let count = Bucket(TestData.count)
        let labels = Bucket(TestData.labels)
        let subscriptions = Bucket(
            TestData.subscription,
            partitionedBy: Purpose.self
        ) { _ in }
        let subscription = subscriptions[.buy]
        var updates = bucketUpdates(
            observing: (count, labels, subscription),
            transform: { count, labels, subscription in
                "\(count):\(labels.count):\(subscription.plan)"
            }
        ).makeAsyncIterator()

        try await count.store(2)
        try await labels.store(Label(id: 1, text: "one"))
        try await subscription.store(
            Subscription(plan: "plus", runs: [])
        )

        guard case .result(.success(let value))? = await updates.next() else {
            Issue.record("Expected the combined value")
            return
        }
        #expect(value == "2:1:plus")

        try await labels.reset()

        guard case .reset? = await updates.next() else {
            Issue.record("Expected the combined stream to reset")
            return
        }
    }

    @Test("Aggregate failures take precedence over unavailable sources")
    func combinedUpdateFailurePrecedence() async {
        let unavailable = TestUpdateSource<String>(.reset)
        let failed = TestUpdateSource<Int>(
            .result(.failure(TestError.failed))
        )
        var updates = bucketUpdates(
            observing: (unavailable, failed),
            transform: { _, _ in "unreachable" }
        ).makeAsyncIterator()

        guard case .result(.failure(let error))? = await updates.next() else {
            Issue.record("Expected the dependency failure")
            return
        }
        #expect(error is TestError)
    }

    @Test("Aggregate failures can preserve every dependency error")
    func combinedUpdateFailureCollection() async {
        let first = TestUpdateSource<Int>(
            .result(.failure(DependencyError(id: 1)))
        )
        let unavailable = TestUpdateSource<String>(.reset)
        let second = TestUpdateSource<Bool>(
            .result(.failure(DependencyError(id: 2)))
        )
        var updates = bucketUpdates(
            observing: (first, unavailable, second),
            mapFailures: { errors in
                CollectedDependencyErrors(
                    ids: errors.compactMap {
                        ($0 as? DependencyError)?.id
                    }
                )
            },
            transform: { _, _, _ in "unreachable" }
        ).makeAsyncIterator()

        guard case .result(.failure(let error))? = await updates.next(),
              let collected = error as? CollectedDependencyErrors else {
            Issue.record("Expected the collected dependency failures")
            return
        }
        #expect(collected.ids == [1, 2])
    }

    @Test("A same-turn replacement skips aggregate reset")
    func combinedUpdateReplacement() async {
        let source = TestUpdateSource<Int>(.result(.success(1)))
        var updates = bucketUpdates(
            observing: source,
            transform: { $0 }
        ).makeAsyncIterator()
        _ = await updates.next()

        source.replace(2)

        guard case .result(.success(let value))? = await updates.next() else {
            Issue.record("Expected the replacement result")
            return
        }
        #expect(value == 2)
    }

    @Test("Transform errors become aggregate failures")
    func combinedUpdateTransformFailure() async {
        let source = TestUpdateSource<Int>(.result(.success(1)))
        var updates = bucketUpdates(
            observing: source,
            transform: { _ in throw TestError.failed }
        ).makeAsyncIterator()

        guard case .result(.failure(let error))? = await updates.next() else {
            Issue.record("Expected the transform failure")
            return
        }
        #expect(error is TestError)
    }

    @Test("A refresh does not republish the established snapshot")
    func updatesIgnoreRefreshBookkeeping() async throws {
        let gate = SnapshotGate<[Label]>()
        let labels = Bucket(TestData.labels) {
            RemoteSource {
                await gate.load()
            }
        }

        let initialLoad = Task {
            try await labels.load(using: .remote)
        }
        await gate.waitForLoads(1)
        await gate.complete(
            load: 1,
            with: [Label(id: 1, text: "before")]
        )
        _ = try await initialLoad.value

        var updates = labels.updates().makeAsyncIterator()
        _ = await updates.next()

        let refresh = Task {
            try await labels.load(using: .remote)
        }
        await gate.waitForLoads(2)
        await gate.complete(
            load: 2,
            with: [Label(id: 2, text: "after")]
        )
        _ = try await refresh.value

        guard case .result(.success(let snapshot))? = await updates.next() else {
            Issue.record("Expected the refreshed snapshot")
            return
        }
        #expect(snapshot.map(\.text) == ["after"])
    }

    @Test("A loaded empty partition does not load another partition")
    func partitionedEmptyState() async throws {
        let labels = Bucket(
            TestData.labels,
            partitionedBy: Purpose.self
        ) { _ in
            RemoteSource { [] }
        }

        _ = try await labels[.buy].load()

        #expect(labels[.buy].isLoaded)
        #expect(labels[.buy].isEmpty)
        #expect(labels[.sell].isLoaded == false)
        #expect(labels[.sell].isEmpty)
    }

    @Test("Partition mutations remain scoped to the selected list")
    func partitionedMutations() async throws {
        let labels = Bucket(
            TestData.labels,
            partitionedBy: Purpose.self
        ) { _ in }

        try await labels[.buy].store(Label(id: 1, text: "buy"))
        try await labels[.sell].store(Label(id: 1, text: "sell"))

        #expect(labels[.buy][1]?.text == "buy")
        #expect(labels[.sell][1]?.text == "sell")

        try await labels[.buy].remove(1)

        #expect(labels[.buy].isLoaded)
        #expect(labels[.buy].isEmpty)
        #expect(labels[.sell][1]?.text == "sell")
    }

    @Test("Loads coalesce within a partition but not across partitions")
    func partitionedCoalescing() async throws {
        let counter = PartitionCounter<Purpose>()
        let labels = Bucket(
            TestData.labels,
            partitionedBy: Purpose.self
        ) { purpose in
            RemoteSource {
                await counter.increment(purpose)
                try await Task.sleep(for: .milliseconds(20))
                return [Label(id: purpose == .buy ? 1 : 2, text: "loaded")]
            }
        }

        async let firstBuy = labels[.buy].load()
        async let secondBuy = labels[.buy].load()
        async let sell = labels[.sell].load()
        _ = try await (firstBuy, secondBuy, sell)

        #expect(await counter.value(for: .buy) == 1)
        #expect(await counter.value(for: .sell) == 1)
    }

    @Test("A selected partition participates in observation tracking")
    func partitionObservation() async throws {
        let labels = Bucket(
            TestData.labels,
            partitionedBy: Purpose.self
        ) { _ in }
        let buying = labels[.buy]

        try await confirmation { changed in
            withObservationTracking {
                _ = buying.values
            } onChange: {
                changed()
            }

            try await buying.store(Label(id: 1, text: "buy"))
        }
    }

    @Test("Cached loads try local snapshots in declaration order")
    func localSourceOrder() async throws {
        let first = Counter()
        let second = Counter()
        let remote = Counter()
        let labels = Bucket(TestData.labels) {
            LocalSource {
                await first.increment()
                return nil
            }
            LocalSource {
                await second.increment()
                return [Label(id: 5, text: "local")]
            }
            RemoteSource {
                await remote.increment()
                return [Label(id: 5, text: "remote")]
            }
        }

        let snapshot = try await labels.load()

        #expect(snapshot == [Label(id: 5, text: "local")])
        #expect(await first.value == 1)
        #expect(await second.value == 1)
        #expect(await remote.value == 0)
    }

    @Test("Mutations persist normalized atomic snapshots")
    func mutationPersistence() async throws {
        let writes = SnapshotRecorder<[Label]>()
        let labels = Bucket(TestData.labels) {
            LocalSource {
                nil
            } persist: { snapshot in
                await writes.record(snapshot)
            }
        }

        try await labels.store(Label(id: 1, text: "one"))
        try await labels.store(Label(id: 2, text: "two"))
        try await labels.remove(1)
        try await labels.remove(2)

        let snapshots = await writes.snapshots
        #expect(snapshots.count == 4)
        #expect(snapshots[0]?.map(\.id) == [1])
        #expect(snapshots[1]?.map(\.id) == [1, 2])
        #expect(snapshots[2]?.map(\.id) == [2])
        #expect(snapshots[3]?.isEmpty == true)
        #expect(labels.isLoaded)
        #expect(labels.isEmpty)
    }

    @Test("Remote mutations publish locally before remote reconciliation")
    func remoteMutations() async throws {
        let calls = StringRecorder()
        let writes = SnapshotRecorder<[Label]>()
        let remoteStores = SnapshotRecorder<Label>()
        let submitted = Label(id: 1, text: "draft")
        let authoritative = Label(id: 1, text: "published")
        let labels = Bucket(TestData.labels) {
            LocalSource {
                nil
            } persist: { snapshot in
                await calls.record("local")
                await writes.record(snapshot)
            }
            RemoteSource {
                Load {
                    [Label]()
                }
                Store { value in
                    await remoteStores.record(value)
                    await calls.record("remote store")
                    return authoritative
                }
                Remove { id in
                    #expect(id == authoritative.id)
                    await calls.record("remote remove")
                }
            }
        }

        try await labels.store(submitted)

        #expect(labels.values == [authoritative])
        #expect(await remoteStores.snapshots == [submitted])
        #expect(await calls.values == ["local", "remote store", "local"])
        #expect(await writes.snapshots == [[submitted], [authoritative]])

        try await labels.remove(authoritative.id)

        #expect(labels.isLoaded)
        #expect(labels.isEmpty)
        #expect(
            await calls.values
                == [
                    "local", "remote store", "local", "local",
                    "remote remove",
                ]
        )
        #expect(await writes.snapshots == [[submitted], [authoritative], []])
    }

    @Test("A mutation publishes before its remote store completes")
    func resetDuringMutation() async throws {
        let remoteStore = SnapshotGate<Label>()
        let writes = SnapshotRecorder<[Label]>()
        let labels = Bucket(TestData.labels) {
            LocalSource {
                nil
            } persist: { snapshot in
                await writes.record(snapshot)
            }
            RemoteSource {
                Load {
                    [Label]()
                }
                Store { _ in
                    await remoteStore.load()
                }
            }
        }
        let mutation = Task {
            try await labels.store(Label(id: 1, text: "draft"))
        }
        await remoteStore.waitForLoads(1)

        #expect(labels.values == [Label(id: 1, text: "draft")])
        #expect(await writes.snapshots == [[Label(id: 1, text: "draft")]])

        let reset = Task {
            try await labels.reset()
        }
        while labels.isLoaded {
            await Task.yield()
        }
        await remoteStore.complete(
            load: 1,
            with: Label(id: 1, text: "published")
        )

        await #expect(throws: CancellationError.self) {
            try await mutation.value
        }
        try await reset.value
        #expect(
            await writes.snapshots
                == [[Label(id: 1, text: "draft")], nil]
        )
        #expect(labels.isLoaded == false)
        #expect(labels.isEmpty)
    }

    @Test("An authoritative remote identifier replaces the submitted entry")
    func authoritativeRemoteIdentifier() async throws {
        let existing = Label(id: 1, text: "existing")
        let labels = Bucket(TestData.labels) {
            LocalSource {
                [existing]
            }
            RemoteSource {
                Load {
                    [Label]()
                }
                Store { _ in
                    Label(id: 2, text: "created")
                }
            }
        }
        _ = try await labels.load()

        try await labels.store(Label(id: 1, text: "draft"))

        #expect(labels.keys == [2])
        #expect(labels[1] == nil)
        #expect(labels[2]?.text == "created")
    }

    @Test("A void remote store retains the submitted value")
    func voidRemoteStore() async throws {
        let stored = SnapshotRecorder<Int>()
        let removed = Counter()
        let count = Bucket(TestData.count) {
            RemoteSource {
                Load<Int> {
                    0
                }
                Store<Int> { value in
                    await stored.record(value)
                }
                Remove<SingletonInput> {
                    await removed.increment()
                }
            }
        }

        try await count.store(3)

        #expect(count.value == 3)
        #expect(await stored.snapshots == [3])

        try await count.remove()

        #expect(count.value == nil)
        #expect(count.isLoaded == false)
        #expect(await removed.value == 1)
    }

    @Test("A failed remote store rolls back local and memory state")
    func remoteStoreFailure() async throws {
        let writes = SnapshotRecorder<[Label]>()
        let remoteStore = SnapshotGate<Label>()
        let existing = Label(id: 1, text: "existing")
        let labels = Bucket(TestData.labels) {
            LocalSource {
                [existing]
            } persist: { snapshot in
                await writes.record(snapshot)
            }
            RemoteSource {
                Load {
                    [Label]()
                }
                Store { (_: Label) async throws -> Label in
                    _ = await remoteStore.load()
                    throw TestError.failed
                }
            }
        }
        _ = try await labels.load()

        let mutation = Task {
            try await labels.store(Label(id: 2, text: "new"))
        }
        await remoteStore.waitForLoads(1)

        #expect(labels.values == [existing, Label(id: 2, text: "new")])
        #expect(
            await writes.snapshots
                == [[existing, Label(id: 2, text: "new")]]
        )

        await remoteStore.complete(
            load: 1,
            with: Label(id: 2, text: "ignored")
        )

        await #expect(throws: TestError.self) {
            try await mutation.value
        }

        #expect(
            await writes.snapshots
                == [[existing, Label(id: 2, text: "new")], [existing]]
        )
        #expect(labels.values == [existing])
        #expect(labels.isLoaded)
        #expect(labels.error is TestError)
    }

    @Test("A failed remote remove rolls back local and memory state")
    func remoteRemoveFailure() async throws {
        let writes = SnapshotRecorder<[Label]>()
        let remoteRemove = SnapshotGate<Void>()
        let existing = Label(id: 1, text: "existing")
        let labels = Bucket(TestData.labels) {
            LocalSource {
                [existing]
            } persist: { snapshot in
                await writes.record(snapshot)
            }
            RemoteSource {
                Load {
                    [Label]()
                }
                Remove { (_: Int) async throws in
                    await remoteRemove.load()
                    throw TestError.failed
                }
            }
        }
        _ = try await labels.load()

        let mutation = Task {
            try await labels.remove(existing.id)
        }
        await remoteRemove.waitForLoads(1)

        #expect(labels.isEmpty)
        #expect(await writes.snapshots == [[]])

        await remoteRemove.complete(load: 1, with: ())

        await #expect(throws: TestError.self) {
            try await mutation.value
        }

        #expect(await writes.snapshots == [[], [existing]])
        #expect(labels.values == [existing])
        #expect(labels.error is TestError)
    }

    @Test("A local failure rolls back before requesting the remote store")
    func localFailureAfterRemoteStore() async throws {
        let remoteStores = Counter()
        let localWrites = Counter()
        let existing = Label(id: 1, text: "existing")
        let labels = Bucket(TestData.labels) {
            LocalSource {
                [existing]
            } persist: { _ in
                await localWrites.increment()
                if await localWrites.value == 1 {
                    throw TestError.failed
                }
            }
            RemoteSource {
                Load {
                    [Label]()
                }
                Store { (value: Label) in
                    await remoteStores.increment()
                    return value
                }
            }
        }
        _ = try await labels.load()

        await #expect(throws: TestError.self) {
            try await labels.store(Label(id: 2, text: "remote"))
        }

        #expect(await remoteStores.value == 0)
        #expect(await localWrites.value == 2)
        #expect(labels.values == [existing])
        #expect(labels.error is TestError)
    }

    @Test("Removing a singleton persists an absent snapshot")
    func singletonPersistenceRemoval() async throws {
        let writes = SnapshotRecorder<Int>()
        let count = Bucket(TestData.count) {
            LocalSource {
                nil
            } persist: { snapshot in
                await writes.record(snapshot)
            }
        }

        try await count.store(3)
        try await count.remove()

        let snapshots = await writes.snapshots
        #expect(snapshots.count == 2)
        #expect(snapshots[0] == 3)
        #expect(snapshots[1] == nil)
        #expect(count.isLoaded == false)
    }

    @Test("Writable local sources stop at the first failure")
    func persistenceFailureOrdering() async throws {
        let calls = StringRecorder()
        let existing = Label(id: 1, text: "existing")
        let labels = Bucket(TestData.labels) {
            LocalSource {
                [existing]
            } persist: { _ in
                await calls.record("first")
            }
            LocalSource {
                nil
            } persist: { _ in
                await calls.record("second")
                throw TestError.failed
            }
            LocalSource {
                nil
            } persist: { _ in
                await calls.record("third")
            }
        }
        _ = try await labels.load()

        await #expect(throws: TestError.self) {
            try await labels.store(Label(id: 2, text: "new"))
        }

        #expect(await calls.values == ["first", "second", "first", "second"])
        #expect(labels.values == [existing])
        #expect(labels.isLoaded)
    }

    @Test("Concurrent mutations derive snapshots serially")
    func concurrentMutationPersistence() async throws {
        let writes = PersistenceGate<[Label]>()
        let labels = Bucket(TestData.labels) {
            LocalSource {
                nil
            } persist: { snapshot in
                await writes.persist(snapshot)
            }
        }

        let first = Task {
            try await labels.store(Label(id: 1, text: "one"))
        }
        await writes.waitForWrites(1)

        let second = Task {
            try await labels.store(Label(id: 2, text: "two"))
        }
        await Task.yield()

        #expect(await writes.writeCount == 1)
        await writes.complete(write: 1)
        await writes.waitForWrites(2)

        let snapshots = await writes.snapshots
        #expect(snapshots[1]?.map(\.id) == [1, 2])

        await writes.complete(write: 2)
        _ = try await (first.value, second.value)

        #expect(labels.keys == [1, 2])
    }

    @Test("Coalesced remote loading writes through once")
    func remoteWriteThroughCoalescing() async throws {
        let writes = SnapshotRecorder<[Label]>()
        let labels = Bucket(TestData.labels) {
            LocalSource {
                nil
            } persist: { snapshot in
                await writes.record(snapshot)
            }
            RemoteSource {
                try await Task.sleep(for: .milliseconds(20))
                return [Label(id: 1, text: "remote")]
            }
        }

        async let first = labels.load()
        async let second = labels.load()
        _ = try await (first, second)

        #expect(await writes.snapshots.count == 1)
        #expect(labels[1]?.text == "remote")
    }

    @Test("A persistence failure prevents remote publication")
    func remotePersistenceFailure() async {
        let labels = Bucket(TestData.labels) {
            LocalSource {
                nil
            } persist: { _ in
                throw TestError.failed
            }
            RemoteSource {
                [Label(id: 1, text: "remote")]
            }
        }

        await #expect(throws: TestError.self) {
            try await labels.load()
        }

        #expect(labels.isLoaded == false)
        #expect(labels.isEmpty)
        #expect(labels.error is TestError)
    }

    @Test("Indexed values preserve insertion order")
    func insertionOrder() async throws {
        let labels = Bucket(TestData.labels)

        #expect(labels.isEmpty)
        #expect(labels.isLoaded == false)

        try await labels.store(Label(id: 3, text: "three"))
        try await labels.store(Label(id: 1, text: "one"))
        try await labels.store(Label(id: 2, text: "two"))

        #expect(labels.count == 3)
        #expect(labels.keys == [3, 1, 2])
        #expect(labels.values.map(\.text) == ["three", "one", "two"])

        try await labels.store(Label(id: 1, text: "ONE"))

        #expect(labels.keys == [3, 1, 2])
        #expect(labels.values.map(\.text) == ["three", "ONE", "two"])

        try await labels.remove(1)
        try await labels.store(Label(id: 1, text: "one-again"))

        #expect(labels.keys == [3, 2, 1])
        #expect(labels.values.map(\.text) == ["three", "two", "one-again"])
    }

    @Test("Duplicate indices retain first position and last value")
    func duplicateNormalization() async throws {
        let labels = Bucket(TestData.labels) {
            LocalSource {
                [
                    Label(id: 1, text: "first"),
                    Label(id: 2, text: "second"),
                    Label(id: 1, text: "replacement"),
                ]
            }
        }

        _ = try await labels.load()

        #expect(labels.keys == [1, 2])
        #expect(labels.values.map(\.text) == ["replacement", "second"])
    }

    @Test("A successful empty snapshot is loaded, not untouched")
    func emptySnapshotState() async throws {
        let labels = Bucket(TestData.labels) {
            RemoteSource { [] }
        }

        #expect(labels.values.isEmpty)
        #expect(labels.isLoaded == false)

        let snapshot = try await labels.load()

        #expect(snapshot.isEmpty)
        #expect(labels.values.isEmpty)
        #expect(labels.isEmpty)
        #expect(labels.isLoaded)
        #expect(labels.isLoading == false)
        #expect(labels.error == nil)
    }

    @Test("A cached empty snapshot resolves from memory")
    func cachedEmptySnapshot() async throws {
        let counter = Counter()
        let labels = Bucket(TestData.labels) {
            RemoteSource {
                await counter.increment()
                return []
            }
        }

        _ = try await labels.load()
        _ = try await labels.load()

        #expect(await counter.value == 1)
    }

    @Test("A cached load falls through local misses to remote")
    func remoteFallback() async throws {
        let labels = Bucket(TestData.labels) {
            LocalSource { nil }
            LocalSource { nil }
            RemoteSource { [Label(id: 5, text: "remote")] }
        }

        let snapshot = try await labels.load()

        #expect(snapshot == [Label(id: 5, text: "remote")])
    }

    @Test("A missing remote source becomes observable state")
    func missingRemoteSource() async {
        let labels = Bucket(TestData.labels)

        await #expect(throws: ContinuumError.self) {
            try await labels.load(using: .remote)
        }

        #expect(labels.isLoaded == false)
        #expect(labels.isLoading == false)
        #expect(labels.error is ContinuumError)
    }

    @Test("A remote load skips established memory and local snapshots")
    func remoteLoading() async throws {
        let local = Counter()
        let labels = Bucket(TestData.labels) {
            LocalSource {
                await local.increment()
                return [Label(id: 3, text: "local")]
            }
            RemoteSource {
                [Label(id: 3, text: "remote")]
            }
        }

        try await labels.store(Label(id: 3, text: "memory"))
        let snapshot = try await labels.load(using: .remote)

        #expect(snapshot.map(\.text) == ["remote"])
        #expect(await local.value == 0)
        #expect(labels[3]?.text == "remote")
    }

    @Test("Concurrent cached loads share one source flight")
    func coalescing() async throws {
        let counter = Counter()
        let labels = Bucket(TestData.labels) {
            RemoteSource {
                await counter.increment()
                try await Task.sleep(for: .milliseconds(20))
                return [Label(id: 4, text: "four")]
            }
        }

        async let first = labels.load()
        async let second = labels.load()
        let snapshots = try await (first, second)

        #expect(snapshots.0 == snapshots.1)
        #expect(await counter.value == 1)
    }

    @Test("A remote load supersedes cached source work")
    func remoteSupersedesCachedLoad() async throws {
        let local = Counter()
        let remoteCounter = Counter()
        let labels = Bucket(TestData.labels) {
            LocalSource {
                await local.increment()
                try await Task.sleep(for: .milliseconds(20))
                return [Label(id: 8, text: "local")]
            }
            RemoteSource {
                await remoteCounter.increment()
                return [Label(id: 8, text: "remote")]
            }
        }

        let cached = Task {
            try await labels.load()
        }
        await local.waitUntilIncremented()

        #expect(labels.isLoading)
        #expect(labels.isLoaded == false)

        let remoteLoad = Task {
            try await labels.load(using: .remote)
        }
        let snapshots = try await (cached.value, remoteLoad.value)

        #expect(snapshots.0 == snapshots.1)
        #expect(snapshots.0.map(\.text) == ["remote"])
        #expect(await local.value == 1)
        #expect(await remoteCounter.value == 1)
        #expect(labels.isLoading == false)
        #expect(labels.isLoaded)
    }

    @Test("Repeated remote loads use the latest source flight")
    func repeatedremoteLoads() async throws {
        let remote = SnapshotGate<[Label]>()
        let labels = Bucket(TestData.labels) {
            RemoteSource {
                await remote.load()
            }
        }

        let first = Task {
            try await labels.load(using: .remote)
        }
        await remote.waitForLoads(1)

        let second = Task {
            try await labels.load(using: .remote)
        }
        await remote.waitForLoads(2)

        await remote.complete(
            load: 1,
            with: [Label(id: 1, text: "superseded")]
        )
        await remote.complete(
            load: 2,
            with: [Label(id: 2, text: "latest")]
        )

        let snapshots = try await (first.value, second.value)

        #expect(snapshots.0 == snapshots.1)
        #expect(snapshots.0.map(\.text) == ["latest"])
        #expect(labels.keys == [2])
        #expect(labels.isLoading == false)
    }

    @Test("A cached load can return memory during a remote refresh")
    func cachedMemoryDuringRefresh() async throws {
        let remote = SnapshotGate<[Label]>()
        let labels = Bucket(TestData.labels) {
            RemoteSource {
                await remote.load()
            }
        }
        try await labels.store(Label(id: 1, text: "memory"))

        let refresh = Task {
            try await labels.load(using: .remote)
        }
        await remote.waitForLoads(1)

        let cached = try await labels.load()

        #expect(cached.map(\.text) == ["memory"])
        #expect(labels.isLoading)
        #expect(labels[1]?.text == "memory")

        await remote.complete(
            load: 1,
            with: [Label(id: 2, text: "remote")]
        )
        _ = try await refresh.value

        #expect(labels.keys == [2])
        #expect(labels.isLoading == false)
    }

    @Test("Cached then remote publishes both phases and returns remote")
    func cachedThenRemote() async throws {
        let local = Counter()
        let remote = SnapshotGate<[Label]>()
        let labels = Bucket(TestData.labels) {
            LocalSource {
                await local.increment()
                return [Label(id: 1, text: "cached")]
            }
            RemoteSource {
                await remote.load()
            }
        }

        let first = Task {
            try await labels.load(using: .cachedThenRemote)
        }
        await remote.waitForLoads(1)

        #expect(labels.isLoaded)
        #expect(labels.isLoading)
        #expect(labels[1]?.text == "cached")

        let second = Task {
            try await labels.load(using: .cachedThenRemote)
        }
        await remote.complete(
            load: 1,
            with: [Label(id: 2, text: "remote")]
        )

        let snapshots = try await (first.value, second.value)

        #expect(snapshots.0 == snapshots.1)
        #expect(snapshots.0.map(\.text) == ["remote"])
        #expect(labels.keys == [2])
        #expect(labels.isLoading == false)
        #expect(labels.error == nil)
        #expect(await local.value == 1)
        #expect(await remote.loadCount == 1)
    }

    @Test("Cached then remote skips local sources when memory exists")
    func cachedThenRemoteFromMemory() async throws {
        let local = Counter()
        let labels = Bucket(TestData.labels) {
            LocalSource {
                await local.increment()
                return [Label(id: 1, text: "local")]
            }
            RemoteSource {
                [Label(id: 2, text: "remote")]
            }
        }
        try await labels.store(Label(id: 1, text: "memory"))

        let snapshot = try await labels.load(using: .cachedThenRemote)

        #expect(snapshot.map(\.text) == ["remote"])
        #expect(await local.value == 0)
        #expect(labels.keys == [2])
    }

    @Test("Cached then remote upgrades cached work before refreshing")
    func cachedThenRemoteUpgradesCachedLoad() async throws {
        let local = SnapshotGate<[Label]?>()
        let remote = Counter()
        let labels = Bucket(TestData.labels) {
            LocalSource {
                await local.load()
            }
            RemoteSource {
                await remote.increment()
                return [Label(id: 2, text: "remote")]
            }
        }

        let cached = Task {
            try await labels.load()
        }
        await local.waitForLoads(1)

        let completion = Task {
            await Task.yield()
            await local.complete(
                load: 1,
                with: [Label(id: 1, text: "cached")]
            )
        }

        let refreshed = try await labels.load(using: .cachedThenRemote)
        _ = await completion.value
        let snapshots = try await (cached.value, refreshed)

        #expect(snapshots.0.map(\.text) == ["remote"])
        #expect(snapshots.1.map(\.text) == ["remote"])
        #expect(await local.loadCount == 1)
        #expect(await remote.value == 1)
        #expect(labels.keys == [2])
    }

    @Test("Cached then remote requires a remote source before using cache")
    func cachedThenRemoteRequiresRemote() async throws {
        let local = Counter()
        let labels = Bucket(TestData.labels) {
            LocalSource {
                await local.increment()
                return [Label(id: 1, text: "local")]
            }
        }
        try await labels.store(Label(id: 1, text: "memory"))

        await #expect(throws: ContinuumError.self) {
            try await labels.load(using: .cachedThenRemote)
        }

        #expect(await local.value == 0)
        #expect(labels[1]?.text == "memory")
        #expect(labels.isLoaded)
        #expect(labels.isLoading == false)
        #expect(labels.error is ContinuumError)
    }

    @Test("A failed remote phase preserves the published cached phase")
    func cachedThenRemoteFailure() async {
        let labels = Bucket(TestData.labels) {
            LocalSource {
                [Label(id: 1, text: "cached")]
            }
            RemoteSource { () async throws -> [Label] in
                throw TestError.failed
            }
        }

        await #expect(throws: TestError.self) {
            try await labels.load(using: .cachedThenRemote)
        }

        #expect(labels[1]?.text == "cached")
        #expect(labels.isLoaded)
        #expect(labels.isLoading == false)
        #expect(labels.error is TestError)
    }

    @Test("A failed refresh preserves the established snapshot")
    func failedRefresh() async throws {
        let labels = Bucket(TestData.labels) {
            RemoteSource { () async throws -> [Label] in
                throw TestError.failed
            }
        }
        try await labels.store(Label(id: 1, text: "existing"))

        await #expect(throws: TestError.self) {
            try await labels.load(using: .remote)
        }

        #expect(labels.isLoaded)
        #expect(labels.isLoading == false)
        #expect(labels[1]?.text == "existing")
        #expect(labels.error != nil)
    }

    @Test("Paginated remote pages append in order and replace duplicates")
    func pagination() async throws {
        let nextPages = Counter()
        let writes = SnapshotRecorder<[Label]>()
        let labels = Bucket(TestData.labels) {
            LocalSource {
                nil
            } persist: { snapshot in
                await writes.record(snapshot)
            }
            RemoteSource {
                Load {
                    Page(
                        values: [
                            Label(id: 1, text: "one"),
                            Label(id: 2, text: "two"),
                        ],
                        next: 2
                    )
                }
                NextPage { cursor in
                    await nextPages.increment()
                    #expect(cursor == 2)
                    return Page(
                        values: [
                            Label(id: 2, text: "TWO"),
                            Label(id: 3, text: "three"),
                        ],
                        next: Optional<Int>.none
                    )
                }
            }
        }

        #expect(labels.hasNextPage == false)
        #expect(labels.isLoadingNextPage == false)

        let first = try await labels.load()

        #expect(first.map(\.id) == [1, 2])
        #expect(labels.hasNextPage)
        #expect(labels.pagination.error == nil)

        let merged = try await labels.loadNext()

        #expect(merged.map(\.id) == [1, 2, 3])
        #expect(merged.map(\.text) == ["one", "TWO", "three"])
        #expect(labels.keys == [1, 2, 3])
        #expect(labels.hasNextPage == false)
        #expect(labels.isLoadingNextPage == false)
        #expect(labels.nextPageError == nil)
        let persisted = await writes.snapshots
        #expect(persisted.count == 2)
        #expect(persisted[0]?.map(\.id) == [1, 2])
        #expect(persisted[1]?.map(\.id) == [1, 2, 3])

        let exhausted = try await labels.loadNext()

        #expect(exhausted == merged)
        #expect(await nextPages.value == 1)
    }

    @Test("A paginated remote source can also mutate values")
    func paginatedRemoteMutations() async throws {
        let calls = StringRecorder()
        let labels = Bucket(TestData.labels) {
            RemoteSource {
                Load {
                    Page(
                        values: [Label(id: 1, text: "one")],
                        next: 2
                    )
                }
                NextPage { _ in
                    Page(
                        values: [Label](),
                        next: Optional<Int>.none
                    )
                }
                Store { (value: Label) in
                    await calls.record("store \(value.id)")
                    return Label(id: value.id, text: "remote")
                }
                Remove { (id: Int) in
                    await calls.record("remove \(id)")
                }
            }
        }
        _ = try await labels.load()

        try await labels.store(Label(id: 2, text: "draft"))
        try await labels.remove(1)

        #expect(labels.keys == [2])
        #expect(labels[2]?.text == "remote")
        #expect(labels.hasNextPage)
        #expect(await calls.values == ["store 2", "remove 1"])
    }

    @Test("Concurrent next-page loads share one cursor operation")
    func paginationCoalescing() async throws {
        let nextPage = SnapshotGate<Page<[Label], Int>>()
        let labels = Bucket(TestData.labels) {
            RemoteSource {
                Load {
                    Page(
                        values: [Label(id: 1, text: "one")],
                        next: 2
                    )
                }
                NextPage { cursor in
                    #expect(cursor == 2)
                    return await nextPage.load()
                }
            }
        }
        _ = try await labels.load()

        async let first = labels.loadNext()
        async let second = labels.loadNext()
        await nextPage.waitForLoads(1)

        #expect(labels.isLoadingNextPage)
        #expect(labels.isLoading == false)

        await nextPage.complete(
            load: 1,
            with: Page(
                values: [Label(id: 2, text: "two")],
                next: nil
            )
        )
        let snapshots = try await (first, second)

        #expect(snapshots.0 == snapshots.1)
        #expect(snapshots.0.map(\.id) == [1, 2])
        #expect(await nextPage.loadCount == 1)
    }

    @Test("A failed next page preserves values and can be retried")
    func paginationFailure() async throws {
        let attempts = Counter()
        let labels = Bucket(TestData.labels) {
            RemoteSource {
                Load {
                    Page(
                        values: [Label(id: 1, text: "one")],
                        next: "cursor"
                    )
                }
                NextPage { cursor in
                    #expect(cursor == "cursor")
                    await attempts.increment()
                    if await attempts.value == 1 {
                        throw TestError.failed
                    }
                    return Page(
                        values: [Label(id: 2, text: "two")],
                        next: Optional<String>.none
                    )
                }
            }
        }
        _ = try await labels.load()

        await #expect(throws: TestError.self) {
            try await labels.loadNext()
        }

        #expect(labels.keys == [1])
        #expect(labels.hasNextPage)
        #expect(labels.isLoadingNextPage == false)
        #expect(labels.nextPageError is TestError)

        let recovered = try await labels.loadNext()

        #expect(recovered.map(\.id) == [1, 2])
        #expect(labels.hasNextPage == false)
        #expect(labels.nextPageError == nil)
        #expect(await attempts.value == 2)
    }

    @Test("A failed optimistic reset restores its pagination checkpoint")
    func resetFailureRestoresPagination() async throws {
        let existing = Label(id: 1, text: "existing")
        let writes = SnapshotRecorder<[Label]>()
        let labels = Bucket(TestData.labels) {
            LocalSource {
                nil
            } persist: { snapshot in
                await writes.record(snapshot)
                if snapshot == nil {
                    throw TestError.failed
                }
            }
            RemoteSource {
                Load {
                    Page(values: [existing], next: "cursor")
                }
                NextPage { (_: String) in
                    Page(values: [Label](), next: nil)
                }
            }
        }
        _ = try await labels.load()

        await #expect(throws: TestError.self) {
            try await labels.reset()
        }

        let snapshots = await writes.snapshots
        #expect(snapshots.count == 3)
        #expect(snapshots[0] == [existing])
        #expect(snapshots[1] == nil)
        #expect(snapshots[2] == [existing])
        #expect(labels.values == [existing])
        #expect(labels.hasNextPage)
        #expect(labels.error is TestError)
    }

    @Test("A remote initial page prevents an obsolete next page from publishing")
    func paginationSupersession() async throws {
        let firstPages = Counter()
        let nextPage = SnapshotGate<Page<[Label], Int>>()
        let labels = Bucket(TestData.labels) {
            RemoteSource {
                Load {
                    await firstPages.increment()
                    if await firstPages.value == 1 {
                        return Page(
                            values: [Label(id: 1, text: "first")],
                            next: 2
                        )
                    }
                    return Page(
                        values: [Label(id: 9, text: "refreshed")],
                        next: nil
                    )
                }
                NextPage { _ in
                    await nextPage.load()
                }
            }
        }
        _ = try await labels.load()

        let obsolete = Task {
            try await labels.loadNext()
        }
        await nextPage.waitForLoads(1)

        let refreshed = try await labels.load(using: .remote)

        await nextPage.complete(
            load: 1,
            with: Page(
                values: [Label(id: 2, text: "obsolete")],
                next: nil
            )
        )
        await #expect(throws: CancellationError.self) {
            try await obsolete.value
        }

        #expect(refreshed.map(\.id) == [9])
        #expect(labels.keys == [9])
        #expect(labels.hasNextPage == false)
        #expect(labels.isLoadingNextPage == false)
    }

    @Test("Pagination remains independent for each selected partition")
    func partitionPagination() async throws {
        let pages = PartitionCounter<Purpose>()
        let labels = Bucket(
            TestData.labels,
            partitionedBy: Purpose.self
        ) { purpose in
            RemoteSource {
                Load {
                    Page(
                        values: [
                            Label(
                                id: purpose == .buy ? 1 : 10,
                                text: "first"
                            ),
                        ],
                        next: 2
                    )
                }
                NextPage { _ in
                    await pages.increment(purpose)
                    return Page(
                        values: [
                            Label(
                                id: purpose == .buy ? 2 : 20,
                                text: "next"
                            ),
                        ],
                        next: Optional<Int>.none
                    )
                }
            }
        }

        _ = try await labels[.buy].load()
        _ = try await labels[.sell].load()
        _ = try await labels[.buy].loadNext()

        #expect(labels[.buy].keys == [1, 2])
        #expect(labels[.buy].hasNextPage == false)
        #expect(labels[.sell].keys == [10])
        #expect(labels[.sell].hasNextPage)
        #expect(await pages.value(for: .buy) == 1)
        #expect(await pages.value(for: .sell) == 0)
    }

    @Test("Next-page loading requires the paginated remote initial page")
    func paginationPreconditions() async throws {
        let ordinary = Bucket(TestData.labels) {
            RemoteSource { [Label(id: 1, text: "one")] }
        }
        let paginated = Bucket(TestData.labels) {
            RemoteSource {
                Load {
                    Page(
                        values: [Label(id: 1, text: "one")],
                        next: 2
                    )
                }
                NextPage { _ in
                    Page(
                        values: [Label](),
                        next: Optional<Int>.none
                    )
                }
            }
        }

        await #expect(throws: ContinuumError.self) {
            try await ordinary.loadNext()
        }
        await #expect(throws: ContinuumError.self) {
            try await paginated.loadNext()
        }

        #expect(ordinary.isLoaded == false)
        #expect(ordinary.nextPageError is ContinuumError)
        #expect(paginated.isLoaded == false)
        #expect(paginated.nextPageError is ContinuumError)
    }

    @Test("A nested page refreshes siblings and accumulates only its collection")
    func nestedPagination() async throws {
        let subscription = Bucket(TestData.subscription) {
            RemoteSource {
                Load {
                    Page(
                        value: Subscription(
                            plan: "starter",
                            runs: [
                                Run(id: 1, status: "finished"),
                                Run(id: 2, status: "running"),
                            ]
                        ),
                        next: 2
                    )
                }
                NextPage(
                    accumulating: \.runs,
                    indexedBy: \.id
                ) { cursor in
                    #expect(cursor == 2)
                    return Page(
                        value: Subscription(
                            plan: "pro",
                            runs: [
                                Run(id: 2, status: "finished"),
                                Run(id: 3, status: "running"),
                            ]
                        ),
                        next: Optional<Int>.none
                    )
                }
            }
        }

        let first = try await subscription.load()

        #expect(first.plan == "starter")
        #expect(first.runs.map(\.id) == [1, 2])
        #expect(subscription.hasNextPage)

        let merged = try await subscription.loadNext()

        #expect(merged.plan == "pro")
        #expect(merged.runs.map(\.id) == [1, 2, 3])
        #expect(
            merged.runs.map(\.status)
                == ["finished", "finished", "running"]
        )
        #expect(subscription.value == merged)
        #expect(subscription.hasNextPage == false)
    }

    @Test("Reset distinguishes untouched state from loaded empty")
    func reset() async throws {
        let writes = SnapshotRecorder<[Label]>()
        let labels = Bucket(TestData.labels) {
            LocalSource {
                []
            } persist: { snapshot in
                await writes.record(snapshot)
            }
        }

        _ = try await labels.load()
        #expect(await writes.snapshots.isEmpty)

        #expect(labels.isEmpty)
        #expect(labels.isLoaded)

        try await labels.reset()

        #expect(await writes.snapshots == [nil])
        #expect(labels.isEmpty)
        #expect(labels.isLoaded == false)
    }

    @Test("The deprecated synchronous reset schedules the canonical reset")
    func synchronousResetCompatibility() async throws {
        let existing = Label(id: 1, text: "existing")
        let writes = SnapshotRecorder<[Label]>()
        let labels = Bucket(TestData.labels) {
            LocalSource {
                [existing]
            } persist: { snapshot in
                await writes.record(snapshot)
            }
        }

        try await labels.store(existing)
        func callLegacyReset() {
            labels.reset()
        }
        callLegacyReset()

        await writes.waitForSnapshots(2)
        while labels.isLoaded {
            await Task.yield()
        }

        #expect(await writes.snapshots == [[existing], nil])
        #expect(labels.isLoaded == false)
    }

    @Test("A memory reset prevents pending source work from publishing")
    func resetDuringLoad() async throws {
        let remote = SnapshotGate<[Label]>()
        let labels = Bucket(TestData.labels) {
            RemoteSource {
                await remote.load()
            }
        }
        let load = Task {
            try await labels.load()
        }
        await remote.waitForLoads(1)

        try await labels.reset()
        await remote.complete(
            load: 1,
            with: [Label(id: 1, text: "obsolete")]
        )

        await #expect(throws: CancellationError.self) {
            try await load.value
        }
        #expect(labels.isLoaded == false)
        #expect(labels.isEmpty)
    }

    @Test("A reset removes persistence and memory")
    func localInclusiveReset() async throws {
        let existing = Label(id: 1, text: "one")
        let writes = PersistenceGate<[Label]>()
        let labels = Bucket(TestData.labels) {
            LocalSource {
                [existing]
            } persist: { snapshot in
                await writes.persist(snapshot)
            }
        }
        _ = try await labels.load()

        let mutation = Task {
            try await labels.reset()
        }
        await writes.waitForWrites(1)

        #expect(labels.isLoaded == false)
        #expect(labels.isEmpty)
        #expect(await writes.snapshots == [nil])

        await writes.complete(write: 1)
        try await mutation.value

        #expect(labels.isLoaded == false)
        #expect(labels.isEmpty)
        #expect(labels.error == nil)
    }

    @Test("A failed reset preserves memory")
    func localInclusiveResetFailure() async throws {
        let existing = Label(id: 1, text: "existing")
        let labels = Bucket(TestData.labels) {
            LocalSource {
                nil
            } persist: { snapshot in
                guard snapshot != nil else {
                    throw TestError.failed
                }
            }
        }
        try await labels.store(existing)
        var updates = labels.updates().makeAsyncIterator()
        _ = await updates.next()

        await #expect(throws: TestError.self) {
            try await labels.reset()
        }

        #expect(labels.values == [existing])
        #expect(labels.isLoaded)
        #expect(labels.error is TestError)
        guard case .result(.failure(let error))? = await updates.next() else {
            Issue.record("Expected the reset failure")
            return
        }
        #expect(error is TestError)
    }

    @Test("An invalidation signal resets local and in-memory state")
    func invalidationSignal() async throws {
        let writes = SnapshotRecorder<[Label]>()
        let (events, continuation) = AsyncStream<Void>.makeStream()
        let labels = Bucket(TestData.labels) {
            LocalSource {
                nil
            } persist: { snapshot in
                await writes.record(snapshot)
            }
            InvalidationSignal {
                events
            }
        }
        try await labels.store(Label(id: 1, text: "one"))

        continuation.yield()
        await writes.waitForSnapshots(2)
        while labels.isLoaded {
            await Task.yield()
        }
        continuation.finish()

        let snapshots = await writes.snapshots
        try #require(snapshots.count == 2)
        #expect(snapshots[1] == nil)
        #expect(labels.isLoaded == false)
        #expect(labels.isEmpty)
    }

    @Test("An invalidation signal retries after reset failure")
    func invalidationRetry() async throws {
        let persistence = ResetAttemptStore()
        let (events, continuation) = AsyncStream<Void>.makeStream()
        let existing = Label(id: 1, text: "existing")
        let labels = Bucket(TestData.labels) {
            LocalSource {
                nil
            } persist: { snapshot in
                try await persistence.persist(snapshot)
            }
            InvalidationSignal {
                events
            }
        }
        try await labels.store(existing)

        continuation.yield()
        await persistence.waitForResetAttempts(1)
        while labels.error == nil {
            await Task.yield()
        }

        #expect(labels.values == [existing])
        #expect(labels.error is TestError)

        continuation.yield()
        await persistence.waitForResetAttempts(2)
        while labels.isLoaded {
            await Task.yield()
        }
        continuation.finish()

        #expect(labels.isEmpty)
        #expect(labels.error == nil)
    }

    @Test("NotificationCenter can provide invalidation events")
    func notificationInvalidation() async throws {
        let writes = SnapshotRecorder<[Label]>()
        let name = Notification.Name(
            "ContinuumTests.notificationInvalidation"
        )
        let labels = Bucket(TestData.labels) {
            LocalSource {
                nil
            } persist: { snapshot in
                await writes.record(snapshot)
            }
            InvalidationSignal {
                NotificationCenter.default.notifications(named: name)
            }
        }
        try await labels.store(Label(id: 1, text: "one"))

        NotificationCenter.default.post(name: name, object: nil)
        await writes.waitForSnapshots(2)
        while labels.isLoaded {
            await Task.yield()
        }

        #expect(labels.isEmpty)
    }
}

private enum TestError: Error {
    case failed
}

private struct DependencyError: Error {
    let id: Int
}

private struct CollectedDependencyErrors: Error {
    let ids: [Int]
}

@Observable
private final class TestUpdateSource<Snapshot: Sendable>:
    BucketUpdateSource
{
    private var update: BucketUpdate<Snapshot>

    init(_ update: BucketUpdate<Snapshot>) {
        self.update = update
    }

    func _latestUpdateForObservation() -> BucketUpdate<Snapshot> {
        update
    }

    func replace(_ snapshot: Snapshot) {
        update = .reset
        update = .result(.success(snapshot))
    }
}

private actor Counter {
    private(set) var value = 0
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func increment() {
        value += 1
        let waiters = waiters
        self.waiters.removeAll()
        for waiter in waiters {
            waiter.resume()
        }
    }

    func waitUntilIncremented() async {
        guard value == 0 else { return }
        await withCheckedContinuation { continuation in
            waiters.append(continuation)
        }
    }
}

private actor PartitionCounter<Partition: Hashable & Sendable> {
    private var values: [Partition: Int] = [:]

    func increment(_ partition: Partition) {
        values[partition, default: 0] += 1
    }

    func value(for partition: Partition) -> Int {
        values[partition, default: 0]
    }
}

private actor SnapshotRecorder<Snapshot: Sendable> {
    private(set) var snapshots: [Snapshot?] = []
    private var waiters:
        [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []

    func record(_ snapshot: Snapshot?) {
        snapshots.append(snapshot)
        resumeWaiters()
    }

    func waitForSnapshots(_ count: Int) async {
        guard snapshots.count < count else { return }

        await withCheckedContinuation { continuation in
            waiters.append((count, continuation))
        }
    }

    private func resumeWaiters() {
        let ready = waiters.filter { $0.count <= snapshots.count }
        waiters.removeAll { $0.count <= snapshots.count }
        for waiter in ready {
            waiter.continuation.resume()
        }
    }
}

private actor StringRecorder {
    private(set) var values: [String] = []

    func record(_ value: String) {
        values.append(value)
    }
}

private actor ResetAttemptStore {
    private(set) var resetAttempts = 0
    private var waiters:
        [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []

    func persist<Snapshot: Sendable>(_ snapshot: Snapshot?) throws {
        guard case nil = snapshot else { return }

        resetAttempts += 1
        resumeWaiters()

        if resetAttempts == 1 {
            throw TestError.failed
        }
    }

    func waitForResetAttempts(_ count: Int) async {
        guard resetAttempts < count else { return }

        await withCheckedContinuation { continuation in
            waiters.append((count, continuation))
        }
    }

    private func resumeWaiters() {
        let ready = waiters.filter { $0.count <= resetAttempts }
        waiters.removeAll { $0.count <= resetAttempts }
        for waiter in ready {
            waiter.continuation.resume()
        }
    }
}

private actor PersistenceGate<Snapshot: Sendable> {
    private(set) var snapshots: [Snapshot?] = []
    private var writes: [Int: CheckedContinuation<Void, Never>] = [:]
    private var waiters:
        [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []

    var writeCount: Int {
        snapshots.count
    }

    func persist(_ snapshot: Snapshot?) async {
        snapshots.append(snapshot)
        let write = snapshots.count
        resumeWaiters()

        await withCheckedContinuation { continuation in
            writes[write] = continuation
        }
    }

    func waitForWrites(_ count: Int) async {
        guard writeCount < count else { return }

        await withCheckedContinuation { continuation in
            waiters.append((count, continuation))
        }
    }

    func complete(write: Int) {
        writes.removeValue(forKey: write)?.resume()
    }

    private func resumeWaiters() {
        let ready = waiters.filter { $0.count <= writeCount }
        waiters.removeAll { $0.count <= writeCount }
        for waiter in ready {
            waiter.continuation.resume()
        }
    }
}

private actor SnapshotGate<Snapshot: Sendable> {
    private(set) var loadCount = 0
    private var loads: [Int: CheckedContinuation<Snapshot, Never>] = [:]
    private var waiters:
        [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []

    func load() async -> Snapshot {
        loadCount += 1
        let load = loadCount
        resumeWaiters()

        return await withCheckedContinuation { continuation in
            loads[load] = continuation
        }
    }

    func waitForLoads(_ count: Int) async {
        guard loadCount < count else { return }

        await withCheckedContinuation { continuation in
            waiters.append((count, continuation))
        }
    }

    func complete(load: Int, with snapshot: Snapshot) {
        loads.removeValue(forKey: load)?.resume(returning: snapshot)
    }

    private func resumeWaiters() {
        let ready = waiters.filter { $0.count <= loadCount }
        waiters.removeAll { $0.count <= loadCount }
        for waiter in ready {
            waiter.continuation.resume()
        }
    }
}
