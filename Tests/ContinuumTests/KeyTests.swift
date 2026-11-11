import Continuum
import Testing

@Suite("Keys")
struct KeyTests {
    @Test("Singleton keys use namespace and version identity")
    func singletonIdentity() {
        let first = Key<Int>("settings.launch-count")
        let same = Key<Int>("settings.launch-count")
        let migrated = Key<Int>("settings.launch-count", version: 2)

        #expect(first == same)
        #expect(first != migrated)
    }

    @Test("Indexed keys retain their typed index and value contract")
    func indexedIdentity() {
        nonisolated struct Post: Sendable {
            let id: Int
        }

        let first = IndexedKey<Int, Post>("posts", indexedBy: \.id)
        let same = IndexedKey<Int, Post>("posts", indexedBy: \.id)
        let other = IndexedKey<Int, Post>("comments", indexedBy: \.id)

        #expect(first == same)
        #expect(first != other)
    }

    @Test("Identifiable values use their id by default")
    func identifiableIndex() {
        nonisolated struct Post: Identifiable, Sendable {
            let id: Int
        }

        let posts = IndexedKey<Post.ID, Post>("posts", version: 2)
        let post = Post(id: 42)

        #expect(posts.namespace == "posts")
        #expect(posts.version == 2)
        #expect(posts.input(for: post) == post.id)
        #expect(posts.value(for: post.id, in: [post])?.id == post.id)
    }

    @Test("Repository requirements use bucket aliases and infer key types at construction")
    func repositoryAliases() async throws {
        let repository: any BucketRepository = AliasRepository()
        #expect(try await repository.options.load() == true)
        #expect(try await repository.optionsByRegion["eu"].load() == ["eu"])
        _ = try await repository.cards.load()
        #expect(repository.cards[1]?.code == "standard")
        _ = try await repository.cardsByRegion["eu"].load()
        #expect(repository.cardsByRegion["eu"][2]?.code == "eu")
        _ = try await repository.cardsByCode.load()
        #expect(repository.cardsByCode["standard"]?.id == 1)
        _ = try await repository.itemsByRegion["eu"].load()
        #expect(repository.itemsByRegion["eu"]["eu"]?.code == "eu")
    }

    @Test("Typed bucket aliases preserve operations in repository requirements and partitions")
    func mutableRepositoryAliases() async throws {
        let repository: any MutableBucketRepository = MutableAliasRepository()
        let indexed: PartitionedMutableIndexedBucket<String, Int, Card, CardOperation> =
            repository.cardsByRegion
        let cards: MutableBucket<IndexedKey<Int, Card>, CardOperation> = repository.cards

        try await cards.load()
        try await indexed["eu"].load()
        try await indexed["us"].load()
        try await repository.option.load()
        try await repository.optionsByRegion["eu"].load()

        try await cards.mutate(.clear)
        try await indexed["eu"].mutate(.clear)
        try await repository.option.mutate(.set(true))
        try await repository.optionsByRegion["eu"].mutate(.set(true))

        #expect(cards.isLoaded)
        #expect(cards.isEmpty)
        #expect(indexed["eu"].isEmpty)
        #expect(indexed["us"][1]?.code == "us")
        #expect(repository.option.value == true)
        #expect(repository.optionsByRegion["eu"].value == true)
    }
}

private nonisolated struct Card: Identifiable, Sendable {
    let id: Int
    let code: String
}

private nonisolated struct Item: Sendable {
    let code: String
}

@MainActor
private protocol BucketRepository {
    var options: ValueBucket<Bool> { get }
    var optionsByRegion: PartitionedValueBucket<String, [String]> { get }
    var cards: IdentifiedBucket<Card> { get }
    var cardsByRegion: PartitionedIdentifiedBucket<String, Card> { get }
    var cardsByCode: IndexedBucket<String, Card> { get }
    var itemsByRegion: PartitionedIndexedBucket<String, String, Item> { get }
}

@MainActor
private struct AliasRepository: BucketRepository {
    let options: ValueBucket<Bool> = Bucket(Key("options")) {
        RemoteSource { true }
    }
    let optionsByRegion: PartitionedValueBucket<String, [String]> = Bucket(
        Key("options-by-region"), partitionedBy: String.self
    ) { region in
        RemoteSource { [region] }
    }
    let cards: IdentifiedBucket<Card> = Bucket(IndexedKey("cards")) {
        RemoteSource { [Card(id: 1, code: "standard")] }
    }
    let cardsByRegion: PartitionedIdentifiedBucket<String, Card> = Bucket(
        IndexedKey("cards-by-region"), partitionedBy: String.self
    ) { region in
        RemoteSource { [Card(id: 2, code: region)] }
    }
    let cardsByCode: IndexedBucket<String, Card> = Bucket(IndexedKey("cards-by-code", indexedBy: \.code)) {
        RemoteSource { [Card(id: 1, code: "standard")] }
    }
    let itemsByRegion: PartitionedIndexedBucket<String, String, Item> = Bucket(
        IndexedKey("items-by-region", indexedBy: { $0.code }), partitionedBy: String.self
    ) { region in
        RemoteSource { [Item(code: region)] }
    }
}

private nonisolated enum CardOperation: Sendable { case clear }
private nonisolated enum OptionOperation: Sendable { case set(Bool) }

@MainActor
private protocol MutableBucketRepository {
    var cards: MutableIndexedBucket<Int, Card, CardOperation> { get }
    var cardsByRegion: PartitionedMutableBucket<String, IndexedKey<Int, Card>, CardOperation> { get }
    var option: MutableValueBucket<Bool, OptionOperation> { get }
    var optionsByRegion: PartitionedMutableValueBucket<String, Bool, OptionOperation> { get }
}

@MainActor
private struct MutableAliasRepository: MutableBucketRepository {
    let cards: MutableIdentifiedBucket<Card, CardOperation> = Bucket(IndexedKey("cards")) {
        RemoteSource { [Card(id: 1, code: "standard")] }
        Mutate { (_: CardOperation, mutation) in try await mutation.update { _ in [] } }
    }
    let cardsByRegion: PartitionedMutableIdentifiedBucket<String, Card, CardOperation> = Bucket(
        IndexedKey("cards-by-region"), partitionedBy: String.self
    ) { region in
        RemoteSource { [Card(id: 1, code: region)] }
        Mutate { (_: CardOperation, mutation) in try await mutation.update { _ in [] } }
    }
    let option: MutableValueBucket<Bool, OptionOperation> = Bucket(Key("option")) {
        RemoteSource { false }
        Mutate { (operation: OptionOperation, mutation) in
            switch operation {
            case let .set(value): try await mutation.update { _ in value }
            }
        }
    }
    let optionsByRegion: PartitionedMutableValueBucket<String, Bool, OptionOperation> = Bucket(
        Key("options-by-region"), partitionedBy: String.self
    ) { _ in
        RemoteSource { false }
        Mutate { (operation: OptionOperation, mutation) in
            switch operation {
            case let .set(value): try await mutation.update { _ in value }
            }
        }
    }
}
