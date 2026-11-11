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
