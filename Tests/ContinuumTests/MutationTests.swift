import Continuum
import Testing

@Suite("Bucket mutations", .timeLimit(.minutes(1)))
struct MutationTests {
    @Test("A typed handler edits loaded snapshots and persists authoritative entries")
    func typedHandler() async throws {
        let service = CardService()
        let cards = Bucket(IndexedKey<Int, Card>("cards")) {
            LocalSource { [Card(id: 1, name: "Old"), Card(id: 2, name: "Other")] } persist: {
                await service.persist($0)
            }
            RemoteSource { [Card]() }
            Mutate { (operation: CardOperation, mutation) in
                switch operation {
                case let .rename(id, name):
                    try await mutation.update(id: id) { card in
                        Card(id: card.id, name: name)
                    } remotely: {
                        await service.rename(id, to: name)
                    }
                case let .terminate(id):
                    try await mutation.update { $0.filter { $0.id != id } } remotely: {
                        await service.terminate(id)
                    }
                case .request:
                    await service.request()
                }
            }
        }

        try await cards.mutate(.rename(1, name: "Unloaded"))
        try await cards.mutate(.request)
        #expect(!cards.isLoaded)
        #expect(await service.events == [.rename(1, "Unloaded"), .request])

        try await cards.load()
        try await cards.mutate(.rename(1, name: "Travel"))
        try await cards.mutate(.terminate(2))
        try await cards.mutate(.request)

        let optimistic = Card(id: 1, name: "Travel")
        let confirmed = Card(id: 1, name: "TRAVEL")
        let sibling = Card(id: 2, name: "Other")
        #expect(cards.values == [confirmed])
        #expect(await service.events == [
            .rename(1, "Unloaded"),
            .request,
            .persist([optimistic, sibling]),
            .rename(1, "Travel"),
            .persist([confirmed, sibling]),
            .persist([confirmed]),
            .terminate(2),
            .request,
        ])
    }

    @Test("Each selected partition owns the handler and its snapshot")
    func partitionedHandler() async throws {
        let bucket = Bucket(Key<String>("labels"), partitionedBy: String.self) { partition in
            RemoteSource { partition }
            Mutate { (operation: LabelOperation, mutation) in
                switch operation {
                case let .append(suffix):
                    try await mutation.update { $0 + suffix }
                }
            }
        }
        try await bucket["first"].load()
        try await bucket["second"].load()

        try await bucket["first"].mutate(.append("!"))
        #expect(bucket["first"].value == "first!")
        #expect(bucket["second"].value == "second")
    }
}

private nonisolated struct Card: Identifiable, Equatable, Sendable {
    let id: Int
    let name: String
}

private nonisolated enum CardOperation: Sendable {
    case rename(Int, name: String)
    case terminate(Int)
    case request
}

private nonisolated enum LabelOperation: Sendable {
    case append(String)
}

private actor CardService {
    enum Event: Equatable, Sendable {
        case persist([Card]?)
        case rename(Int, String)
        case terminate(Int)
        case request
    }

    private(set) var events: [Event] = []

    func persist(_ cards: [Card]?) { events.append(.persist(cards)) }

    func rename(_ id: Int, to name: String) -> Card {
        events.append(.rename(id, name))
        return Card(id: id, name: name.uppercased())
    }

    func terminate(_ id: Int) { events.append(.terminate(id)) }

    func request() { events.append(.request) }
}
