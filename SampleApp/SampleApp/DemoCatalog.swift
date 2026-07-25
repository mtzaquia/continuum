import Continuum
import Foundation
import Observation

nonisolated struct Product: Identifiable, Hashable, Sendable {
    let id: Int
    let name: String
    let detail: String
    let price: Int
}

nonisolated enum CatalogData {
    static let products = IndexedKey<Int, Product>("sample.products")
    static let recommendations = IndexedKey<Int, Product>("sample.recommendations")
}

nonisolated enum Shelf: String, CaseIterable, Identifiable, Sendable {
    case home
    case office

    var id: Self { self }
    var title: String { rawValue.capitalized }
}

nonisolated struct DemoActivity: Identifiable, Sendable {
    nonisolated enum Kind: Sendable {
        case request
        case cache
        case remote
        case mutation
        case success
        case failure
    }

    let id = UUID()
    let kind: Kind
    let title: String
    let detail: String
    let date = Date()
}

@Observable
final class DemoCatalog {
    let products: IndexedBucket<Int, Product>
    let recommendations: PartitionedIndexedBucket<Shelf, Int, Product>

    private(set) var activity = [
        DemoActivity(
            kind: .cache,
            title: "Memory cache seeded",
            detail: "Two stale products are ready for the first frame."
        ),
    ]
    private(set) var isFailureQueued = false

    private let remote: DemoRemote

    init() {
        let cache = DemoCache(snapshot: [
            Product(id: 1, name: "Cached notebook", detail: "Available on the first frame", price: 12),
            Product(id: 2, name: "Cached pen", detail: "Replaced by the remote snapshot", price: 4),
        ])
        let remote = DemoRemote()
        self.remote = remote

        products = Bucket(CatalogData.products) {
            LocalSource {
                await cache.load()
            } persist: { snapshot in
                await cache.persist(snapshot)
            }
            RemoteSource {
                Load {
                    let page = try await remote.products(after: nil)
                    return Page(values: page.values, next: page.next)
                }
                NextPage { cursor in
                    let page = try await remote.products(after: cursor)
                    return Page(values: page.values, next: page.next)
                }
                Store { product in
                    try await remote.store(product)
                }
                Remove { id in
                    try await remote.remove(id: id)
                }
            }
        }

        recommendations = Bucket(
            CatalogData.recommendations,
            partitionedBy: Shelf.self
        ) { shelf in
            RemoteSource {
                try await remote.recommendations(for: shelf)
            }
        }
    }

    func loadCatalog() async {
        record(
            .request,
            title: "Refresh requested",
            detail: "cachedThenRemote publishes cache, then requires a fresh remote snapshot."
        )
        do {
            try await products.load(using: .cachedThenRemote)
            isFailureQueued = false
            record(
                .success,
                title: "Remote snapshot established",
                detail: "\(products.count) products were persisted locally before entering memory."
            )
        } catch {
            isFailureQueued = false
            record(
                .failure,
                title: "Refresh failed safely",
                detail: "The established snapshot stayed visible while the bucket exposed the error."
            )
        }
    }

    func loadNextPage() async {
        record(
            .remote,
            title: "Next page requested",
            detail: "Continuum is following the cursor while preserving the established snapshot."
        )
        do {
            try await products.loadNext()
            record(
                .success,
                title: "Page merged",
                detail: "The ordered snapshot now contains \(products.count) products."
            )
        } catch {
            record(
                .failure,
                title: "Page request failed",
                detail: "The continuation error is separate from the established snapshot."
            )
        }
    }

    func addProduct() async {
        let product = Product(
            id: Int.random(in: 100...999),
            name: "A new server item",
            detail: "Stored remotely, persisted locally, then published",
            price: 18
        )
        record(
            .mutation,
            title: "Store mutation started",
            detail: "Remote → local persistence → observable memory."
        )
        do {
            try await products.store(product)
            record(
                .success,
                title: "Server item stored",
                detail: "The authoritative value is now part of the atomic snapshot."
            )
        } catch {
            record(
                .failure,
                title: "Store mutation failed",
                detail: "Memory was left unchanged."
            )
        }
    }

    func remove(_ product: Product) async {
        record(
            .mutation,
            title: "Remove mutation started",
            detail: "\(product.name) will be removed remotely before local publication."
        )
        do {
            try await products.remove(product.id)
            record(
                .success,
                title: "Product removed",
                detail: "The normalized snapshot now contains \(products.count) products."
            )
        } catch {
            record(
                .failure,
                title: "Remove mutation failed",
                detail: "The established snapshot was preserved."
            )
        }
    }

    func loadRecommendations(for shelf: Shelf) async {
        record(
            .remote,
            title: "\(shelf.title) partition requested",
            detail: "This shelf owns loading state and values independently."
        )
        do {
            try await recommendations[shelf].load()
            record(
                .success,
                title: "\(shelf.title) partition established",
                detail: "Switch shelves to see that the other partition remains untouched."
            )
        } catch {
            record(
                .failure,
                title: "\(shelf.title) partition failed",
                detail: "Failures remain scoped to the selected partition."
            )
        }
    }

    func failNextCatalogRequest() async {
        await remote.failNextCatalogRequest()
        isFailureQueued = true
        record(
            .failure,
            title: "Failure queued",
            detail: "Tap Refresh to observe stale-while-error behavior."
        )
    }

    func resetCatalog() {
        products.reset()
        record(
            .cache,
            title: "Memory reset",
            detail: "The persisted cache still exists. Refresh to watch it return."
        )
    }

    private func record(
        _ kind: DemoActivity.Kind,
        title: String,
        detail: String
    ) {
        activity.insert(
            DemoActivity(kind: kind, title: title, detail: detail),
            at: 0
        )
        activity = Array(activity.prefix(8))
    }
}

private actor DemoCache {
    private var snapshot: [Product]?

    init(snapshot: [Product]?) {
        self.snapshot = snapshot
    }

    func load() -> [Product]? { snapshot }
    func persist(_ snapshot: [Product]?) { self.snapshot = snapshot }
}

private actor DemoRemote {
    private var shouldFailNextCatalogRequest = false
    private var products = [
        Product(id: 1, name: "Canvas notebook", detail: "Remote replacement for cached content", price: 14),
        Product(id: 3, name: "Desk organizer", detail: "First remote page", price: 22),
        Product(id: 4, name: "Mechanical pencil", detail: "First remote page", price: 7),
        Product(id: 5, name: "Paper clips", detail: "Second remote page", price: 3),
        Product(id: 6, name: "Weekly planner", detail: "Second remote page", price: 16),
    ]

    func products(after page: Int?) async throws -> (values: [Product], next: Int?) {
        try await pause()
        if shouldFailNextCatalogRequest {
            shouldFailNextCatalogRequest = false
            throw DemoFailure.offline
        }

        switch page {
        case nil:
            return (Array(products.prefix(3)), 1)
        default:
            return (Array(products.dropFirst(3)), nil)
        }
    }

    func store(_ product: Product) async throws -> Product {
        try await pause()
        products.removeAll { $0.id == product.id }
        products.insert(product, at: 0)
        return product
    }

    func remove(id: Int) async throws {
        try await pause()
        products.removeAll { $0.id == id }
    }

    func recommendations(for shelf: Shelf) async throws -> [Product] {
        try await pause()
        switch shelf {
        case .home:
            return [Product(id: 20, name: "Reading lamp", detail: "Home recommendation", price: 31)]
        case .office:
            return [Product(id: 30, name: "Monitor stand", detail: "Office recommendation", price: 42)]
        }
    }

    func failNextCatalogRequest() { shouldFailNextCatalogRequest = true }

    private func pause() async throws {
        try await Task.sleep(for: .milliseconds(450))
    }
}

nonisolated enum DemoFailure: LocalizedError, Sendable {
    case offline

    var errorDescription: String? {
        "The simulated remote service is offline. The established snapshot remains available."
    }
}
