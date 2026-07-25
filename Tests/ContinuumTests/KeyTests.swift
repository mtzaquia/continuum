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
        struct Post: Sendable {
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
}
