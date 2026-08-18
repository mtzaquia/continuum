import Continuum
import Testing

@Suite("Isolation")
nonisolated struct IsolationTests {
    @Test("Value and source APIs remain usable away from the main actor")
    func nonisolatedValues() async {
        let source = LocalSource<Key<Int>> { 42 }
        let (state, pagination) = await MainActor.run {
            let bucket = Bucket(Key<Int>("isolation"))
            return (bucket.state, bucket.pagination)
        }

        #expect([source].count == 1)
        #expect(state.isLoading == false)
        #expect(state.isLoaded == false)
        #expect(pagination.isLoading == false)
        #expect(pagination.hasNextPage == false)
    }
}
