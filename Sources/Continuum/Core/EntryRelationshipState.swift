//
//  Copyright (c) 2026 @mtzaquia
//
//  Permission is hereby granted, free of charge, to any person obtaining a copy
//  of this software and associated documentation files (the "Software"), to deal
//  in the Software without restriction, including without limitation the rights
//  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//  copies of the Software, and to permit persons to whom the Software is
//  furnished to do so, subject to the following conditions:
//
//  The above copyright notice and this permission notice shall be included in all
//  copies or substantial portions of the Software.
//
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//  SOFTWARE.
//

import Observation

/// An entry's returned value belongs to the relationship, not to the collection.
@MainActor
@Observable
final class EntryRelationshipState<ID: Hashable & Sendable, Value: Sendable> {
    private let partition: BucketPartition<IndexedKey<ID, Value>>
    private let id: ID
    @ObservationIgnored private var lastObserved: (revision: UInt, value: Value)?
    private var returned: (revision: UInt, result: Result<Value, any Error>)?

    init(partition: BucketPartition<IndexedKey<ID, Value>>, id: ID) {
        self.partition = partition
        self.id = id
    }

    func read() -> CompositionInputState<Value> {
        let revision = partition.compositionResetRevision
        let result = returned?.revision == revision ? returned?.result : nil
        if let current = partition[id] {
            lastObserved = (revision, current)
        }
        if case .failure(let error) = result {
            return .init(value: nil, unavailable: false, failures: [error], resetRevision: revision)
        }
        let retained = lastObserved?.revision == revision ? lastObserved?.value : nil
        let value = retained ?? (try? result?.get())
        return .init(value: value, unavailable: value == nil, failures: [], resetRevision: revision)
    }

    func load(_ policy: LoadPolicy) async throws -> Value {
        let revision = partition.compositionResetRevision
        do {
            let value = try await partition.load(id: id, using: policy)
            try Task.checkCancellation()
            guard partition.compositionResetRevision == revision else { throw CancellationError() }
            lastObserved = nil
            returned = (revision, .success(value))
            return value
        } catch {
            if !(error is CancellationError), partition.compositionResetRevision == revision {
                returned = (revision, .failure(error))
            }
            throw error
        }
    }
}
