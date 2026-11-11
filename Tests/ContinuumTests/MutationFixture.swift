@testable import Continuum

extension Bucket where Scope == UnpartitionedBucketScope, Operation == MutationAction<Space> {
    func put(_ value: Space.Value) async throws {
        try await storage.put(value)
    }

    func delete(_ input: Space.Input) async throws {
        try await storage.delete(input)
    }

    func delete() async throws where Space.Input == SingletonInput {
        try await storage.delete()
    }
}

extension BucketPartition where Operation == MutationAction<Space> {
    func put(_ value: Space.Value) async throws {
        let key = keySpace
        try await mutate { mutation in
            try await mutation.replace(with: key.storing(value, in: mutation.snapshot))
        }
    }

    func delete(_ input: Space.Input) async throws {
        let key = keySpace
        try await mutate { mutation in
            try await mutation.update { key.removing(input, from: $0) }
        }
    }

    func delete() async throws where Space.Input == SingletonInput {
        try await mutate { try await $0.replace(with: nil) }
    }
}
