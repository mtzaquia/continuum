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

private typealias PersistenceOperation<Space: ContinuumKeySpace> =
    @Sendable (Space.Snapshot?) async throws -> Void

actor LocalPersistenceCoordinator<Space: ContinuumKeySpace> {
    private let operations: [PersistenceOperation<Space>]
    private let logIdentity: BucketLogIdentity

    init(
        sources: [LocalSource<Space>],
        logIdentity: BucketLogIdentity
    ) {
        operations = sources.compactMap(\.persistence)
        self.logIdentity = logIdentity
    }

    func persist(_ snapshot: Space.Snapshot?) async throws {
        guard operations.isEmpty == false else {
            try Task.checkCancellation()
            return
        }

        continuumDebug(
            .persistenceStarted(
                logIdentity,
                destinations: operations.count
            )
        )
        for operation in operations {
            try Task.checkCancellation()
            try await operation(snapshot)
        }
        try Task.checkCancellation()
        continuumDebug(
            .persistenceCompleted(
                logIdentity,
                destinations: operations.count
            )
        )
    }
}
