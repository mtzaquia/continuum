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

/// Controls whether a sequence input replaces its subscription when loaded.
public enum InputSubscriptionBehavior: Sendable, Equatable {
    /// Keeps the current subscription, even if it has completed.
    case keep
    /// Cancels the old subscription and creates a new iterator before loading.
    case restart
}

@MainActor
@Observable
final class SequenceInputState<Value: Sendable> {
    var result: Result<Value, any Error>?
    @ObservationIgnored private var task: Task<Void, Never>?
    @ObservationIgnored private var ready: Task<Void, Never>?
    @ObservationIgnored private let start: @MainActor (
        SequenceInputState<Value>, AsyncStream<Void>.Continuation
    ) -> Task<Void, Never>

    init<Source: AsyncSequence & Sendable, Failure: Error>(
        _ factory: @escaping @MainActor () -> Source
    ) where Source.Element == Result<Value, Failure> {
        start = { state, ready in
            Task { @concurrent [weak state] in
                defer { ready.finish() }
                guard !Task.isCancelled else { return }
                let source = await factory()
                var iterator = source.makeAsyncIterator()
                ready.yield(())
                ready.finish()
                do {
                    while !Task.isCancelled, let result = try await iterator.next() {
                        await state?.receive(result.mapError { $0 as any Error })
                    }
                } catch {
                    await state?.receive(.failure(error))
                }
            }
        }
    }

    private func receive(_ result: Result<Value, any Error>) {
        guard !Task.isCancelled else { return }
        self.result = result
    }

    func subscribe() {
        task?.cancel()
        let (stream, continuation) = AsyncStream<Void>.makeStream()
        // A shared readiness task prevents one cancelled load from terminating
        // the readiness stream for other concurrent loads.
        ready = Task { @MainActor in
            for await _ in stream { break }
        }
        task = start(self, continuation)
    }

    func waitUntilSubscribed() async throws {
        let subscription = ready
        await subscription?.value
        try Task.checkCancellation()
        guard ready == subscription else { throw CancellationError() }
    }

    deinit { task?.cancel() }
}
