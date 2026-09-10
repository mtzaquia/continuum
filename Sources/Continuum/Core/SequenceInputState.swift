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
    @ObservationIgnored private(set) var generation: UInt = 0
    @ObservationIgnored private var task: Task<Void, Never>?
    @ObservationIgnored private var ready: Task<Void, Never>?
    @ObservationIgnored private let start: @MainActor (
        SequenceInputState<Value>, UInt, AsyncStream<Void>.Continuation
    ) -> Task<Void, Never>

    init<Source: AsyncSequence, Failure: Error>(
        _ factory: @escaping @MainActor () -> Source
    ) where Source.Element == Result<Value, Failure>,
          Source.AsyncIterator: SendableMetatype {
        start = { state, generation, ready in
            Task { @MainActor [weak state] in
                defer { ready.finish() }
                guard !Task.isCancelled else { return }
                var iterator = factory().makeAsyncIterator()
                ready.yield(())
                ready.finish()
                do {
                    while !Task.isCancelled, let result = try await iterator.next() {
                        guard !Task.isCancelled, let state,
                              state.generation == generation else { return }
                        state.result = result.mapError { $0 as any Error }
                    }
                } catch {
                    guard !Task.isCancelled, let state,
                          state.generation == generation else { return }
                    state.result = .failure(error)
                }
            }
        }
    }

    func subscribe() {
        generation &+= 1
        task?.cancel()
        let (stream, continuation) = AsyncStream<Void>.makeStream()
        // A shared readiness task prevents one cancelled load from terminating
        // the readiness stream for other concurrent loads.
        ready = Task { @MainActor in
            for await _ in stream { break }
        }
        task = start(self, generation, continuation)
    }

    func waitUntilSubscribed() async {
        await ready?.value
    }

    deinit { task?.cancel() }
}
