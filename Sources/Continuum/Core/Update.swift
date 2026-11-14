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

/// A coherent result or unavailable state published by a bucket or composition.
nonisolated public enum Update<Snapshot: Sendable>: Sendable {
    /// An available snapshot or a failure of the source's current outcome.
    case result(Result<Snapshot, any Error>)

    /// The observed state is unavailable.
    case reset
}

extension Update {
    var snapshot: Snapshot? {
        guard case .result(.success(let value)) = self else { return nil }
        return .some(value)
    }
}

/// An observable snapshot and its source-owned error.
@MainActor
public protocol UpdateSource<Snapshot>: AnyObject, Observable {
    associatedtype Snapshot: Sendable

    /// The current snapshot, failure, or unavailability.
    var latest: Update<Snapshot> { get }
    /// An operation error that can coexist with an available snapshot.
    var error: (any Error)? { get }
}

public extension UpdateSource {
    var error: (any Error)? { nil }
}

// Keeps invalidation visible when Observation coalesces a reset and its replacement.
@MainActor
protocol CompositionResetSource: AnyObject {
    var resetRevision: UInt { get }
}

/// A source that can participate in a composition load. Publish failures through
/// `latest` or `error` so all observers see the same outcome.
@MainActor
public protocol LoadableUpdateSource<Snapshot>: UpdateSource, Sendable {
    /// The value returned to direct callers of a load.
    associatedtype LoadResult

    /// Loads using the requested policy and publishes the outcome before returning.
    @discardableResult
    func load(using policy: LoadPolicy) async throws -> LoadResult
}

func makeSourceObservation<Source: UpdateSource & Sendable>(
    _ source: Source
) -> (stream: AsyncStream<Update<Source.Snapshot>>, cancel: @Sendable () -> Void) {
    let (stream, continuation) = AsyncStream<Update<Source.Snapshot>>.makeStream(
        bufferingPolicy: .unbounded
    )
    let producer = Task { @MainActor in
        let (changes, changeContinuation) = AsyncStream<UInt>.makeStream(
            bufferingPolicy: .unbounded
        )
        var changeIterator = changes.makeAsyncIterator()
        var observationID: UInt = 0
        var emittedResult = false

        @MainActor func read(_ id: UInt) -> Update<Source.Snapshot> {
            withObservationTracking {
                source.latest
            } onChange: {
                changeContinuation.yield(id)
            }
        }

        defer {
            changeContinuation.finish()
            continuation.finish()
        }

        while Task.isCancelled == false {
            observationID &+= 1
            var observedID = observationID
            var update = read(observedID)

            if case .reset = update, emittedResult {
                await Task.yield()
                observationID &+= 1
                observedID = observationID
                update = read(observedID)
            }

            switch update {
            case .result:
                emittedResult = true
                if case .terminated = continuation.yield(update) {
                    return
                }
            case .reset where emittedResult:
                emittedResult = false
                if case .terminated = continuation.yield(.reset) {
                    return
                }
            case .reset:
                break
            }

            var observedChange = false
            while let changedObservationID = await changeIterator.next() {
                if changedObservationID >= observedID {
                    observedChange = true
                    break
                }
            }
            guard observedChange else { return }
        }
    }
    continuation.onTermination = { @Sendable _ in
        producer.cancel()
    }
    return (stream, {
        continuation.finish()
        producer.cancel()
    })
}

extension Bucket: LoadableUpdateSource, CompositionResetSource {}
