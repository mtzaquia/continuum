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

/// The observable-source contract implemented by Continuum types.
///
/// Continuum provides conformances for ``Bucket``, ``BucketPartition``, and
/// ``Composition``. Bridge external observable state with
/// ``Input/init(update:load:)`` and asynchronous sequences with
/// ``Input/init(updates:subscriptionOnLoad:load:)``.
@MainActor
public protocol UpdateSource<Snapshot>: AnyObject, Observable {
    associatedtype Snapshot: Sendable

    /// The source's current observable outcome.
    var latest: Update<Snapshot> { get }
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

// A bucket's outcome remains authoritative when its load action throws with data available.
protocol BucketSnapshotSource: UpdateSource {
    var error: (any Error)? { get }
}

extension Bucket: UpdateSource, BucketSnapshotSource where Scope == UnpartitionedBucketScope {}

extension BucketPartition: UpdateSource, BucketSnapshotSource, CompositionResetSource {}
