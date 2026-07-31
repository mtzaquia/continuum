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

/// A coherent result or unavailable state published by a bucket.
nonisolated public enum BucketUpdate<Snapshot: Sendable>: Sendable {
    /// The bucket established a snapshot or reported an error.
    case result(Result<Snapshot, any Error>)

    /// The observed state is unavailable.
    case reset
}

/// A bucket or selected partition that can participate in aggregate updates.
///
/// Continuum provides conformances for ``Bucket`` and ``BucketPartition``.
@MainActor
public protocol BucketUpdateSource<Snapshot>: AnyObject {
    associatedtype Snapshot: Sendable

    /// Returns the source's coherent state for aggregate observation.
    ///
    /// Consume ``Bucket/updates()`` or use
    /// ``bucketUpdates(observing:transform:)`` instead of calling this
    /// infrastructure method directly.
    func _latestUpdateForObservation() -> BucketUpdate<Snapshot>
}

/// Observes buckets and transforms their successful snapshots.
///
/// The tuple can freely mix unpartitioned buckets and selected partitions. The
/// sequence remains silent until every source has a successful snapshot, unless
/// a source reports an error. After emitting a result, it emits one reset when a
/// source becomes unavailable. Initial and repeated unavailable states remain
/// silent.
///
/// Errors take precedence over unavailable sources. By default, the first
/// failure in tuple order is emitted. Use `mapFailures` to combine every current
/// dependency failure into one error. The transform runs only when every source
/// has a successful snapshot, and a thrown transform error is emitted directly.
///
/// - Parameters:
///   - sources: The buckets and selected partitions the transform requires.
///   - mapFailures: The error produced from every currently failing dependency,
///     in tuple order.
///   - transform: The value produced from the successful snapshots.
/// - Returns: An independent update stream that ends when iteration is
///   cancelled.
public func bucketUpdates<
    each Source: BucketUpdateSource,
    Output: Sendable
>(
    observing sources: (repeat each Source),
    mapFailures: @escaping @MainActor ([any Error]) -> any Error = { $0[0] },
    transform: @escaping @MainActor
        (repeat (each Source).Snapshot) throws -> Output
) -> AsyncStream<BucketUpdate<Output>> {
    makeBucketUpdates { id, changes in
        let updates = withObservationTracking {
            (repeat (each sources)._latestUpdateForObservation())
        } onChange: {
            changes.yield(id)
        }
        var failures: [any Error] = []
        var hasReset = false

        for update in repeat each updates {
            switch update {
            case .result(.failure(let error)):
                failures.append(error)
            case .reset:
                hasReset = true
            case .result(.success):
                break
            }
        }

        if failures.isEmpty == false {
            return .result(.failure(mapFailures(failures)))
        }
        if hasReset {
            return .reset
        }

        do {
            return .result(
                .success(
                    try transform(
                        repeat successfulSnapshot(from: each updates)
                    )
                )
            )
        } catch {
            return .result(.failure(error))
        }
    }
}

/// Observes one bucket or selected partition and transforms its successful
/// snapshot.
///
/// Delivery and failure behavior matches the tuple overload.
///
/// - Parameters:
///   - source: The bucket or selected partition the transform requires.
///   - mapFailures: The error produced from the source failure.
///   - transform: The value produced from the successful snapshot.
/// - Returns: An independent update stream that ends when iteration is
///   cancelled.
public func bucketUpdates<
    Source: BucketUpdateSource,
    Output: Sendable
>(
    observing source: Source,
    mapFailures: @escaping @MainActor ([any Error]) -> any Error = { $0[0] },
    transform: @escaping @MainActor (Source.Snapshot) throws -> Output
) -> AsyncStream<BucketUpdate<Output>> {
    makeBucketUpdates { id, changes in
        let update = withObservationTracking {
            source._latestUpdateForObservation()
        } onChange: {
            changes.yield(id)
        }

        switch update {
        case .result(.success(let snapshot)):
            do {
                return .result(.success(try transform(snapshot)))
            } catch {
                return .result(.failure(error))
            }
        case .result(.failure(let error)):
            return .result(.failure(mapFailures([error])))
        case .reset:
            return .reset
        }
    }
}

private func makeBucketUpdates<Output: Sendable>(
    evaluate: @escaping @MainActor (
        _ id: UInt,
        _ changes: AsyncStream<UInt>.Continuation
    ) -> BucketUpdate<Output>
) -> AsyncStream<BucketUpdate<Output>> {
    let (stream, continuation) = AsyncStream<BucketUpdate<Output>>.makeStream(
        bufferingPolicy: .unbounded
    )
    let producer = Task { @MainActor in
        let (changes, changeContinuation) = AsyncStream<UInt>.makeStream(
            bufferingPolicy: .unbounded
        )
        var changeIterator = changes.makeAsyncIterator()
        var observationID: UInt = 0
        var emittedResult = false

        defer {
            changeContinuation.finish()
            continuation.finish()
        }

        while Task.isCancelled == false {
            observationID &+= 1
            var observedID = observationID
            var update = evaluate(observedID, changeContinuation)

            if case .reset = update, emittedResult {
                await Task.yield()
                observationID &+= 1
                observedID = observationID
                update = evaluate(observedID, changeContinuation)
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
    return stream
}

extension Bucket: BucketUpdateSource
where Scope == UnpartitionedBucketScope {
    public func _latestUpdateForObservation() -> BucketUpdate<Space.Snapshot> {
        latestUpdate
    }
}

extension BucketPartition: BucketUpdateSource {
    public func _latestUpdateForObservation() -> BucketUpdate<Space.Snapshot> {
        latestUpdate
    }
}

nonisolated private func successfulSnapshot<Snapshot: Sendable>(
    from update: BucketUpdate<Snapshot>
) -> Snapshot {
    guard case .result(.success(let snapshot)) = update else {
        preconditionFailure("Bucket updates must be successful before unwrapping")
    }
    return snapshot
}
