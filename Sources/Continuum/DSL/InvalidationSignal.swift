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

/// An application event stream that invalidates one atomic bucket snapshot.
///
/// Every event performs ``BucketPartition/reset()``. It immediately clears
/// observable memory and pagination, then sends `nil` to every writable
/// ``LocalSource``. This prevents a later cached load from restoring a
/// snapshot that the event declared stale.
///
/// A persistence failure restores the previous snapshot and becomes the
/// bucket's error. The bucket owns the observation task for its lifetime. A
/// partitioned bucket starts one observation when each accessed partition is
/// created.
///
/// ```swift
/// InvalidationSignal {
///     NotificationCenter.default.notifications(
///         named: .accountDidChange
///     )
/// }
/// ```
public struct InvalidationSignal: Sendable {
    let observation:
        @Sendable (
            @escaping @MainActor @Sendable () async -> Void,
            @escaping @MainActor @Sendable (any Error) async -> Void
        ) async -> Void

    /// Creates an invalidation signal from an asynchronous sequence.
    ///
    /// - Parameter events: A sendable operation creating a sendable event
    ///   sequence when the bucket starts observing it. Each event clears
    ///   observable memory and writable local snapshots. A sequence failure
    ///   becomes the bucket's observable error and ends this observation.
    public init<
        Events: AsyncSequence & Sendable & SendableMetatype
    >(
        _ events: @escaping @Sendable () -> Events
    ) where Events.AsyncIterator: SendableMetatype {
        observation = { invalidate, fail in
            do {
                for try await _ in events() {
                    guard Task.isCancelled == false else { return }
                    await invalidate()
                }
            } catch is CancellationError {
                return
            } catch {
                await fail(error)
            }
        }
    }
}
