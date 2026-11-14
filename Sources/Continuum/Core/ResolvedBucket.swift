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

import Foundation
import Observation

/// A root collection and its resolved entries, published together.
public struct ResolvedSnapshot<Element: Sendable, ID: Hashable & Sendable, Related: Sendable>: Sendable {
    /// Root entries in snapshot order.
    public let root: [Element]
    /// The related entry for each distinct ID in the root.
    public let related: [ID: Related]
}

/// Resolves an array snapshot against an indexed bucket on the main actor.
///
/// Create this source with `bucket.resolving(_:from:)` and pass it to
/// ``Input`` to compose the result. Load pages and reset data on the
/// underlying buckets.
@MainActor
@Observable
public final class ResolvedBucket<Element: Sendable, ID: Hashable & Sendable, Related: Sendable>: UpdateSource {
    /// The root array and related entries published together.
    public typealias Snapshot = ResolvedSnapshot<Element, ID, Related>

    /// The last complete pair, an initial failure, or `.reset` when unavailable.
    public private(set) var latest: Update<Snapshot> = .reset

    /// The current resolution or root load error, including while a retry is in progress.
    public var error: (any Error)? { resolutionError ?? root.error }

    /// Whether lookups for the current root are in progress.
    public private(set) var isResolving = false

    private var resolutionError: (any Error)?
    private(set) var resetRevision: UInt = 0
    @ObservationIgnored private let root: any LoadableUpdateSource<[Element]>
    @ObservationIgnored private let rootReset: any CompositionResetSource
    @ObservationIgnored private let key: KeyPath<Element, ID>
    @ObservationIgnored private let readRelated: @MainActor () -> (entries: [ID: Related], revision: UInt)
    @ObservationIgnored private let loadEntry: @MainActor @Sendable (ID) async throws -> Related
    @ObservationIgnored private let observation = ObservationSubscription()
    private enum Lookup {
        case pending(UUID, Task<Void, Never>)
        case resolved(Related)
        case failed((any Error)?)

        func cancel() {
            if case .pending(_, let task) = self { task.cancel() }
        }
    }

    @ObservationIgnored private var lookups: [ID: Lookup] = [:]
    @ObservationIgnored private var waiters: [UUID: AsyncStream<Void>.Continuation] = [:]
    @ObservationIgnored private var previousRootReset: UInt
    @ObservationIgnored private var previousRelatedReset: UInt
    @ObservationIgnored private var waitsAfterRelatedReset = false
    @ObservationIgnored private var rootLoad: UUID?

    fileprivate init<Space: ContinuumKeySpace, Operation, RelatedOperation>(
        root: Bucket<Space, Operation>,
        key: KeyPath<Element, ID>,
        related: Bucket<IndexedKey<ID, Related>, RelatedOperation>
    ) where Space.Snapshot == [Element] {
        self.root = root
        rootReset = root
        self.key = key
        readRelated = {
            let entries = Dictionary(uniqueKeysWithValues: related.values.map {
                (related.keySpace.input(for: $0), $0)
            })
            return (entries, related.resetRevision)
        }
        loadEntry = { try await related.load(id: $0, using: .cached) }
        previousRootReset = root.resetRevision
        previousRelatedReset = related.resetRevision
        reconcile()
    }

    /// Loads the root if needed and waits for related lookups to settle.
    ///
    /// `.cached` reuses an available root. The other policies load the root
    /// before resolving missing entries. All policies reuse available related
    /// entries. Retry failed lookups by calling this method again.
    ///
    /// - Parameter policy: The policy used when a root load is needed.
    public func load(using policy: LoadPolicy = .cached) async {
        await SourceLoadContext.load(self, using: policy)
    }

    func load(in context: SourceLoadContext) async {
        guard !Task.isCancelled else { return }
        let revision = rootReset.resetRevision
        let relatedRevision = readRelated().revision
        if context.policy != .cached || root.latest.snapshot == nil {
            let request = UUID()
            rootLoad = request
            await context.load(root)
            if rootLoad == request { rootLoad = nil }
        }
        guard rootReset.resetRevision == revision, readRelated().revision == relatedRevision else {
            reconcile()
            return
        }
        reconcile(explicit: !Task.isCancelled)
        guard !Task.isCancelled, rootLoad != nil || isResolving else { return }

        // Wait for the current snapshot to settle, even if a page replaces it
        // while lookups are in progress. Cancellation ends only this wait.
        let id = UUID()
        let (stream, continuation) = AsyncStream<Void>.makeStream()
        waiters[id] = continuation
        defer { waiters[id] = nil }
        for await _ in stream {}
    }

    private func reconcile(explicit: Bool = false) {
        let (update, rootRevision, related) = observation.track {
            (root.latest, rootReset.resetRevision, readRelated())
        } onChange: { [weak self] in
            self?.reconcile()
        }

        let relatedWasReset = related.revision != previousRelatedReset
        if rootRevision != previousRootReset || relatedWasReset {
            clear()
        }
        if relatedWasReset { waitsAfterRelatedReset = true }
        if explicit { waitsAfterRelatedReset = false }
        previousRootReset = rootRevision
        previousRelatedReset = related.revision

        switch update {
        case .reset:
            if case .result = latest { clear() }
        case .result(.failure(let error)):
            resolutionError = nil
            latest = .result(.failure(error))
            finishWaiting()
        case .result(.success(let elements)):
            guard rootLoad == nil else { return }
            resolve(elements, available: related.entries, explicit: explicit)
        }
    }

    private func resolve(_ elements: [Element], available: [ID: Related], explicit: Bool) {
        var seen: Set<ID> = []
        let ids = elements.map { $0[keyPath: key] }.filter { seen.insert($0).inserted }
        for id in Array(lookups.keys) where !seen.contains(id) || available[id] != nil {
            lookups.removeValue(forKey: id)?.cancel()
        }

        var entries: [ID: Related] = [:]
        var firstError: (any Error)?
        var pending = false
        for id in ids {
            if let local = available[id] {
                entries[id] = local
                continue
            }
            switch lookups[id] {
            case .resolved(let value):
                entries[id] = value
            case .pending:
                pending = true
            case .failed(let error) where !explicit:
                if firstError == nil { firstError = error }
            default:
                if !waitsAfterRelatedReset {
                    startLookup(id)
                    pending = true
                }
            }
        }

        isResolving = pending
        guard !pending else { return }
        if let firstError {
            resolutionError = firstError
            if latest.snapshot == nil { latest = .result(.failure(firstError)) }
        } else if entries.count == ids.count {
            waitsAfterRelatedReset = false
            resolutionError = nil
            latest = .result(.success(.init(root: elements, related: entries)))
        }
        finishWaiting()
    }

    private func startLookup(_ id: ID) {
        let request = UUID()
        let loadEntry = loadEntry
        let task = Task { @MainActor [weak self] in
            let result: Lookup
            do {
                result = .resolved(try await loadEntry(id))
            } catch {
                result = .failed(error is CancellationError ? nil : error)
            }
            guard let self, !Task.isCancelled,
                  case .pending(let current, _) = lookups[id], current == request else { return }
            lookups[id] = result
            reconcile()
        }
        lookups[id] = .pending(request, task)
    }

    private func finishWaiting() {
        for waiter in waiters.values { waiter.finish() }
        waiters.removeAll()
    }

    private func clear() {
        for lookup in lookups.values { lookup.cancel() }
        lookups.removeAll()
        isResolving = false
        resolutionError = nil
        resetRevision &+= 1
        latest = .reset
        finishWaiting()
    }

    deinit {
        for lookup in lookups.values { lookup.cancel() }
    }
}

extension ResolvedBucket: LoadableUpdateSource, CompositionResetSource {}

public extension Bucket {
    /// Observes this collection and looks up each distinct related ID as needed.
    ///
    /// Resolution uses existing entries and fetches missing IDs concurrently
    /// through the related bucket's entry loader. Pages are loaded on this
    /// bucket. The resolved source publishes each complete pair asynchronously.
    /// Keep the resolved source to share its lookup results across consumers.
    ///
    /// - Parameters:
    ///   - key: The related bucket's index on each root entry.
    ///   - related: The indexed bucket that owns the related entries and loader.
    func resolving<Element, ID, Related, RelatedOperation>(
        _ key: KeyPath<Element, ID>,
        from related: Bucket<IndexedKey<ID, Related>, RelatedOperation>
    ) -> ResolvedBucket<Element, ID, Related> where Space.Snapshot == [Element] {
        ResolvedBucket(root: self, key: key, related: related)
    }
}
