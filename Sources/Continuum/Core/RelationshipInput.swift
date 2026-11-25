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

@MainActor
@Observable
private final class RelationshipInput<Element: Sendable, ID: Hashable & Sendable, Related: Sendable, RelatedOperation: Sendable>:
    LoadableSource
{
    typealias Snapshot = ResolvedSnapshot<Element, ID, Related>
    private var latest: Update<Snapshot> = .reset
    private var resolutionError: (any Error)?
    private var readRootErrors: @MainActor () -> [any Error] = { [] }

    private var resetRevision: UInt = 0
    @ObservationIgnored private let readRoot: @MainActor () -> CompositionInputState<[Element]>
    @ObservationIgnored private let rootLoader: (any LoadableSource)?
    @ObservationIgnored private let key: KeyPath<Element, ID>
    @ObservationIgnored private let relatedBucket: Bucket<IndexedKey<ID, Related>, RelatedOperation>
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

    init(
        root: Input<[Element]>,
        key: KeyPath<Element, ID>,
        related: Bucket<IndexedKey<ID, Related>, RelatedOperation>
    ) {
        readRoot = root.read
        rootLoader = root.loadSource
        self.key = key
        relatedBucket = related
        previousRootReset = root.read().resetRevision
        previousRelatedReset = related.resetRevision
        reconcile()
    }

    func load(in context: SourceLoadContext) async {
        guard !Task.isCancelled else { return }
        let rootRevision = readRoot().resetRevision
        let relatedRevision = relatedBucket.resetRevision
        if let rootLoader {
            let request = UUID()
            rootLoad = request
            await context.load(rootLoader)
            if rootLoad == request { rootLoad = nil }
        }
        guard readRoot().resetRevision == rootRevision, relatedBucket.resetRevision == relatedRevision else {
            reconcile()
            return
        }
        let hasPendingLookups = reconcile(explicit: !Task.isCancelled)
        guard !Task.isCancelled, rootLoad != nil || hasPendingLookups else { return }

        // Wait for the current snapshot to settle, even if a page replaces it
        // while lookups are in progress. Cancellation ends only this wait.
        let id = UUID()
        let (stream, continuation) = AsyncStream<Void>.makeStream()
        waiters[id] = continuation
        defer { waiters[id] = nil }
        for await _ in stream {}
    }

    @discardableResult
    private func reconcile(explicit: Bool = false) -> Bool {
        let (root, relatedValues, relatedRevision) = observation.track {
            (readRoot(), relatedBucket.values, relatedBucket.resetRevision)
        } onChange: { [weak self] in
            self?.reconcile()
        }

        readRootErrors = root.errors
        let relatedWasReset = relatedRevision != previousRelatedReset
        if root.resetRevision != previousRootReset || relatedWasReset {
            clear()
        }
        if relatedWasReset { waitsAfterRelatedReset = true }
        if explicit { waitsAfterRelatedReset = false }
        previousRootReset = root.resetRevision
        previousRelatedReset = relatedRevision

        switch root.outcome {
        case .reset:
            if case .result = latest { clear() }
            return false
        case .result(.failure(let error)):
            resolutionError = nil
            latest = .result(.failure(error))
            finishWaiting()
            return false
        case .result(.success(let elements)):
            guard rootLoad == nil else { return false }
            let entries = Dictionary(uniqueKeysWithValues: relatedValues.map {
                (relatedBucket.keySpace.input(for: $0), $0)
            })
            return resolve(elements, available: entries, explicit: explicit)
        }
    }

    func read() -> CompositionInputState<Snapshot> {
        .init(outcome: latest, resetRevision: resetRevision, reportedFailures: {
            let rootErrors = self.readRootErrors()
            if let resolutionError = self.resolutionError { return rootErrors + [resolutionError] }
            return rootErrors.isEmpty ? nil : rootErrors
        })
    }

    private func resolve(_ elements: [Element], available: [ID: Related], explicit: Bool) -> Bool {
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

        guard !pending else { return true }
        if let firstError {
            resolutionError = firstError
            if latest.snapshot == nil { latest = .result(.failure(firstError)) }
        } else if entries.count == ids.count {
            waitsAfterRelatedReset = false
            resolutionError = nil
            latest = .result(.success(.init(root: elements, related: entries)))
        }
        finishWaiting()
        return false
    }

    private func startLookup(_ id: ID) {
        let request = UUID()
        let relatedBucket = relatedBucket
        let task = Task { @MainActor [weak self] in
            let result: Lookup
            do {
                result = .resolved(try await relatedBucket.load(id: id))
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
        resolutionError = nil
        resetRevision &+= 1
        latest = .reset
        finishWaiting()
    }

    deinit {
        for lookup in lookups.values { lookup.cancel() }
    }
}

public extension Input {
    /// Resolves each distinct ID referenced by this array input.
    ///
    /// Reuses entries already in the related bucket. Distinct missing IDs
    /// load concurrently through its `loadEntry:` closure. The input supplies
    /// the root and resolved entries together as one ``ResolvedSnapshot``.
    /// A failed lookup retains the last complete pair when available.
    /// Call the composition's `load()` to retry and paginate on the buckets.
    /// If this input has loading disabled, the root is observed and lookups
    /// still follow the available root.
    ///
    /// - Parameters:
    ///   - key: The related bucket's index on each root entry.
    ///   - related: The indexed bucket that supplies related entries. It needs
    ///     a loader only for IDs that are missing from its current snapshot.
    func resolving<Element, ID, Related, RelatedOperation>(
        _ key: KeyPath<Element, ID>,
        from related: Bucket<IndexedKey<ID, Related>, RelatedOperation>
    ) -> Input<ResolvedSnapshot<Element, ID, Related>> where Value == [Element] {
        let relationship = RelationshipInput(root: self, key: key, related: related)
        return Input<ResolvedSnapshot<Element, ID, Related>>(
            read: relationship.read,
            loadSource: relationship
        )
    }
}
