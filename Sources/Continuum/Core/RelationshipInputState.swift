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

@MainActor
struct RelationshipSource<Value: Sendable> {
    let read: (@MainActor () -> CompositionInputState<Value>)?
    let load: @MainActor @Sendable (LoadPolicy) async throws -> Value
}

@MainActor
@Observable
final class RelationshipInputState<Element: Sendable, ID: Hashable & Sendable, Resolved: Sendable> {
    var rootState = CompositionInputState<[Element]>(value: nil, unavailable: true, failures: [])
    var relatedState = CompositionInputState<[ID: Resolved]>(value: nil, unavailable: true, failures: [])

    @ObservationIgnored private let root: Input<[Element]>
    @ObservationIgnored private let key: KeyPath<Element, ID> & Sendable
    @ObservationIgnored private let select: @MainActor (ID) -> RelationshipSource<Resolved>
    @ObservationIgnored private let rootObservation = ObservationSubscription()
    @ObservationIgnored private let relatedObservation = ObservationSubscription()
    @ObservationIgnored private var task: Task<Void, Never>?
    @ObservationIgnored private var loadingRoot = false
    @ObservationIgnored private var resolving = false
    @ObservationIgnored private var snapshot: [Element]?
    @ObservationIgnored private var sources: [ID: RelationshipSource<Resolved>] = [:]
    @ObservationIgnored private var values: [ID: Resolved] = [:]
    @ObservationIgnored private var failures: [ID: any Error] = [:]
    @ObservationIgnored private var keys: [ID] = []
    // Reset history is presentation invalidation, not async task ownership.
    @ObservationIgnored private var resetRevision: UInt = 0
    @ObservationIgnored private var lastRootReset: UInt = 0
    @ObservationIgnored private var lastRelatedResets: [ID: UInt] = [:]

    init(root: Input<[Element]>, key: KeyPath<Element, ID> & Sendable,
         select: @escaping @MainActor (ID) -> RelationshipSource<Resolved>) {
        self.root = root
        self.key = key
        self.select = select
    }

    func start() { refresh() }

    private func observeRoot() -> CompositionInputState<[Element]> {
        rootObservation.track { root.read() } onChange: { [weak self] in
            guard let self else { return }
            if self.loadingRoot { _ = self.inspectRoot() }
            else { self.refresh() }
        }
    }

    private func refresh() {
        task?.cancel()
        loadingRoot = false
        let ids = prepareRoot(reload: false)
        guard !ids.isEmpty else { task = nil; return }
        let observation = rootObservation.change
        let operations = operations(for: ids)
        task = Task { @MainActor [weak self] in
            let results = await Self.resolve(operations, policy: .cached)
            guard !Task.isCancelled, observation.isCurrent else { return }
            self?.complete(results)
        }
    }

    /// Resets and failures take effect even while an explicit root load is suspended.
    private func inspectRoot() -> CompositionInputState<[Element]> {
        let state = observeRoot()
        if state.resetRevision != lastRootReset || (state.unavailable && !rootState.unavailable) {
            resetRevision &+= 1
            values = [:]
            relatedState = .init(value: nil, unavailable: true, failures: [], resetRevision: resetRevision)
        }
        lastRootReset = state.resetRevision
        if state.unavailable || !state.failures.isEmpty {
            snapshot = nil
            if state.unavailable {
                values = [:]
                sources = [:]
                keys = []
                lastRelatedResets = [:]
                relatedObservation.cancel()
            }
            rootState = .init(value: nil, unavailable: state.unavailable, failures: state.failures,
                              resetRevision: resetRevision)
            relatedState = .init(value: nil, unavailable: state.unavailable, failures: [],
                                 resetRevision: resetRevision)
        }
        return state
    }

    /// Captures one root snapshot and chooses its work without publishing it early.
    private func prepareRoot(reload: Bool) -> [ID] {
        resolving = false
        let state = inspectRoot()
        guard let snapshot = state.value, !state.unavailable, state.failures.isEmpty else { return [] }
        self.snapshot = snapshot
        var seen: Set<ID> = []
        keys = snapshot.map { $0[keyPath: key] }.filter { seen.insert($0).inserted }
        values = values.filter { seen.contains($0.key) }
        failures = [:]
        sources = sources.filter { seen.contains($0.key) }
        lastRelatedResets = lastRelatedResets.filter { seen.contains($0.key) }
        let newKeys = Set(keys.filter { sources[$0] == nil })
        for id in newKeys { sources[id] = select(id) }
        let needed = reload ? keys : keys.filter {
            values[$0] == nil && (sources[$0]?.read == nil || newKeys.contains($0))
        }
        resolving = !needed.isEmpty
        observeRelated()
        if !resolving { publish() }
        return needed
    }

    private func observeRelated() {
        let states = relatedObservation.track {
            keys.compactMap { id in sources[id]?.read.map { (id, $0()) } }
        } onChange: { [weak self] in
            self?.observeRelated()
            self?.publish()
        }
        for (id, state) in states {
            if let previous = lastRelatedResets[id], previous != state.resetRevision {
                resetRevision &+= 1
                // A required partition invalidates the retained pair even when
                // another relationship is still being resolved.
                relatedState = .init(value: nil, unavailable: true, failures: [], resetRevision: resetRevision)
            }
            lastRelatedResets[id] = state.resetRevision
            values[id] = state.failures.isEmpty && !state.unavailable ? state.value : nil
            failures[id] = state.failures.first
        }
    }

    private func operations(for ids: [ID]) -> [(ID, @MainActor @Sendable (LoadPolicy) async throws -> Resolved)] {
        ids.compactMap { id in sources[id].map { (id, $0.load) } }
    }

    private static func resolve(
        _ operations: [(ID, @MainActor @Sendable (LoadPolicy) async throws -> Resolved)], policy: LoadPolicy
    ) async -> [(ID, Result<Resolved, any Error>)] {
        await withTaskGroup(of: (ID, Result<Resolved, any Error>).self) { group in
            for (id, operation) in operations {
                group.addTask {
                    do {
                        try Task.checkCancellation()
                        return (id, .success(try await operation(policy)))
                    } catch { return (id, .failure(error)) }
                }
            }
            var results: [(ID, Result<Resolved, any Error>)] = []
            for await result in group { results.append(result) }
            return results
        }
    }

    private func complete(_ results: [(ID, Result<Resolved, any Error>)]) {
        resolving = false
        // Native partitions own their outcome, including external mutations that
        // superseded a load. Only one-shot lookups establish data from returns.
        observeRelated()
        for (id, result) in results {
            if sources[id]?.read == nil {
                switch result {
                case .success(let value): values[id] = value
                case .failure(let error):
                    if !(error is CancellationError) { failures[id] = error }
                }
            } else if case .failure(let error) = result,
                      !(error is CancellationError), values[id] == nil, failures[id] == nil {
                failures[id] = error
            }
        }
        publish()
    }

    private func publish() {
        guard let snapshot, !resolving, !loadingRoot, rootObservation.change.isCurrent else { return }
        let errors = keys.compactMap { failures[$0] }
        let unavailable = keys.contains { values[$0] == nil && failures[$0] == nil }
        // Install the pair in one actor turn before Observation reevaluates it.
        rootState = .init(value: snapshot, unavailable: false, failures: [], resetRevision: resetRevision)
        relatedState = .init(value: unavailable || !errors.isEmpty ? nil : values,
                             unavailable: unavailable, failures: errors, resetRevision: resetRevision)
    }

    func load(_ policy: LoadPolicy) async {
        guard !Task.isCancelled else { return }
        task?.cancel()
        loadingRoot = true
        let action = root.load
        let task = Task { @MainActor [weak self] in
            await action?(policy)
            guard !Task.isCancelled else { return }
            self?.loadingRoot = false
            let ids = self?.prepareRoot(reload: true) ?? []
            guard let observation = self?.rootObservation.change else { return }
            let operations = self?.operations(for: ids) ?? []
            let results = await Self.resolve(operations, policy: policy)
            guard !Task.isCancelled, observation.isCurrent else { return }
            self?.complete(results)
        }
        self.task = task
        await withTaskCancellationHandler { await task.value } onCancel: { task.cancel() }
        if self.task == task {
            loadingRoot = false
            resolving = false
            if Task.isCancelled { _ = inspectRoot() }
        }
    }

    deinit { task?.cancel() }
}
