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
import OSLog
import os

/// Process-wide configuration for Continuum.
nonisolated public enum Continuum {
    /// The amount of optional diagnostic detail emitted by Continuum.
    ///
    /// Diagnostic events are available only in debug builds. Both enabled
    /// levels omit bucket values, inputs, partition values, pagination cursors,
    /// and free-form error descriptions.
    public enum DebugLogLevel: Equatable, Sendable {
        /// Disables optional diagnostic events.
        case off

        /// Emits operation requests, decisions, waits, failures, and outcomes.
        case normal

        /// Adds source, queue, and persistence lifecycle details.
        case trace
    }

    private static let debugLock =
        OSAllocatedUnfairLock(initialState: DebugLogLevel.off)

    /// The process-wide diagnostic level.
    ///
    /// The default is ``DebugLogLevel/off``. Set this property during app or
    /// test startup. Reads and writes are safe from any concurrency domain.
    /// Enabled levels emit unified-log events only in debug builds.
    public static var debug: DebugLogLevel {
        get { debugLock.withLock { $0 } }
        set { debugLock.withLock { $0 = newValue } }
    }
}

nonisolated struct BucketLogIdentity: Equatable, Sendable {
    let namespace: String
    let version: Int
    let instanceID: String

    init(
        namespace: String,
        version: Int,
        instanceID: String = ContinuumLogContext.makeIdentifier()
    ) {
        self.namespace = namespace
        self.version = version
        self.instanceID = instanceID
    }

    var rendered: String {
        """
        bucket="\(continuumEscaped(namespace))" \
        version=\(version) instance=\(continuumShortIdentifier(instanceID))
        """
    }
}

nonisolated enum BucketMutationKind: String, Sendable {
    case store
    case remove
    case resetMemory = "reset-memory"
    case resetLocal = "reset-local"
}

nonisolated enum ContinuumLogEvent {
    case bucketConfigured(
        BucketLogIdentity,
        localSources: Int,
        writableSources: Int,
        hasRemote: Bool,
        paginated: Bool,
        invalidationSignals: Int
    )
    case loadRequested(
        BucketLogIdentity,
        policy: LoadPolicy,
        established: Bool
    )
    case loadReturnedMemory(BucketLogIdentity, count: Int)
    case loadJoined(BucketLogIdentity, activeOperationID: String)
    case localSourceStarted(BucketLogIdentity, index: Int, total: Int)
    case localSourceMissed(BucketLogIdentity, index: Int)
    case localSourceHit(BucketLogIdentity, index: Int, count: Int)
    case remoteSourceStarted(BucketLogIdentity)
    case cachedSnapshotPublished(BucketLogIdentity, count: Int)
    case loadCompleted(
        BucketLogIdentity,
        origin: String,
        count: Int,
        hasNextPage: Bool
    )
    case operationSuperseded(
        BucketLogIdentity,
        operation: String,
        replacement: String,
        replacementOperationID: String
    )
    case operationCancelled(BucketLogIdentity, operation: String)
    case operationFailed(
        BucketLogIdentity,
        operation: String,
        error: any Error
    )
    case mutationRequested(
        BucketLogIdentity,
        kind: BucketMutationKind,
        queued: Bool
    )
    case remoteMutationStarted(
        BucketLogIdentity,
        kind: BucketMutationKind
    )
    case mutationCompleted(
        BucketLogIdentity,
        kind: BucketMutationKind,
        count: Int,
        established: Bool
    )
    case persistenceStarted(BucketLogIdentity, destinations: Int)
    case persistenceCompleted(BucketLogIdentity, destinations: Int)
    case paginationRequested(BucketLogIdentity)
    case paginationWaitingForLoad(
        BucketLogIdentity,
        activeOperationID: String
    )
    case paginationJoined(
        BucketLogIdentity,
        activeOperationID: String
    )
    case paginationExhausted(BucketLogIdentity, count: Int)
    case paginationCompleted(
        BucketLogIdentity,
        count: Int,
        hasNextPage: Bool
    )
    case invalidationReceived(BucketLogIdentity)
    case invalidationStreamFailed(BucketLogIdentity, error: any Error)

    var minimumLevel: Continuum.DebugLogLevel {
        switch self {
        case .bucketConfigured,
             .localSourceStarted,
             .localSourceMissed,
             .remoteSourceStarted,
             .remoteMutationStarted,
             .persistenceStarted,
             .persistenceCompleted:
            .trace
        case .loadRequested,
             .loadReturnedMemory,
             .loadJoined,
             .localSourceHit,
             .cachedSnapshotPublished,
             .loadCompleted,
             .operationSuperseded,
             .operationCancelled,
             .operationFailed,
             .mutationRequested,
             .mutationCompleted,
             .paginationRequested,
             .paginationWaitingForLoad,
             .paginationJoined,
             .paginationExhausted,
             .paginationCompleted,
             .invalidationReceived,
             .invalidationStreamFailed:
            .normal
        }
    }

    var category: String {
        switch self {
        case .bucketConfigured:
            "lifecycle"
        case .loadRequested,
             .loadReturnedMemory,
             .loadJoined,
             .localSourceStarted,
             .localSourceMissed,
             .localSourceHit,
             .remoteSourceStarted,
             .cachedSnapshotPublished,
             .loadCompleted:
            "load"
        case .operationSuperseded,
             .operationCancelled,
             .operationFailed:
            "operation"
        case .mutationRequested,
             .remoteMutationStarted,
             .mutationCompleted:
            "mutation"
        case .persistenceStarted,
             .persistenceCompleted:
            "persistence"
        case .paginationRequested,
             .paginationWaitingForLoad,
             .paginationJoined,
             .paginationExhausted,
             .paginationCompleted:
            "pagination"
        case .invalidationReceived,
             .invalidationStreamFailed:
            "invalidation"
        }
    }

    var marker: String {
        switch self {
        case .bucketConfigured:
            "•"
        case .loadRequested,
             .localSourceStarted,
             .remoteSourceStarted,
             .mutationRequested,
             .remoteMutationStarted,
             .persistenceStarted,
             .paginationRequested,
             .invalidationReceived:
            "⇢"
        case .loadReturnedMemory,
             .localSourceHit,
             .cachedSnapshotPublished,
             .loadCompleted,
             .mutationCompleted,
             .persistenceCompleted,
             .paginationCompleted:
            "✓"
        case .loadJoined,
             .paginationWaitingForLoad,
             .paginationJoined:
            "⏳"
        case .localSourceMissed,
             .paginationExhausted:
            "⊘"
        case .operationSuperseded:
            "↻"
        case .operationCancelled:
            "•"
        case .operationFailed,
             .invalidationStreamFailed:
            "✗"
        }
    }

    func renderedMessage(operationID: String?) -> String {
        let trace = operationID.map {
            "[op:\(continuumShortIdentifier($0))]"
        } ?? ""
        return "[\(category)]\(trace) \(marker) \(body)"
    }

    private var body: String {
        switch self {
        case .bucketConfigured(
            let bucket,
            let localSources,
            let writableSources,
            let hasRemote,
            let paginated,
            let invalidationSignals
        ):
            """
            configured \(bucket.rendered) local=\(localSources) \
            writable=\(writableSources) remote=\(hasRemote) \
            paginated=\(paginated) invalidations=\(invalidationSignals)
            """
        case .loadRequested(let bucket, let policy, let established):
            """
            requested \(bucket.rendered) policy=\(policy.debugName) \
            established=\(established)
            """
        case .loadReturnedMemory(let bucket, let count):
            "returned established memory \(bucket.rendered) count=\(count)"
        case .loadJoined(let bucket, let activeOperationID):
            """
            joined active load \(bucket.rendered) \
            active=\(continuumShortIdentifier(activeOperationID))
            """
        case .localSourceStarted(let bucket, let index, let total):
            """
            trying local source \(bucket.rendered) source=\(index)/\(total)
            """
        case .localSourceMissed(let bucket, let index):
            "local source missed \(bucket.rendered) source=\(index)"
        case .localSourceHit(let bucket, let index, let count):
            """
            local source resolved \(bucket.rendered) source=\(index) \
            count=\(count)
            """
        case .remoteSourceStarted(let bucket):
            "requesting remote source \(bucket.rendered)"
        case .cachedSnapshotPublished(let bucket, let count):
            """
            published cached snapshot while refreshing \(bucket.rendered) \
            count=\(count)
            """
        case .loadCompleted(
            let bucket,
            let origin,
            let count,
            let hasNextPage
        ):
            """
            loaded \(bucket.rendered) origin=\(origin) count=\(count) \
            next-page=\(hasNextPage)
            """
        case .operationSuperseded(
            let bucket,
            let operation,
            let replacement,
            let replacementOperationID
        ):
            """
            superseded \(operation) \(bucket.rendered) by=\(replacement) \
            replacement=\(continuumShortIdentifier(replacementOperationID))
            """
        case .operationCancelled(let bucket, let operation):
            "cancelled \(operation) \(bucket.rendered)"
        case .operationFailed(let bucket, let operation, let error):
            """
            \(operation) failed \(bucket.rendered) \
            error=\(continuumErrorName(error))
            """
        case .mutationRequested(let bucket, let kind, let queued):
            """
            requested \(kind.rawValue) \(bucket.rendered) queued=\(queued)
            """
        case .remoteMutationStarted(let bucket, let kind):
            "requesting remote \(kind.rawValue) \(bucket.rendered)"
        case .mutationCompleted(
            let bucket,
            let kind,
            let count,
            let established
        ):
            """
            completed \(kind.rawValue) \(bucket.rendered) count=\(count) \
            established=\(established)
            """
        case .persistenceStarted(let bucket, let destinations):
            """
            writing local snapshots \(bucket.rendered) \
            destinations=\(destinations)
            """
        case .persistenceCompleted(let bucket, let destinations):
            """
            wrote local snapshots \(bucket.rendered) \
            destinations=\(destinations)
            """
        case .paginationRequested(let bucket):
            "requested next page \(bucket.rendered)"
        case .paginationWaitingForLoad(let bucket, let activeOperationID):
            """
            waiting for initial load \(bucket.rendered) \
            active=\(continuumShortIdentifier(activeOperationID))
            """
        case .paginationJoined(let bucket, let activeOperationID):
            """
            joined active next page \(bucket.rendered) \
            active=\(continuumShortIdentifier(activeOperationID))
            """
        case .paginationExhausted(let bucket, let count):
            """
            skipped next page because pagination is exhausted \
            \(bucket.rendered) count=\(count)
            """
        case .paginationCompleted(let bucket, let count, let hasNextPage):
            """
            loaded next page \(bucket.rendered) count=\(count) \
            next-page=\(hasNextPage)
            """
        case .invalidationReceived(let bucket):
            "received invalidation \(bucket.rendered)"
        case .invalidationStreamFailed(let bucket, let error):
            """
            invalidation stream failed \(bucket.rendered) \
            error=\(continuumErrorName(error))
            """
        }
    }
}

nonisolated enum ContinuumLogContext {
    @TaskLocal
    static var operationID: String?

    static func makeIdentifier() -> String {
        #if DEBUG
        UUID().uuidString
        #else
        ""
        #endif
    }

    static func withOperation<Result>(
        _ operation: () throws -> Result
    ) rethrows -> Result {
        #if DEBUG
        if operationID != nil {
            return try operation()
        }
        return try $operationID.withValue(makeIdentifier(), operation: operation)
        #else
        return try operation()
        #endif
    }

    static func withOperation<Result>(
        _ operation: () async throws -> Result
    ) async rethrows -> Result {
        #if DEBUG
        if operationID != nil {
            return try await operation()
        }
        return try await $operationID.withValue(
            makeIdentifier(),
            operation: operation
        )
        #else
        return try await operation()
        #endif
    }

    static func withOperationID<Result>(
        _ identifier: String,
        _ operation: () throws -> Result
    ) rethrows -> Result {
        #if DEBUG
        try $operationID.withValue(identifier, operation: operation)
        #else
        try operation()
        #endif
    }

    static func withOperationID<Result>(
        _ identifier: String,
        _ operation: () async throws -> Result
    ) async rethrows -> Result {
        #if DEBUG
        try await $operationID.withValue(
            identifier,
            operation: operation
        )
        #else
        try await operation()
        #endif
    }
}

nonisolated func continuumDebug(
    _ event: @autoclosure () -> ContinuumLogEvent
) {
    #if DEBUG
    let level = Continuum.debug
    guard level != .off else { return }

    let event = event()
    guard level.includes(event.minimumLevel) else { return }

    let message = event.renderedMessage(
        operationID: ContinuumLogContext.operationID
    )
    Logger(
        subsystem: "eu.lelfe.Continuum",
        category: event.category
    ).debug("\(message, privacy: .public)")
    #endif
}

private nonisolated func continuumEscaped(_ value: String) -> String {
    value
        .replacingOccurrences(of: "\\", with: "\\\\")
        .replacingOccurrences(of: "\"", with: "\\\"")
        .replacingOccurrences(of: "\n", with: "\\n")
        .replacingOccurrences(of: "\r", with: "\\r")
        .replacingOccurrences(of: "\t", with: "\\t")
}

private nonisolated func continuumShortIdentifier(_ identifier: String) -> String {
    String(identifier.prefix(8)).uppercased()
}

private nonisolated func continuumErrorName(_ error: any Error) -> String {
    String(reflecting: type(of: error))
        .split(separator: ".")
        .last
        .map(String.init)
        ?? "Error"
}

nonisolated extension Continuum.DebugLogLevel {
    func includes(_ minimumLevel: Self) -> Bool {
        switch (self, minimumLevel) {
        case (.normal, .normal),
             (.trace, .normal),
             (.trace, .trace):
            true
        case (.off, _),
             (.normal, .trace),
             (_, .off):
            false
        }
    }
}

private nonisolated extension LoadPolicy {
    var debugName: String {
        switch self {
        case .cached:
            "cached"
        case .cachedThenRemote:
            "cached-then-remote"
        case .remote:
            "remote"
        }
    }
}
