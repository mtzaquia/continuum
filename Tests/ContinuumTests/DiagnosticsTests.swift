import Foundation
import Testing
@testable import Continuum

@Suite("Diagnostics", .serialized)
struct DiagnosticsTests {
    private struct SensitiveError: LocalizedError {
        let secret: String

        var errorDescription: String? {
            "The private value was \(secret)"
        }
    }

    private let identity = BucketLogIdentity(
        namespace: "posts",
        version: 2,
        instanceID: "FACE1234-0000-0000-0000-000000000000"
    )

    @Test("Every event has a deliberate minimum level")
    func eventLevels() {
        let error = SensitiveError(secret: "never-log-me")
        let normalEvents: [ContinuumLogEvent] = [
            .loadRequested(identity, policy: .cached, established: false),
            .loadRequestedWithoutSources(
                identity,
                policy: .cached,
                established: false
            ),
            .loadReturnedMemory(identity, count: 2),
            .loadJoined(identity, activeOperationID: "AAAA1111"),
            .localSourceHit(identity, index: 1, count: 2),
            .cachedSnapshotPublished(identity, count: 2),
            .loadCompleted(
                identity,
                origin: "remote",
                count: 2,
                hasNextPage: true
            ),
            .operationSuperseded(
                identity,
                operation: "load",
                replacement: "remote-load",
                replacementOperationID: "BBBB2222"
            ),
            .operationCancelled(identity, operation: "load"),
            .operationFailed(identity, operation: "load", error: error),
            .mutationRequested(identity, kind: .store, queued: false),
            .mutationCompleted(
                identity,
                kind: .store,
                count: 3,
                established: true
            ),
            .paginationRequested(identity),
            .paginationWaitingForLoad(
                identity,
                activeOperationID: "AAAA1111"
            ),
            .paginationJoined(
                identity,
                activeOperationID: "BBBB2222"
            ),
            .paginationExhausted(identity, count: 3),
            .paginationCompleted(
                identity,
                count: 4,
                hasNextPage: false
            ),
            .invalidationReceived(identity),
            .invalidationStreamFailed(identity, error: error),
        ]
        let traceEvents: [ContinuumLogEvent] = [
            .bucketConfigured(
                identity,
                localSources: 2,
                writableSources: 1,
                hasRemote: true,
                paginated: true,
                invalidationSignals: 1
            ),
            .localSourceStarted(identity, index: 1, total: 2),
            .localSourceMissed(identity, index: 1),
            .remoteSourceStarted(identity),
            .remoteMutationStarted(identity, kind: .store),
            .persistenceStarted(identity, destinations: 1),
            .persistenceCompleted(identity, destinations: 1),
        ]

        #expect(normalEvents.allSatisfy { $0.minimumLevel == .normal })
        #expect(traceEvents.allSatisfy { $0.minimumLevel == .trace })
    }

    @Test("Log levels include only their intended events")
    func levelInclusion() {
        #expect(Continuum.DebugLogLevel.off.includes(.normal) == false)
        #expect(Continuum.DebugLogLevel.off.includes(.trace) == false)
        #expect(Continuum.DebugLogLevel.normal.includes(.normal))
        #expect(Continuum.DebugLogLevel.normal.includes(.trace) == false)
        #expect(Continuum.DebugLogLevel.trace.includes(.normal))
        #expect(Continuum.DebugLogLevel.trace.includes(.trace))
    }

    @Test("Disabled events are not constructed")
    func lazyEvents() {
        Continuum.debug = .off
        defer { Continuum.debug = .off }
        var constructions = 0

        continuumDebug(traceEvent(constructions: &constructions))
        #expect(constructions == 0)
    }

    @Test("Load messages are deterministic and escaped")
    func loadRendering() {
        let identity = BucketLogIdentity(
            namespace: "posts\n\"private\"\\archive",
            version: 3,
            instanceID: "face1234-more"
        )
        let event = ContinuumLogEvent.loadRequested(
            identity,
            policy: .cachedThenRemote,
            established: true
        )

        #expect(
            event.renderedMessage(operationID: "12ab34cd-more")
                == """
                [load][op:12AB34CD] ⇢ requested \
                bucket="posts\\n\\\"private\\\"\\\\archive" \
                version=3 instance=FACE1234 \
                policy=cached-then-remote established=true
                """
        )
    }

    @Test("Source-less load warnings identify in-memory buckets")
    func sourceLessLoadWarningRendering() {
        let event = ContinuumLogEvent.loadRequestedWithoutSources(
            identity,
            policy: .remote,
            established: false
        )

        #expect(
            event.renderedMessage(operationID: "12ab34cd-more")
                == """
                [load][op:12AB34CD] ⚠ requested load for source-less in-memory bucket \
                bucket="posts" version=2 instance=FACE1234 policy=remote established=false
                """
        )
    }

    @Test("Outcome messages contain state but never payloads")
    func outcomeRendering() {
        let event = ContinuumLogEvent.mutationCompleted(
            identity,
            kind: .store,
            count: 42,
            established: true
        )

        #expect(
            event.renderedMessage(operationID: "ABCDEF12")
                == """
                [mutation][op:ABCDEF12] ✓ completed store \
                bucket="posts" version=2 instance=FACE1234 \
                count=42 established=true
                """
        )
    }

    @Test("Failures render only the error type")
    func errorRedaction() {
        let event = ContinuumLogEvent.operationFailed(
            identity,
            operation: "load",
            error: SensitiveError(secret: "never-log-me")
        )
        let rendered = event.renderedMessage(operationID: "BADF00D0")

        #expect(
            rendered
                == """
                [operation][op:BADF00D0] ✗ load failed \
                bucket="posts" version=2 instance=FACE1234 \
                error=SensitiveError
                """
        )
        #expect(rendered.contains("never-log-me") == false)
        #expect(rendered.contains("private value") == false)
    }

    @Test("Operation identifiers nest, restore, and remain unique")
    func operationIdentifiers() async {
        #expect(ContinuumLogContext.operationID == nil)

        let first = await ContinuumLogContext.withOperation {
            let outer = ContinuumLogContext.operationID
            let inherited = await ContinuumLogContext.withOperation {
                await Task.yield()
                return ContinuumLogContext.operationID
            }
            let nested = await ContinuumLogContext.withOperationID("INNER-ID") {
                await Task.yield()
                return ContinuumLogContext.operationID
            }
            let restored = ContinuumLogContext.operationID
            return (outer, inherited, nested, restored)
        }
        let second = await ContinuumLogContext.withOperation {
            await Task.yield()
            return ContinuumLogContext.operationID
        }

        #expect(first.0 != nil)
        #expect(first.0 == first.1)
        #expect(first.2 == "INNER-ID")
        #expect(first.0 == first.3)
        #expect(first.0 != second)
        #expect(ContinuumLogContext.operationID == nil)
    }

    @Test("The process-wide level supports concurrent access")
    func concurrentConfiguration() async {
        Continuum.debug = .off
        defer { Continuum.debug = .off }

        let levels = await withTaskGroup(
            of: Continuum.DebugLogLevel.self,
            returning: [Continuum.DebugLogLevel].self
        ) { group in
            for index in 0..<200 {
                group.addTask {
                    Continuum.debug = index.isMultiple(of: 2)
                        ? .normal
                        : .trace
                    return Continuum.debug
                }
            }

            var values: [Continuum.DebugLogLevel] = []
            for await value in group {
                values.append(value)
            }
            return values
        }

        #expect(levels.count == 200)
        #expect(levels.allSatisfy { $0 == .normal || $0 == .trace })
    }

    private func traceEvent(
        constructions: inout Int
    ) -> ContinuumLogEvent {
        constructions += 1
        return .remoteSourceStarted(identity)
    }
}
