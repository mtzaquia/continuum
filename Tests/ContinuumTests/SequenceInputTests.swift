import Continuum
import Testing

@Suite("Sequence inputs", .timeLimit(.minutes(1)))
struct SequenceInputTests {
    @Test("Result streams gate required inputs and recover without reset")
    func outcomes() async {
        let (stream, producer) = AsyncStream<Result<Int, StreamError>>.makeStream()
        let composition = Composition { Input(updates: { stream }) } transform: { $0 + 1 }
        #expect(isReset(composition.latest))
        let first = Task {
            var iterator = composition.makeAsyncIterator()
            return await iterator.next()
        }
        producer.yield(.success(1))
        #expect(value(await first.value) == 2)
        var iterator = composition.makeAsyncIterator()
        _ = await iterator.next()
        producer.yield(.failure(.failed))
        #expect(isFailure(await iterator.next()))
        producer.yield(.success(3))
        #expect(value(await iterator.next()) == 4)
        producer.finish()
        for _ in 0..<10 { await Task.yield() }
        #expect(value(composition.latest) == 4)
    }

    @Test("Optional streams supply nil while unavailable or failed and flatten domain nil")
    func optionalStream() async {
        let (stream, producer) = AsyncStream<Result<Int?, StreamError>>.makeStream()
        let composition = Composition {
            Input(updates: { stream }).optional()
        } transform: { (number: Int?) in number ?? -1 }
        #expect(value(composition.latest) == -1)
        var iterator = composition.makeAsyncIterator()
        _ = await iterator.next()
        producer.yield(.success(4))
        #expect(value(await iterator.next()) == 4)
        producer.yield(.failure(.failed))
        #expect(value(await iterator.next()) == -1)
        producer.yield(.success(nil))
        #expect(value(await iterator.next()) == -1)
        producer.finish()
    }

    @Test("Restart creates the iterator before loading and keeps the previous outcome")
    func restart() async {
        var producers: [AsyncStream<Result<Int, StreamError>>.Continuation] = []
        var factories = 0
        var loads = 0
        let composition = Composition {
            Input(updates: {
                factories += 1
                let (stream, producer) = AsyncStream<Result<Int, StreamError>>.makeStream()
                producers.append(producer)
                return stream
            }, subscriptionOnLoad: .restart, load: { policy in
                #expect(factories == 2)
                if case .remote = policy { loads += 1 }
                producers.last?.yield(.success(9))
            })
        } transform: { $0 + 1 }
        for _ in 0..<10 { await Task.yield() }
        var iterator = composition.makeAsyncIterator()
        producers[0].yield(.success(1))
        #expect(value(await iterator.next()) == 2)
        await composition.load(using: .remote)
        #expect(loads == 1)
        // Repeated current outcomes are allowed, but restarting must not reset.
        while let update = await iterator.next() {
            #expect(!isReset(update))
            if value(update) == 10 { break }
        }
        #expect(factories == 2)
        producers[0].yield(.success(99))
        for _ in 0..<10 { await Task.yield() }
        #expect(value(composition.latest) == 10)
    }

    @Test("Default keep does not restart a completed subscription")
    func keepCompleted() async {
        var factories = 0
        var loads = 0
        let composition = Composition {
            Input(updates: {
                factories += 1
                return AsyncStream<Result<Int, StreamError>> { $0.finish() }
            }, load: { _ in loads += 1 })
        } transform: { $0 + 1 }
        await composition.load()
        await composition.load()
        #expect(factories == 1)
        #expect(loads == 2)
        #expect(isReset(composition.latest))
    }

    @Test("Thrown iteration errors are failures and explicit restart recovers")
    func throwingStream() async {
        var factories = 0
        let composition = Composition {
            Input(updates: {
                factories += 1
                return AsyncThrowingStream<Result<Int, StreamError>, any Error> { producer in
                    if factories == 1 { producer.finish(throwing: StreamError.failed) }
                    else { producer.yield(.success(8)); producer.finish() }
                }
            }, subscriptionOnLoad: .restart)
        } transform: { $0 + 1 }
        var iterator = composition.makeAsyncIterator()
        #expect(isFailure(await iterator.next()))
        await composition.load()
        while let update = await iterator.next() {
            if value(update) == 9 { break }
        }
        #expect(factories == 2)
    }

    @Test("Input declarations create independent subscriptions per composition")
    func independentOwnership() async {
        var factories = 0
        let input = Input(updates: {
            factories += 1
            return AsyncStream<Result<Int, StreamError>> { $0.yield(.success(factories)); $0.finish() }
        })
        #expect(factories == 0)
        let a = Composition { input } transform: { $0 + 0 }
        let b = Composition { input } transform: { $0 + 0 }
        var first = a.makeAsyncIterator()
        var second = b.makeAsyncIterator()
        #expect(value(await first.next()) != nil)
        #expect(value(await second.next()) != nil)
        #expect(factories == 2)
        #expect(value(a.latest) != value(b.latest))
    }

    @Test("Loading errors use required and optional treatment and recover on retry")
    func loadingErrors() async {
        var shouldFail = true
        let input = Input(updates: {
            AsyncStream<Result<Int, StreamError>> { $0.yield(.success(5)); $0.finish() }
        }, load: { _ in if shouldFail { throw StreamError.failed } })
        let required = Composition { input } transform: { $0 + 1 }
        let optional = Composition { input.optional() } transform: { $0 ?? -1 }
        await required.load()
        await optional.load()
        #expect(isFailure(required.latest))
        #expect(value(optional.latest) == -1)
        shouldFail = false
        await required.load()
        await optional.load()
        #expect(value(required.latest) == 6)
        #expect(value(optional.latest) == 5)
    }

    @Test("Multiple consumers share one upstream subscription")
    func sharedSubscription() async {
        var factories = 0
        let composition = Composition {
            Input(updates: {
                factories += 1
                return AsyncStream<Result<Int, StreamError>> { $0.yield(.success(5)); $0.finish() }
            })
        } transform: { $0 + 1 }
        var first = composition.makeAsyncIterator()
        var second = composition.makeAsyncIterator()
        #expect(value(await first.next()) == 6)
        #expect(value(await second.next()) == 6)
        #expect(factories == 1)
    }

    @Test("Releasing a composition cancels its stream subscription")
    func lifetime() async {
        let (cancelled, cancellation) = AsyncStream<Void>.makeStream()
        var composition: Composition<Int>? = Composition {
            Input(updates: {
                AsyncStream<Result<Int, StreamError>> { producer in
                    producer.yield(.success(1))
                    producer.onTermination = { _ in cancellation.yield(()) }
                }
            })
        } transform: { $0 + 1 }
        weak var weakComposition = composition
        var iterator = composition?.makeAsyncIterator()
        #expect(value(await iterator?.next()) == 2)
        iterator = nil
        composition = nil
        var cancellations = cancelled.makeAsyncIterator()
        #expect(await cancellations.next() != nil)
        #expect(weakComposition == nil)
        weakComposition = nil
    }
}

private enum StreamError: Error { case failed }
private func value(_ update: Update<Int>?) -> Int? {
    if case .result(.success(let value)) = update { return value }
    return nil
}
private func isReset<T>(_ update: Update<T>?) -> Bool {
    if case .reset = update { return true }
    return false
}
private func isFailure<T>(_ update: Update<T>?) -> Bool {
    if case .result(.failure) = update { return true }
    return false
}
