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
import os

/// Rearms Observation and ignores callbacks from a replaced tracking pass.
@MainActor
final class ObservationSubscription {
    private(set) var change = Change()

    // onChange runs before the setter completes, potentially off MainActor.
    // Invalidate synchronously so async work can reject an obsolete snapshot
    // even before the main-actor callback has had a chance to read new values.
    final class Change: Sendable {
        private let current = OSAllocatedUnfairLock(initialState: true)
        var isCurrent: Bool { current.withLock { $0 } }
        func invalidate() { current.withLock { $0 = false } }
    }

    func track<Value>(
        _ read: () -> Value,
        onChange: @escaping @MainActor () -> Void
    ) -> Value {
        let change = Change()
        self.change = change
        return withObservationTracking(read) { [weak self] in
            change.invalidate()
            Task { @MainActor [weak self] in
                guard self?.change === change else { return }
                onChange()
            }
        }
    }

    func cancel() {
        change.invalidate()
        change = Change()
    }
}
