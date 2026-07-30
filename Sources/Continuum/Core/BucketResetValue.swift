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

/// An opaque comparison value for unavailable-state transitions in one bucket partition.
///
/// A value is stable across repeated reads and changes when its partition
/// deliberately returns to an unavailable state. Retain the last value handled
/// by an observation source and compare it with the partition's current
/// ``BucketPartition/resetValue`` to recognize a reset without representing
/// reset as a snapshot value. Seed that retained value from the current value
/// when an observation starts to avoid replaying a reset that happened before
/// subscription.
nonisolated public struct BucketResetValue: Equatable, Sendable {
    private let sourceID: UUID
    private let revision: UInt

    init() {
        sourceID = UUID()
        revision = 0
    }

    private init(sourceID: UUID, revision: UInt) {
        self.sourceID = sourceID
        self.revision = revision
    }

    func advanced() -> Self {
        Self(sourceID: sourceID, revision: revision &+ 1)
    }
}
