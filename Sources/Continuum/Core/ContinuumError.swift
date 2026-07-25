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

/// An error raised before configured source work begins.
public enum ContinuumError: LocalizedError, Sendable {
    /// The requested key space has no remote source from which to load a
    /// snapshot.
    case missingRemoteSource(namespace: String)

    /// The requested key space does not declare paginated remote operations.
    case missingPaginatedRemoteSource(namespace: String)

    /// The paginated key space has not loaded its initial remote page.
    case initialPageNotLoaded(namespace: String)

    /// A caller-facing description of the configuration error.
    public var errorDescription: String? {
        switch self {
        case .missingRemoteSource(let namespace):
            "No RemoteSource is configured for data bucket \(namespace.debugDescription)."
        case .missingPaginatedRemoteSource(let namespace):
            "No paginated RemoteSource is configured for data bucket \(namespace.debugDescription)."
        case .initialPageNotLoaded(let namespace):
            "The initial remote page has not been loaded for data bucket \(namespace.debugDescription)."
        }
    }
}
