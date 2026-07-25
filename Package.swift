// swift-tools-version: 6.3
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let swiftSettings: [SwiftSetting] = [
    .defaultIsolation(MainActor.self),
    .enableUpcomingFeature("NonisolatedNonsendingByDefault"),
    .enableUpcomingFeature("InferIsolatedConformances"),
]

let package = Package(
    name: "Continuum",
    platforms: [.iOS(.v17), .macOS(.v14)],
    products: [
        .library(
            name: "Continuum",
            targets: ["Continuum"]
        ),
    ],
    targets: [
        .target(
            name: "Continuum",
            swiftSettings: swiftSettings
        ),
        .testTarget(
            name: "ContinuumTests",
            dependencies: ["Continuum"],
            swiftSettings: swiftSettings
        ),
    ],
    swiftLanguageModes: [.v6]
)
