// swift-tools-version: 6.3
import Foundation
import PackageDescription

// An opt-in consumer fixture; the main library does not depend on Ensemble.
let ensemblePath = ProcessInfo.processInfo.environment["ENSEMBLE_PACKAGE_PATH"]
    ?? "../../../Ensemble"
let package = Package(
    name: "CompositionIntegration",
    platforms: [.macOS(.v14)],
    dependencies: [
        .package(path: "../.."),
        .package(path: ensemblePath),
    ],
    targets: [
        .testTarget(
            name: "IntegrationTests",
            dependencies: [
                .product(name: "Continuum", package: "continuum"),
                .product(name: "Ensemble", package: "ensemble"),
            ]
        ),
    ]
)
