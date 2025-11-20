// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "swift-huggingface",
    platforms: [
        .macOS(.v14),
        .macCatalyst(.v14),
        .iOS(.v16),
        .watchOS(.v10),
        .tvOS(.v17),
        .visionOS(.v1),
    ],
    products: [
        .library(
            name: "HuggingFace",
            targets: ["HuggingFace"]
        )
    ],
    dependencies: [
        .package(url: "https://github.com/mattt/EventSource.git", from: "1.0.0"),
        .package(url: "https://github.com/mattt/swift-xet.git", branch: "main"),
    ],
    targets: [
        .target(
            name: "HuggingFace",
            dependencies: [
                .product(name: "EventSource", package: "EventSource"),
                .product(name: "Xet", package: "swift-xet"),
            ],
            path: "Sources/HuggingFace"
        ),
        .testTarget(
            name: "HuggingFaceTests",
            dependencies: ["HuggingFace"]
        ),
    ]
)
