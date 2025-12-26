// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "swift-huggingface",
    platforms: [
        .macOS(.v13),
        .macCatalyst(.v16),
        .iOS(.v16),
        .watchOS(.v9),
        .tvOS(.v16),
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
        .package(url: "https://github.com/apple/swift-crypto.git", "1.0.0" ..< "5.0.0"),
        .package(url: "https://github.com/mattt/swift-xet.git", from: "0.2.0"),
    ],
    targets: [
        .target(
            name: "HuggingFace",
            dependencies: [
                .product(name: "EventSource", package: "EventSource"),
                .product(name: "Crypto", package: "swift-crypto"),
                .product(name: "Xet", package: "swift-xet"),
            ],
            path: "Sources/HuggingFace"
        ),
        .testTarget(
            name: "HuggingFaceTests",
            dependencies: ["HuggingFace"]
        ),
        .testTarget(
            name: "Benchmarks",
            dependencies: ["HuggingFace"]
        ),
    ]
)
