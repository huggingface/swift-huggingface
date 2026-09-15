// swift-tools-version: 6.1
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
    traits: [
        .trait(
            name: "Xet",
            description: "Enable Xet transport support.",
        )
    ],
    dependencies: [
        .package(url: "https://github.com/mattt/EventSource.git", from: "1.0.0"),
        .package(url: "https://github.com/apple/swift-crypto.git", "1.0.0" ..< "5.0.0"),
        .package(url: "https://github.com/huggingface/swift-xet.git", from: "0.2.0"),
        .package(
            url: "https://github.com/mattt/Replay.git",
            from: "0.6.0",
            // Replay uses canImport(AsyncHTTPClient), which Xet or cached modules can satisfy.
            // Enable its dependency so SwiftPM always supplies the required C modules.
            traits: ["AsyncHTTPClient"]
        ),
    ],
    targets: [
        .target(
            name: "HuggingFace",
            dependencies: [
                .product(name: "EventSource", package: "EventSource"),
                .product(name: "Crypto", package: "swift-crypto"),
                .product(name: "Xet", package: "swift-xet", condition: .when(traits: ["Xet"])),
            ],
            path: "Sources/HuggingFace",
            swiftSettings: [
                .define("HUGGINGFACE_ENABLE_XET", .when(traits: ["Xet"]))
            ]
        ),
        .testTarget(
            name: "HuggingFaceTests",
            dependencies: ["HuggingFace"],
            swiftSettings: [
                .define("HUGGINGFACE_ENABLE_XET", .when(traits: ["Xet"]))
            ]
        ),
        .testTarget(
            name: "HubReplayTests",
            dependencies: [
                "HuggingFace",
                .product(name: "Replay", package: "Replay"),
            ],
            exclude: ["README.md"],
            resources: [.copy("Replays")]
        ),
        .testTarget(
            name: "HubBenchmarks",
            dependencies: ["HuggingFace"],
            swiftSettings: [
                .define("HUGGINGFACE_ENABLE_XET", .when(traits: ["Xet"]))
            ]
        ),
    ]
)
