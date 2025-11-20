// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "download-speed-test",
    platforms: [
        .macOS(.v14),
        .iOS(.v16),
    ],
    products: [
        .executable(
            name: "download-speed-test",
            targets: ["DownloadSpeedTest"]
        )
    ],
    dependencies: [
        .package(path: "../"),
        .package(url: "https://github.com/apple/swift-argument-parser", from: "1.3.0"),
    ],
    targets: [
        .executableTarget(
            name: "DownloadSpeedTest",
            dependencies: [
                .product(name: "HuggingFace", package: "swift-huggingface"),
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
            ]
        )
    ]
)
