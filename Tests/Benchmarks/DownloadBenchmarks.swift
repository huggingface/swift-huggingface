import Foundation

#if canImport(FoundationNetworking)
import FoundationNetworking
#endif
import Testing

@testable import HuggingFace

/// Set the RUN_BENCHMARKS environment variable to enable benchmarks.
///
/// Run from command line:
///   RUN_BENCHMARKS=1 swift test --filter Benchmarks
let benchmarksEnabled = ProcessInfo.processInfo.environment["RUN_BENCHMARKS"] == "1"

/// Benchmark tests to measure download performance improvements.
/// Run these before and after optimization commits to compare.
///
/// These tests require network access and use real Hugging Face repositories.
@Suite("Download Benchmarks", .serialized, .enabled(if: benchmarksEnabled))
struct DownloadBenchmarks {
    static let downloadDestination: URL = {
        let base = FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask).first!
        return base.appending(component: "huggingface-benchmarks")
    }()

    init() {
        // Clean before each test to ensure consistent starting state
        try? FileManager.default.removeItem(at: Self.downloadDestination)
    }

    func createClient() -> HubClient {
        let cache = HubCache(cacheDirectory: Self.downloadDestination)
        return HubClient(
            host: URL(string: "https://huggingface.co")!,
            cache: cache
        )
    }

    /// Measures time to retrieve already-cached files using a commit hash revision.
    /// When the revision is a commit hash, we can skip API calls for cached files.
    @Test("Cached file retrieval with commit hash")
    func cachedFileRetrieval() async throws {
        let repoID: Repo.ID = "mlx-community/Qwen3-0.6B-Base-DQ5"
        let client = createClient()
        let destination = Self.downloadDestination.appending(path: "snapshot")
        let commitHash = "9a160ab0c112dda560cbaee3c9c24fbac2f98549"

        // First download to populate the cache
        _ = try await client.downloadSnapshot(
            of: repoID,
            to: destination,
            revision: commitHash,
            matching: ["*.json"]
        )

        let iterations = 5
        var times: [Double] = []

        printHeader(
            "Cached file retrieval",
            description: "Retrieving *.json from \(repoID) (already in cache, using commit hash)"
        )

        for i in 1 ... iterations {
            let start = CFAbsoluteTimeGetCurrent()
            _ = try await client.downloadSnapshot(
                of: repoID,
                to: destination,
                revision: commitHash,
                matching: ["*.json"]
            )
            let elapsed = (CFAbsoluteTimeGetCurrent() - start) * 1000
            times.append(elapsed)
            printIteration(i, time: elapsed)
        }

        printSummary(times: times)
    }

    /// Measures time to download multiple files concurrently (no cache).
    @Test("Fresh download")
    func freshDownload() async throws {
        let repoID: Repo.ID = "mlx-community/Qwen3-0.6B-Base-DQ5"
        let iterations = 5
        var times: [Double] = []

        printHeader(
            "Fresh download",
            description: "Downloading *.json from \(repoID) concurrently (no cache)"
        )

        for i in 1 ... iterations {
            // Clear cache before each iteration
            try? FileManager.default.removeItem(at: Self.downloadDestination)
            let client = createClient()
            let destination = Self.downloadDestination.appending(path: "snapshot")

            let start = CFAbsoluteTimeGetCurrent()
            _ = try await client.downloadSnapshot(
                of: repoID,
                to: destination,
                matching: ["*.json"]
            )
            let elapsed = (CFAbsoluteTimeGetCurrent() - start) * 1000
            times.append(elapsed)
            printIteration(i, time: elapsed)
        }

        printSummary(times: times)
    }

    private func printHeader(_ name: String, description: String) {
        print("")
        print("  \(name)")
        print("  \(description)")
        print("  ----------------------------------------")
    }

    private func printIteration(_ i: Int, time: Double) {
        print("  Run \(i): \(String(format: "%8.1f ms", time))")
    }

    private func printSummary(times: [Double]) {
        print("  ----------------------------------------")
        let average = times.reduce(0, +) / Double(times.count)
        print("  Avg:   \(String(format: "%8.1f ms", average))")
        print("")
    }
}
