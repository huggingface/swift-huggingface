import Foundation
import Testing

@testable import HuggingFace

/// Benchmarks run only when `HF_TOKEN` is available.
let benchmarksEnabled = ProcessInfo.processInfo.environment["HF_TOKEN"]?.isEmpty == false

@Suite("Hub Benchmarks", .serialized, .enabled(if: benchmarksEnabled))
struct DownloadSnapshotBenchmarks {
    static let cacheDirectory: URL = {
        let base = FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask).first!
        return base.appendingPathComponent("huggingface-benchmarks", isDirectory: true)
    }()

    init() {
        try? FileManager.default.removeItem(at: Self.cacheDirectory)
    }

    func createClient() -> HubClient {
        let cache = HubCache(cacheDirectory: Self.cacheDirectory)
        return HubClient(
            host: URL(string: "https://huggingface.co")!,
            cache: cache
        )
    }

    @Test("Cached snapshot retrieval with commit hash")
    func cachedSnapshotRetrieval() async throws {
        let repoID: Repo.ID = "mlx-community/Qwen3-0.6B-Base-DQ5"
        let commitHash = "9a160ab0c112dda560cbaee3c9c24fbac2f98549"
        let globs = ["*.json"]
        let client = createClient()
        let workload = try await resolveWorkload(
            client: client,
            repoID: repoID,
            revision: commitHash,
            matching: globs
        )

        _ = try await client.downloadSnapshot(
            of: repoID,
            revision: commitHash,
            matching: globs
        )

        let iterations = 5
        var times: [Double] = []
        var baselineChecksums: [String: String]?
        var checksumMatches = true

        printHeader(
            "Cached snapshot retrieval",
            description: "Retrieving *.json from \(repoID) (already cached, commit hash revision)"
        )
        printWorkload(workload, includeLargestFile: true)

        for i in 1 ... iterations {
            let start = CFAbsoluteTimeGetCurrent()
            let snapshotPath = try await client.downloadSnapshot(
                of: repoID,
                revision: commitHash,
                matching: globs
            )
            let elapsed = (CFAbsoluteTimeGetCurrent() - start) * 1000
            times.append(elapsed)

            let checksums = try computeChecksums(
                in: snapshotPath,
                for: workload.files.map(\.path)
            )
            if let baselineChecksums {
                if checksums != baselineChecksums {
                    checksumMatches = false
                }
                #expect(checksums == baselineChecksums)
            } else {
                baselineChecksums = checksums
            }

            printLatencyIteration(i, time: elapsed)
        }

        printLatencySummary(
            times: times
        )
        if !checksumMatches {
            printChecksums(checksums: baselineChecksums ?? [:])
            print("")
        }
    }

    @Test("Fresh snapshot download")
    func freshSnapshotDownload() async throws {
        let repoID: Repo.ID = "mlx-community/Qwen3-0.6B-Base-DQ5"
        let globs = ["*.json"]
        let revision = "main"
        let iterations = 5
        var times: [Double] = []
        var transferRates: [Double] = []
        var baselineChecksums: [String: String]?
        var checksumMatches = true
        let metadataClient = createClient()
        let workload = try await resolveWorkload(
            client: metadataClient,
            repoID: repoID,
            revision: revision,
            matching: globs
        )

        printHeader(
            "Fresh snapshot download",
            description: "Downloading *.json from \(repoID) (cold cache)"
        )
        printWorkload(workload, includeLargestFile: true)

        for i in 1 ... iterations {
            try? FileManager.default.removeItem(at: Self.cacheDirectory)
            let client = createClient()

            let start = CFAbsoluteTimeGetCurrent()
            let snapshotPath = try await client.downloadSnapshot(
                of: repoID,
                revision: revision,
                matching: globs
            )
            let elapsed = (CFAbsoluteTimeGetCurrent() - start) * 1000
            times.append(elapsed)
            let bytesPerSecond = transferRateBytesPerSecond(
                bytesTransferred: workload.totalBytes,
                elapsedMilliseconds: elapsed
            )
            transferRates.append(bytesPerSecond)

            let checksums = try computeChecksums(
                in: snapshotPath,
                for: workload.files.map(\.path)
            )
            if let baselineChecksums {
                if checksums != baselineChecksums {
                    checksumMatches = false
                }
                #expect(checksums == baselineChecksums)
            } else {
                baselineChecksums = checksums
            }

            printNetworkIteration(i, time: elapsed, bytesPerSecond: bytesPerSecond)
        }

        printNetworkSummary(
            times: times,
            transferRates: transferRates
        )
        if !checksumMatches {
            printChecksums(checksums: baselineChecksums ?? [:])
            print("")
        }
    }

    @Test("Fresh Xet large-file download")
    func freshXetLargeFileDownload() async throws {
        let repoID: Repo.ID = "mlx-community/Qwen3-0.6B-Base-DQ5"
        let globs = ["model.safetensors"]
        let revision = "main"
        let iterations = 3
        var times: [Double] = []
        var transferRates: [Double] = []
        var baselineChecksums: [String: String]?
        var checksumMatches = true
        let metadataClient = createClient()
        let workload = try await resolveWorkload(
            client: metadataClient,
            repoID: repoID,
            revision: revision,
            matching: globs
        )
        let checksumPaths = checksumFilePaths(
            from: workload,
            preferred: ["model.safetensors"]
        )

        printHeader(
            "Fresh Xet large-file download",
            description: "Downloading model.safetensors workload from \(repoID) (cold cache)"
        )
        printWorkload(workload, includeLargestFile: true)

        for i in 1 ... iterations {
            try? FileManager.default.removeItem(at: Self.cacheDirectory)
            let client = createClient()

            let start = CFAbsoluteTimeGetCurrent()
            let snapshotPath = try await client.downloadSnapshot(
                of: repoID,
                revision: revision,
                matching: globs
            )
            let elapsed = (CFAbsoluteTimeGetCurrent() - start) * 1000
            times.append(elapsed)
            let bytesPerSecond = transferRateBytesPerSecond(
                bytesTransferred: workload.totalBytes,
                elapsedMilliseconds: elapsed
            )
            transferRates.append(bytesPerSecond)

            let checksums = try computeChecksums(in: snapshotPath, for: checksumPaths)
            if let baselineChecksums {
                if checksums != baselineChecksums {
                    checksumMatches = false
                }
                #expect(checksums == baselineChecksums)
            } else {
                baselineChecksums = checksums
            }

            printNetworkIteration(i, time: elapsed, bytesPerSecond: bytesPerSecond)
        }

        printNetworkSummary(
            times: times,
            transferRates: transferRates
        )
        if !checksumMatches {
            printChecksums(checksums: baselineChecksums ?? [:])
            print("")
        }
    }

    private func printHeader(_ name: String, description: String) {
        print("")
        let headingPrefix = "--- \(name) "
        let headingWidth = 72
        let dashCount = max(3, headingWidth - headingPrefix.count)
        print(headingPrefix + String(repeating: "-", count: dashCount))
        print("    \(description)")
    }

    private func printLatencyIteration(_ i: Int, time: Double) {
        print("    Run \(i): \(String(format: "%8.1f ms", time))")
    }

    private func printNetworkIteration(_ i: Int, time: Double, bytesPerSecond: Double) {
        print(
            "    Run \(i): \(String(format: "%8.1f ms", time))  \(formatBytesPerSecond(bytesPerSecond))"
        )
    }

    private func printLatencySummary(times: [Double]) {
        let stats = latencyStats(for: times)
        print("")
        print(
            "    Median: \(String(format: "%.1f ms", stats.median))"
        )
        print(
            "    Mean:   \(String(format: "%.1f ms", stats.average)) (+/- \(String(format: "%.1f ms", stats.standardDeviation)))"
        )
        print(
            "    Range:  \(String(format: "%.1f ms", stats.minimum)) -- \(String(format: "%.1f ms", stats.maximum))"
        )
        print("")
    }

    private func printNetworkSummary(
        times: [Double],
        transferRates: [Double]
    ) {
        let stats = latencyStats(for: times)
        let transferRateStats = latencyStats(for: transferRates)
        print("")
        print(
            "    Median: \(String(format: "%.1f ms", stats.median)) \(formatBytesPerSecond(transferRateStats.median))"
        )
        print(
            "    Mean:   \(String(format: "%.1f ms", stats.average)) (+/- \(String(format: "%.1f ms", stats.standardDeviation)))"
        )
        print(
            "    Range:  \(String(format: "%.1f ms", stats.minimum)) -- \(String(format: "%.1f ms", stats.maximum))"
        )
        print("")
    }

    private func printChecksums(checksums: [String: String]) {
        print("    Checksums:")
        for (path, digest) in checksums.sorted(by: { $0.key < $1.key }) {
            print("      - \(path): \(digest)")
        }
    }

    private func resolveWorkload(
        client: HubClient,
        repoID: Repo.ID,
        revision: String,
        matching globs: [String]
    ) async throws -> SnapshotWorkload {
        let entries = try await client.listFiles(in: repoID, revision: revision)
        let files =
            entries
            .filter { $0.type == .file && matches(path: $0.path, globs: globs) }
            .map {
                SnapshotWorkload.File(
                    path: $0.path,
                    sizeBytes: Int64($0.size ?? 0)
                )
            }
            .sorted { $0.path < $1.path }
        let totalBytes = files.reduce(Int64(0)) { $0 + $1.sizeBytes }
        return SnapshotWorkload(files: files, totalBytes: totalBytes)
    }

    private func matches(path: String, globs: [String]) -> Bool {
        if globs.isEmpty { return true }
        return globs.contains { glob in
            switch glob {
            case "*":
                return true
            case let suffixPattern where suffixPattern.hasPrefix("*."):
                return path.hasSuffix(String(suffixPattern.dropFirst(1)))
            default:
                return path == glob
            }
        }
    }

    private func printWorkload(_ workload: SnapshotWorkload, includeLargestFile: Bool) {
        let fileLabel = workload.files.count == 1 ? "file" : "files"
        if includeLargestFile, let largestFile = workload.files.max(by: { $0.sizeBytes < $1.sizeBytes }) {
            print(
                "    \(workload.files.count) \(fileLabel), \(formatBytes(workload.totalBytes)) total (largest: \(largestFile.path), \(formatBytes(largestFile.sizeBytes)))"
            )
        } else {
            print("    \(workload.files.count) \(fileLabel), \(formatBytes(workload.totalBytes)) total")
        }
        print("")
    }

    private func formatBytes(_ bytes: Int64) -> String {
        let formatter = MeasurementFormatter()
        formatter.unitOptions = .naturalScale
        return formatter.string(from: Measurement(value: Double(bytes), unit: UnitInformationStorage.bytes))
    }

    private func formatBytesPerSecond(_ bytesPerSecond: Double) -> String {
        let formatter = MeasurementFormatter()
        formatter.unitOptions = .naturalScale
        let throughput = formatter.string(
            from: Measurement(value: bytesPerSecond, unit: UnitInformationStorage.bytes)
        )
        return "\(throughput)/s"
    }

    private func transferRateBytesPerSecond(bytesTransferred: Int64, elapsedMilliseconds: Double) -> Double {
        let seconds = max(elapsedMilliseconds / 1000, 0.000_001)
        return Double(bytesTransferred) / seconds
    }

    private func latencyStats(for values: [Double]) -> LatencyStats {
        let sorted = values.sorted()
        let count = sorted.count
        let average = sorted.reduce(0, +) / Double(count)
        let median: Double
        if count % 2 == 0 {
            median = (sorted[(count / 2) - 1] + sorted[count / 2]) / 2
        } else {
            median = sorted[count / 2]
        }
        let variance =
            sorted
            .map { ($0 - average) * ($0 - average) }
            .reduce(0, +) / Double(count)

        return LatencyStats(
            minimum: sorted.first ?? 0,
            maximum: sorted.last ?? 0,
            average: average,
            median: median,
            standardDeviation: sqrt(variance)
        )
    }

    private func checksumFilePaths(from workload: SnapshotWorkload, preferred: [String]) -> [String] {
        let available = Set(workload.files.map(\.path))
        let selectedPreferred = preferred.filter { available.contains($0) }
        if !selectedPreferred.isEmpty {
            return selectedPreferred
        }
        return workload.files.prefix(2).map(\.path)
    }

    private func computeChecksums(in snapshotPath: URL, for filePaths: [String]) throws -> [String: String] {
        var result: [String: String] = [:]
        for path in filePaths {
            let fileURL = snapshotPath.appending(path: path)
            result[path] = try fnv1a64ChecksumHex(of: fileURL)
        }
        return result
    }

    private func fnv1a64ChecksumHex(of fileURL: URL) throws -> String {
        let handle = try FileHandle(forReadingFrom: fileURL)
        defer {
            try? handle.close()
        }

        var hash: UInt64 = 0xcbf29ce484222325
        let prime: UInt64 = 0x00000100000001B3

        while let chunk = try handle.read(upToCount: 1_048_576), !chunk.isEmpty {
            for byte in chunk {
                hash ^= UInt64(byte)
                hash &*= prime
            }
        }

        return String(format: "%016llx", hash)
    }

    private struct SnapshotWorkload {
        struct File {
            let path: String
            let sizeBytes: Int64
        }

        let files: [File]
        let totalBytes: Int64
    }

    private struct LatencyStats {
        let minimum: Double
        let maximum: Double
        let average: Double
        let median: Double
        let standardDeviation: Double
    }
}
