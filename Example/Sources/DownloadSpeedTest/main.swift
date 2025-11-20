import ArgumentParser
import Foundation
import HuggingFace

@main
struct DownloadSpeedTest: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "download-speed-test",
        abstract: "Benchmark download performance for Hugging Face repositories."
    )

    @Option(
        name: [.short, .long],
        help: "Repository identifier to benchmark (e.g. owner/name)."
    )
    var repo: String = "Qwen/Qwen3-0.6B"

    @Option(
        name: .long,
        help: "Minimum file size in MB to test (filters out small files). Default: 10 MB."
    )
    var minSizeMb: Int = 10

    @Flag(
        name: .long,
        inversion: .prefixedNo,
        help: "Enable Xet acceleration (use --no-xet to force classic LFS)."
    )
    var xet: Bool = HubClient.isXetSupported

    func run() async throws {
        guard let repoID = Repo.ID(rawValue: repo) else {
            throw ValidationError("Invalid repository identifier: \(repo). Expected format is owner/name.")
        }

        let client = HubClient(enableXet: xet)

        print("🚀 Hugging Face Download Speed Test")
        print("Repository: \(repoID)")
        print("=" * 60)
        print()

        if client.isXetEnabled {
            print("✅ Xet support: ENABLED")

            // Show Xet configuration (optimized defaults)
            if let concurrency = ProcessInfo.processInfo.environment["XET_NUM_CONCURRENT_RANGE_GETS"] {
                print("   Concurrent range GETs: \(concurrency)")
            } else {
                print("   Concurrent range GETs: 256 (optimized default)")
            }

            let highPerfDisabled = ProcessInfo.processInfo.environment["XET_HIGH_PERFORMANCE"] == "0"
            print("   High performance mode: \(highPerfDisabled ? "OFF (disabled)" : "ON (default)")")

            print()
            print("   💡 To adjust settings, set XET_NUM_CONCURRENT_RANGE_GETS or XET_HIGH_PERFORMANCE=0")
        } else {
            print("❌ Xet support: DISABLED (using LFS)")
        }
        print()

        print("📋 Listing files in repository...")
        do {
            let testFiles: [Git.TreeEntry]

            // Auto-select large files
            let files = try await client.listFiles(
                in: repoID,
                kind: .model,
                revision: "main",
                recursive: true
            )

            testFiles = Self.selectTestFiles(from: files, minSizeMB: minSizeMb)

            if testFiles.isEmpty {
                print("⚠️  No suitable test files found (minimum size: \(minSizeMb) MB)")
                print("💡 Try lowering --min-size-mb or specify a file with --file")
                return
            }

            print("📦 Selected \(testFiles.count) files for testing:")
            for file in testFiles {
                let size = file.size.map { Self.formatBytes(Int64($0)) } ?? "unknown size"
                print("   • \(file.path) (\(size))")
            }
            print()

            let tempDir = FileManager.default.temporaryDirectory
                .appendingPathComponent("hf-speed-test-\(UUID().uuidString)")
            try FileManager.default.createDirectory(
                at: tempDir,
                withIntermediateDirectories: true
            )
            defer {
                try? FileManager.default.removeItem(at: tempDir)
            }

            var totalTime: TimeInterval = 0
            var totalBytes: Int = 0

            print("⬇️  Starting download tests...")
            print()

            for (index, file) in testFiles.enumerated() {
                let destination = tempDir.appendingPathComponent(file.path)

                try? FileManager.default.createDirectory(
                    at: destination.deletingLastPathComponent(),
                    withIntermediateDirectories: true
                )

                let startTime = Date()

                do {
                    _ = try await client.downloadFile(
                        at: file.path,
                        from: repoID,
                        to: destination,
                        kind: .model,
                        revision: "main"
                    )

                    let elapsed = Date().timeIntervalSince(startTime)
                    let fileSize = file.size ?? 0
                    let speed = fileSize > 0 ? Double(fileSize) / elapsed : 0

                    totalTime += elapsed
                    totalBytes += fileSize

                    print("✅ [\(index + 1)/\(testFiles.count)] \(file.path)")
                    print("   Time: \(String(format: "%.2f", elapsed))s")
                    print("   Size: \(Self.formatBytes(Int64(fileSize)))")
                    print("   Speed: \(Self.formatBytes(Int64(speed)))/s")
                    print()
                } catch {
                    print("❌ [\(index + 1)/\(testFiles.count)] \(file.path)")
                    print("   Error: \(error.localizedDescription)")
                    print()
                }
            }

            print("=" * 60)
            print("📊 Summary")
            print("=" * 60)
            print("Total files: \(testFiles.count)")
            print("Total time: \(String(format: "%.2f", totalTime))s")
            print("Total size: \(Self.formatBytes(Int64(totalBytes)))")
            if totalTime > 0 {
                let avgSpeed = Double(totalBytes) / totalTime
                print("Average speed: \(Self.formatBytes(Int64(avgSpeed)))/s")
            }
            print()
            print("💡 Tip: toggle Xet via --xet / --no-xet to compare backends.")

        } catch {
            print("❌ Error: \(error.localizedDescription)")
            throw ExitCode.failure
        }
    }

    static func selectTestFiles(from files: [Git.TreeEntry], minSizeMB: Int) -> [Git.TreeEntry] {
        let minSizeBytes = minSizeMB * 1024 * 1024

        // Filter files by minimum size first (Xet excels at large files)
        let largeFiles = files.filter { file in
            file.type == .file && (file.size ?? 0) >= minSizeBytes
        }

        guard !largeFiles.isEmpty else {
            return []
        }

        var selected: [Git.TreeEntry] = []

        // Prioritize model files (safetensors, bin) as they're typically large
        let priorities = [
            "*.safetensors",
            "*.bin",
            "*.gguf",
            "*.pt",
            "*.pth",
        ]

        for priority in priorities {
            let pattern = priority.replacingOccurrences(of: "*", with: "")
            if let file = largeFiles.first(where: { $0.path.contains(pattern) }) {
                if !selected.contains(where: { $0.path == file.path }) {
                    selected.append(file)
                }
            }
        }

        // If we need more files, add the largest remaining ones
        if selected.count < 3 {
            let remaining = largeFiles.filter { file in
                !selected.contains(where: { $0.path == file.path })
            }

            let sorted = remaining.sorted { ($0.size ?? 0) > ($1.size ?? 0) }
            selected.append(contentsOf: sorted.prefix(3 - selected.count))
        }

        // Return up to 3 large files for benchmarking
        return Array(selected.prefix(3))
    }

    static func formatBytes(_ bytes: Int64) -> String {
        let formatter = ByteCountFormatter()
        formatter.allowedUnits = [.useKB, .useMB, .useGB]
        formatter.countStyle = .file
        return formatter.string(fromByteCount: bytes)
    }
}

extension String {
    static func * (lhs: String, rhs: Int) -> String {
        return String(repeating: lhs, count: rhs)
    }
}
