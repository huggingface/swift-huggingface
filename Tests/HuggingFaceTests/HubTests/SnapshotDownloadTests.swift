import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Testing

@testable import HuggingFace

/// Thread-safe wrapper around AsyncStream.Continuation for use in @Sendable closures.
private final class StreamYielder<T: Sendable>: Sendable {
    private let continuation: AsyncStream<T>.Continuation

    init(_ continuation: AsyncStream<T>.Continuation) {
        self.continuation = continuation
    }

    func yield(_ value: T) {
        continuation.yield(value)
    }

    func finish() {
        continuation.finish()
    }
}

/// Creates a progress stream for capturing progress updates in tests.
private func makeProgressStream() -> (
    stream: AsyncStream<Double>,
    yielder: StreamYielder<Double>
) {
    let (stream, continuation) = AsyncStream.makeStream(of: Double.self)
    return (stream, StreamYielder(continuation))
}

#if swift(>=6.1)
    @Suite("Snapshot Download Tests", .serialized)
    struct SnapshotDownloadTests {
        static let downloadDestination: URL = {
            let base = FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask).first!
            return base.appending(component: "huggingface-snapshot-tests")
        }()

        init() {
            // Clean before each test to ensure consistent starting state
            try? FileManager.default.removeItem(at: Self.downloadDestination)
        }

        func createClient(useOfflineMode: Bool? = nil) -> HubClient {
            let cache = HubCache(cacheDirectory: Self.downloadDestination)
            return HubClient(
                host: URL(string: "https://huggingface.co")!,
                cache: cache,
                useOfflineMode: useOfflineMode
            )
        }

        // MARK: - Basic Snapshot Download Tests

        @Test("Download snapshot with glob pattern")
        func downloadSnapshotWithGlob() async throws {
            let repoID: Repo.ID = "google-t5/t5-base"
            let client = createClient()
            let destination = Self.downloadDestination.appending(path: "snapshot")

            let result = try await client.downloadSnapshot(
                of: repoID,
                to: destination,
                matching: ["*.json"]
            )

            #expect(FileManager.default.fileExists(atPath: result.path))

            // Verify JSON files exist
            let configPath = destination.appendingPathComponent("config.json")
            #expect(FileManager.default.fileExists(atPath: configPath.path))
        }

        @Test("Download snapshot tracks progress")
        func downloadSnapshotTracksProgress() async throws {
            let repoID: Repo.ID = "google-t5/t5-base"
            // Use a separate directory to ensure fresh download (not cached)
            let cacheDir = Self.downloadDestination.appending(path: "progress-cache")
            let destination = Self.downloadDestination.appending(path: "progress-snapshot")
            try? FileManager.default.removeItem(at: cacheDir)
            try? FileManager.default.removeItem(at: destination)

            let cache = HubCache(cacheDirectory: cacheDir)
            let client = HubClient(
                host: URL(string: "https://huggingface.co")!,
                cache: cache
            )
            let (progressStream, yielder) = makeProgressStream()

            _ = try await client.downloadSnapshot(
                of: repoID,
                to: destination,
                matching: ["*.json"]
            ) { progress in
                yielder.yield(progress.fractionCompleted)
            }
            yielder.finish()

            // Get the last progress value from the stream
            var lastProgress: Double = 0
            for await progress in progressStream {
                lastProgress = progress
            }
            #expect(lastProgress == 1.0)
        }

        @Test("Download snapshot reports aggregate speed")
        func downloadSnapshotReportsSpeed() async throws {
            // Use qwen repo with larger files to ensure measurable download time
            let repoID: Repo.ID = "mlx-community/Qwen3-0.6B-Base-DQ5"
            // Use a separate directory to ensure fresh download (not cached)
            let cacheDir = Self.downloadDestination.appending(path: "speed-cache")
            let destination = Self.downloadDestination.appending(path: "speed-snapshot")
            try? FileManager.default.removeItem(at: cacheDir)
            try? FileManager.default.removeItem(at: destination)

            let cache = HubCache(cacheDirectory: cacheDir)
            let client = HubClient(
                host: URL(string: "https://huggingface.co")!,
                cache: cache
            )

            var lastSpeed: Double?
            // Download safetensors file (larger, takes longer)
            _ = try await client.downloadSnapshot(
                of: repoID,
                to: destination,
                matching: ["*.safetensors"]
            ) { progress, speed in
                if let speed = speed {
                    lastSpeed = speed
                }
            }

            // The final progress callback should have speed computed
            #expect(lastSpeed != nil, "Expected speed to be reported")
            if let speed = lastSpeed {
                #expect(speed > 0, "Speed should be positive")
            }
        }

        // MARK: - Cache Tests

        @Test("Second download uses cache")
        func secondDownloadUsesCache() async throws {
            let repoID: Repo.ID = "google-t5/t5-base"
            let client = createClient()
            let destination = Self.downloadDestination.appending(path: "snapshot")

            // First download (populates cache)
            _ = try await client.downloadSnapshot(
                of: repoID,
                to: destination,
                matching: ["config.json"]
            )

            // Record file timestamp
            let configPath = destination.appendingPathComponent("config.json")
            let attrs1 = try FileManager.default.attributesOfItem(atPath: configPath.path)
            let timestamp1 = attrs1[.modificationDate] as! Date

            // Short delay
            try await Task.sleep(nanoseconds: 100_000_000)

            // Second download (should use cache, not re-download)
            _ = try await client.downloadSnapshot(
                of: repoID,
                to: destination,
                matching: ["config.json"]
            )

            let attrs2 = try FileManager.default.attributesOfItem(atPath: configPath.path)
            let timestamp2 = attrs2[.modificationDate] as! Date

            // File should not have been modified (cache hit)
            #expect(timestamp1 == timestamp2)
        }

        // MARK: - Offline Mode Tests

        @Test("Offline mode returns cached files")
        func offlineModeReturnsCachedFiles() async throws {
            let repoID: Repo.ID = "google-t5/t5-base"
            // Use a separate cache directory for this test to avoid conflicts with init() cleanup
            let cacheDir = Self.downloadDestination.appending(path: "offline-cache")
            let destination = Self.downloadDestination.appending(path: "offline-snapshot")

            // Clean up any previous state
            try? FileManager.default.removeItem(at: cacheDir)
            try? FileManager.default.removeItem(at: destination)

            // Use shared cache for both clients
            let cache = HubCache(cacheDirectory: cacheDir)

            // First download with online client
            let onlineClient = HubClient(
                host: URL(string: "https://huggingface.co")!,
                cache: cache,
                useOfflineMode: false
            )
            _ = try await onlineClient.downloadSnapshot(
                of: repoID,
                to: destination,
                matching: ["config.json"]
            )

            // Remove the destination file but keep the cache
            try FileManager.default.removeItem(at: destination)

            // Verify cache has files
            let snapshotPath = cache.snapshotPath(repo: repoID, kind: .model, revision: "main")
            #expect(snapshotPath != nil, "Cache should have snapshot path for main")

            // Now download with offline client using same cache
            // Note: Offline mode returns all cached files, ignoring globs (matches huggingface_hub)
            let offlineClient = HubClient(
                host: URL(string: "https://huggingface.co")!,
                cache: cache,
                useOfflineMode: true
            )
            let result = try await offlineClient.downloadSnapshot(
                of: repoID,
                to: destination
            )

            #expect(FileManager.default.fileExists(atPath: result.path))
            #expect(FileManager.default.fileExists(atPath: destination.appendingPathComponent("config.json").path))
        }

        @Test("Offline mode fails without cache")
        func offlineModeFailsWithoutCache() async throws {
            let repoID: Repo.ID = "unknown-user/unknown-repo-that-is-not-cached"
            let client = createClient(useOfflineMode: true)
            let destination = Self.downloadDestination.appending(path: "snapshot")

            await #expect(throws: HubCacheError.self) {
                _ = try await client.downloadSnapshot(
                    of: repoID,
                    to: destination
                )
            }
        }

        // MARK: - Download with Revision Tests

        @Test("Download with specific commit hash")
        func downloadWithCommitHash() async throws {
            let repoID: Repo.ID = "google-t5/t5-base"
            let client = createClient()
            let destination = Self.downloadDestination.appending(path: "snapshot")

            // Get the current commit hash
            let model = try await client.getModel(repoID)
            let commitHash = try #require(model.sha)

            let result = try await client.downloadSnapshot(
                of: repoID,
                to: destination,
                revision: commitHash,
                matching: ["config.json"]
            )

            #expect(FileManager.default.fileExists(atPath: result.path))
        }

        @Test("Commit hash revision skips API calls when cached")
        func commitHashSkipsAPIWhenCached() async throws {
            let repoID: Repo.ID = "google-t5/t5-base"
            let client = createClient()
            let destination = Self.downloadDestination.appending(path: "snapshot")

            // Get the current commit hash
            let model = try await client.getModel(repoID)
            let commitHash = try #require(model.sha)

            // First download to populate cache
            _ = try await client.downloadSnapshot(
                of: repoID,
                to: destination,
                revision: commitHash,
                matching: ["config.json"]
            )

            // Remove destination but keep cache
            try FileManager.default.removeItem(at: destination)

            // Download again with commit hash - should be very fast (no API calls)
            let start = CFAbsoluteTimeGetCurrent()
            _ = try await client.downloadSnapshot(
                of: repoID,
                to: destination,
                revision: commitHash,
                matching: ["config.json"]
            )
            let elapsed = CFAbsoluteTimeGetCurrent() - start

            // Should complete in under 100ms (just file copy, no network)
            // Network calls would take at least a few hundred ms
            #expect(elapsed < 0.1, "Expected cache hit to be fast (< 100ms), got \(elapsed * 1000)ms")
            #expect(FileManager.default.fileExists(atPath: destination.appendingPathComponent("config.json").path))
        }

        @Test("Download with invalid revision throws error")
        func downloadWithInvalidRevision() async throws {
            let repoID: Repo.ID = "google-t5/t5-base"
            let client = createClient()
            let destination = Self.downloadDestination.appending(path: "snapshot")

            await #expect(throws: Error.self) {
                _ = try await client.downloadSnapshot(
                    of: repoID,
                    to: destination,
                    revision: "nonexistent-revision",
                    matching: ["config.json"]
                )
            }
        }

        // MARK: - Resume Download Tests

        @Test("Download cleans up incomplete files on success")
        func downloadCleansUpIncompleteFile() async throws {
            let repoID: Repo.ID = "google-t5/t5-base"
            // Use separate directory to ensure fresh download
            let cacheDir = Self.downloadDestination.appending(path: "resume-cache")
            let destination = Self.downloadDestination.appending(path: "resume-snapshot")
            try? FileManager.default.removeItem(at: cacheDir)
            try? FileManager.default.removeItem(at: destination)

            let cache = HubCache(cacheDirectory: cacheDir)
            let client = HubClient(
                host: URL(string: "https://huggingface.co")!,
                cache: cache
            )

            _ = try await client.downloadSnapshot(
                of: repoID,
                to: destination,
                matching: ["config.json"]
            )

            // Verify the file exists
            let filePath = destination.appendingPathComponent("config.json")
            #expect(FileManager.default.fileExists(atPath: filePath.path))

            // Verify no .incomplete files remain (incomplete files include etag in name)
            let contents = try FileManager.default.contentsOfDirectory(atPath: destination.path)
            let incompleteFiles = contents.filter { $0.contains(".incomplete") }
            #expect(incompleteFiles.isEmpty, "Expected no incomplete files, found: \(incompleteFiles)")
        }

        @Test("Direct file download cleans up incomplete files")
        func downloadFileDirectlyCleansUp() async throws {
            let repoID: Repo.ID = "google-t5/t5-base"
            // Use separate directory
            let cacheDir = Self.downloadDestination.appending(path: "file-resume-cache")
            let destination = Self.downloadDestination.appending(path: "file-resume-dest")
            try? FileManager.default.removeItem(at: cacheDir)
            try? FileManager.default.removeItem(at: destination)
            try FileManager.default.createDirectory(at: destination, withIntermediateDirectories: true)

            let cache = HubCache(cacheDirectory: cacheDir)
            let client = HubClient(
                host: URL(string: "https://huggingface.co")!,
                cache: cache
            )

            let fileDest = destination.appendingPathComponent("config.json")

            // Download file directly (not via snapshot)
            _ = try await client.downloadFile(
                at: "config.json",
                from: repoID,
                to: fileDest
            )

            // Verify file exists and is valid
            #expect(FileManager.default.fileExists(atPath: fileDest.path))
            let data = try Data(contentsOf: fileDest)
            #expect(data.count > 0)

            // Verify no incomplete files remain
            let contents = try FileManager.default.contentsOfDirectory(atPath: destination.path)
            let incompleteFiles = contents.filter { $0.contains(".incomplete") }
            #expect(incompleteFiles.isEmpty, "Expected no incomplete files, found: \(incompleteFiles)")
        }
    }
#endif
