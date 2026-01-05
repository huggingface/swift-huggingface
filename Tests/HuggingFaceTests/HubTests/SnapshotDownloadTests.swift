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
            let timestamp1 = try #require(attrs1[.modificationDate] as? Date)

            // Short delay
            try await Task.sleep(nanoseconds: 100_000_000)

            // Second download (should use cache, not re-download)
            _ = try await client.downloadSnapshot(
                of: repoID,
                to: destination,
                matching: ["config.json"]
            )

            let attrs2 = try FileManager.default.attributesOfItem(atPath: configPath.path)
            let timestamp2 = try #require(attrs2[.modificationDate] as? Date)

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

        // MARK: - Resume From Incomplete File Tests

        @Test("Download with existing incomplete file succeeds")
        func downloadWithExistingIncompleteFile() async throws {
            // Verifies that downloads complete successfully when an incomplete file exists.
            // The code sends a Range header to resume, but the server may return either:
            // - 206 Partial Content: resume works, incomplete prefix is preserved
            // - 200 OK: server doesn't support Range, code falls back to full download
            //
            // Both behaviors are correct. This test verifies the code handles both cases
            // and produces a valid final file regardless of server behavior.
            let repoID: Repo.ID = "google-t5/t5-base"
            let filename = "config.json"

            let cacheDir = Self.downloadDestination.appending(path: "resume-incomplete-cache")
            let destination = Self.downloadDestination.appending(path: "resume-incomplete-dest")
            try? FileManager.default.removeItem(at: cacheDir)
            try? FileManager.default.removeItem(at: destination)
            try FileManager.default.createDirectory(at: destination, withIntermediateDirectories: true)

            let cache = HubCache(cacheDirectory: cacheDir)
            let client = HubClient(
                host: URL(string: "https://huggingface.co")!,
                cache: cache
            )

            // Download the file to get its etag and content
            let fileDest = destination.appendingPathComponent(filename)
            _ = try await client.downloadFile(
                at: filename,
                from: repoID,
                to: fileDest
            )

            let completeContent = try Data(contentsOf: fileDest)
            #expect(completeContent.count > 100, "Test file should be larger than 100 bytes")

            // Find the blob and its etag
            let blobsDir = cache.blobsDirectory(repo: repoID, kind: .model)
            let blobs = try FileManager.default.contentsOfDirectory(atPath: blobsDir.path)
                .filter { !$0.hasSuffix(".lock") && !$0.hasSuffix(".incomplete") }
            let etag = try #require(blobs.first, "Should have at least one blob")

            // Delete the blob to force re-download
            let blobPath = blobsDir.appendingPathComponent(etag)
            try FileManager.default.removeItem(at: blobPath)

            // Delete snapshot symlinks
            let snapshotsDir = cache.snapshotsDirectory(repo: repoID, kind: .model)
            if let contents = try? FileManager.default.contentsOfDirectory(atPath: snapshotsDir.path) {
                for commitDir in contents {
                    let snapshotFile =
                        snapshotsDir
                        .appendingPathComponent(commitDir)
                        .appendingPathComponent(filename)
                    try? FileManager.default.removeItem(at: snapshotFile)
                }
            }

            // Create an incomplete file with partial real content
            let partialContent = completeContent.prefix(completeContent.count / 2)
            let incompletePath = blobsDir.appendingPathComponent("\(etag).incomplete")
            try Data(partialContent).write(to: incompletePath)

            // Delete destination
            try FileManager.default.removeItem(at: fileDest)

            // Download again with incomplete file present
            _ = try await client.downloadFile(
                at: filename,
                from: repoID,
                to: fileDest
            )

            // Verify the final file is correct
            let finalContent = try Data(contentsOf: fileDest)
            #expect(finalContent == completeContent, "Final file should match expected content")

            // Verify incomplete file was cleaned up
            #expect(
                !FileManager.default.fileExists(atPath: incompletePath.path),
                "Incomplete file should be removed after successful download"
            )

            // Verify blob exists
            #expect(
                FileManager.default.fileExists(atPath: blobPath.path),
                "Blob should exist after download"
            )
        }

        @Test("Download handles oversized incomplete file (416 scenario)")
        func handleOversizedIncompleteFile() async throws {
            // This test verifies graceful handling when an incomplete file is larger than
            // the actual file. When a Range header requests bytes beyond the file size,
            // the server returns 416 Range Not Satisfiable. The code should:
            // 1. Delete the oversized incomplete file
            // 2. Retry the download from scratch
            // 3. Complete successfully with correct content
            //
            // Note: We can't directly verify the 416 response without HTTP mocking,
            // but we verify the observable outcome: correct content despite bad state.
            let repoID: Repo.ID = "google-t5/t5-base"
            let filename = "config.json"

            let cacheDir = Self.downloadDestination.appending(path: "oversized-incomplete-cache")
            let destination = Self.downloadDestination.appending(path: "oversized-incomplete-dest")
            try? FileManager.default.removeItem(at: cacheDir)
            try? FileManager.default.removeItem(at: destination)
            try FileManager.default.createDirectory(at: destination, withIntermediateDirectories: true)

            let cache = HubCache(cacheDirectory: cacheDir)
            let client = HubClient(
                host: URL(string: "https://huggingface.co")!,
                cache: cache
            )

            // First download to get the etag and correct content
            let fileDest = destination.appendingPathComponent(filename)
            _ = try await client.downloadFile(
                at: filename,
                from: repoID,
                to: fileDest
            )

            let completeContent = try Data(contentsOf: fileDest)
            let actualSize = completeContent.count

            // Find the etag
            let blobsDir = cache.blobsDirectory(repo: repoID, kind: .model)
            let blobs = try FileManager.default.contentsOfDirectory(atPath: blobsDir.path)
                .filter { !$0.hasSuffix(".lock") && !$0.hasSuffix(".incomplete") }
            let etag = try #require(blobs.first)

            // Delete the blob and snapshot to force re-download
            let blobPath = blobsDir.appendingPathComponent(etag)
            try FileManager.default.removeItem(at: blobPath)

            let snapshotsDir = cache.snapshotsDirectory(repo: repoID, kind: .model)
            if let contents = try? FileManager.default.contentsOfDirectory(atPath: snapshotsDir.path) {
                for commitDir in contents {
                    let snapshotFile =
                        snapshotsDir
                        .appendingPathComponent(commitDir)
                        .appendingPathComponent(filename)
                    try? FileManager.default.removeItem(at: snapshotFile)
                }
            }

            // Create an OVERSIZED incomplete file with garbage data
            // This simulates a corrupted/stale incomplete file larger than the actual file
            let oversizedContent = Data(repeating: 0xFF, count: actualSize + 1000)
            let incompletePath = blobsDir.appendingPathComponent("\(etag).incomplete")
            try oversizedContent.write(to: incompletePath)

            #expect(
                oversizedContent.count > actualSize,
                "Incomplete file (\(oversizedContent.count) bytes) must be larger than actual file (\(actualSize) bytes)"
            )

            // Delete destination
            try FileManager.default.removeItem(at: fileDest)

            // Download again - should handle 416 by deleting incomplete and retrying
            _ = try await client.downloadFile(
                at: filename,
                from: repoID,
                to: fileDest
            )

            // Verify the file is correct, not the garbage data
            let downloadedContent = try Data(contentsOf: fileDest)
            #expect(downloadedContent == completeContent, "Downloaded content should match original")
            #expect(
                downloadedContent.count == actualSize,
                "Downloaded file should be \(actualSize) bytes, not \(downloadedContent.count)"
            )

            // Verify incomplete file was cleaned up
            #expect(
                !FileManager.default.fileExists(atPath: incompletePath.path),
                "Incomplete file should be removed"
            )
        }

        // MARK: - Concurrent Download Tests

        @Test("Concurrent downloads of same file use locking")
        func concurrentDownloadsUseLocking() async throws {
            // Verifies that concurrent downloads of the same file are properly
            // serialized via file locking, preventing corruption.
            let repoID: Repo.ID = "google-t5/t5-base"
            let filename = "config.json"

            let cacheDir = Self.downloadDestination.appending(path: "concurrent-cache")
            try? FileManager.default.removeItem(at: cacheDir)

            let cache = HubCache(cacheDirectory: cacheDir)
            let client = HubClient(
                host: URL(string: "https://huggingface.co")!,
                cache: cache
            )

            // Launch concurrent downloads of the same file
            let concurrentCount = 5
            try await withThrowingTaskGroup(of: URL.self) { group in
                for i in 0 ..< concurrentCount {
                    let dest = Self.downloadDestination.appending(path: "concurrent-dest-\(i)")
                    try? FileManager.default.removeItem(at: dest)
                    try FileManager.default.createDirectory(at: dest, withIntermediateDirectories: true)

                    group.addTask {
                        try await client.downloadFile(
                            at: filename,
                            from: repoID,
                            to: dest.appendingPathComponent(filename)
                        )
                    }
                }

                var results: [URL] = []
                for try await result in group {
                    results.append(result)
                }

                #expect(results.count == concurrentCount, "All downloads should complete")
            }

            // Verify only one blob exists (content-addressed, deduplicated)
            let blobsDir = cache.blobsDirectory(repo: repoID, kind: .model)
            let blobs = try FileManager.default.contentsOfDirectory(atPath: blobsDir.path)
                .filter { !$0.hasSuffix(".lock") && !$0.hasSuffix(".incomplete") }
            #expect(blobs.count == 1, "Should have exactly one blob (deduplicated)")

            // Verify all destination files have identical content
            var contents: [Data] = []
            for i in 0 ..< concurrentCount {
                let dest = Self.downloadDestination
                    .appending(path: "concurrent-dest-\(i)")
                    .appendingPathComponent(filename)
                let data = try Data(contentsOf: dest)
                contents.append(data)
            }

            let firstContent = contents[0]
            for (i, content) in contents.enumerated() {
                #expect(content == firstContent, "File \(i) should match first file")
            }

            // Verify no incomplete files remain
            let incompleteFiles = try FileManager.default.contentsOfDirectory(atPath: blobsDir.path)
                .filter { $0.hasSuffix(".incomplete") }
            #expect(incompleteFiles.isEmpty, "No incomplete files should remain")
        }

    }
#endif
