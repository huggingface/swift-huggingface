import FileLock
import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Testing

@testable import HuggingFace

@Suite("FileLock Integration Tests")
struct FileLockTests {
    let tempDirectory: URL

    init() throws {
        tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent("FileLockTests-\(UUID().uuidString)")
        try FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true
        )
    }

    @Test("FileLock properly serializes concurrent access from separate instances")
    func fileLockProperSerialization() async throws {
        // This test demonstrates that the new FileLock from swift-filelock
        // properly serializes access even when multiple instances are created
        // for the same path, because it uses a shared context per path.

        let lockPath = tempDirectory.appendingPathComponent("proper-test.lock")
        let dataPath = tempDirectory.appendingPathComponent("counter2.txt")

        // Initialize counter file
        try "0".write(to: dataPath, atomically: true, encoding: .utf8)

        let iterations = 50
        await withTaskGroup(of: Void.self) { group in
            for _ in 0 ..< iterations {
                group.addTask {
                    // Each task creates its own FileLock instance, but they share context
                    let lock = await FileLock(lockPath: lockPath)
                    try? await lock.withLock {
                        // Read-modify-write is properly serialized
                        let current = Int(try! String(contentsOf: dataPath, encoding: .utf8))!
                        // Small delay to increase chance of race condition if lock is broken
                        try? await Task.sleep(for: .milliseconds(1))
                        try! String(current + 1).write(to: dataPath, atomically: true, encoding: .utf8)
                    }
                }
            }
        }

        let finalValue = Int(try String(contentsOf: dataPath, encoding: .utf8))!

        // With proper locking, finalValue should equal iterations (50)
        #expect(
            finalValue == iterations,
            "FileLock should properly serialize (got \(finalValue), expected \(iterations))"
        )
    }

    // MARK: - HubCache Integration Tests

    @Test("HubCache storeFile uses locking")
    func hubCacheStoreFileUsesLocking() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"
        let etag = "shared-etag"
        let content = "test content"

        // Create source files
        let sourceFile1 = tempDirectory.appendingPathComponent("source1.txt")
        let sourceFile2 = tempDirectory.appendingPathComponent("source2.txt")
        try content.write(to: sourceFile1, atomically: true, encoding: .utf8)
        try content.write(to: sourceFile2, atomically: true, encoding: .utf8)

        // Run concurrent stores to the same blob
        await withTaskGroup(of: Void.self) { group in
            for i in 0 ..< 5 {
                let sourceFile = i % 2 == 0 ? sourceFile1 : sourceFile2
                group.addTask {
                    try? await cache.storeFile(
                        at: sourceFile,
                        repo: repoID,
                        kind: .model,
                        revision: commitHash,
                        filename: "file-\(i).txt",
                        etag: etag
                    )
                }
            }
        }

        // Verify blob was created correctly
        let blobPath = cache.blobsDirectory(repo: repoID, kind: .model)
            .appendingPathComponent(etag)
        #expect(FileManager.default.fileExists(atPath: blobPath.path))

        let storedContent = try String(contentsOf: blobPath, encoding: .utf8)
        #expect(storedContent == content)
    }

    @Test("HubCache storeData uses locking")
    func hubCacheStoreDataUsesLocking() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"
        let etag = "data-etag"
        let content = "test data content"
        let data = Data(content.utf8)

        // Run concurrent stores to the same blob
        await withTaskGroup(of: Void.self) { group in
            for i in 0 ..< 5 {
                group.addTask {
                    try? await cache.storeData(
                        data,
                        repo: repoID,
                        kind: .model,
                        revision: commitHash,
                        filename: "data-\(i).txt",
                        etag: etag
                    )
                }
            }
        }

        // Verify blob was created correctly
        let blobPath = cache.blobsDirectory(repo: repoID, kind: .model)
            .appendingPathComponent(etag)
        #expect(FileManager.default.fileExists(atPath: blobPath.path))

        let storedContent = try String(contentsOf: blobPath, encoding: .utf8)
        #expect(storedContent == content)
    }
}
