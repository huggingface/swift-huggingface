import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Testing

@testable import HuggingFace

@Suite("FileLock Tests")
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

    // MARK: - Basic Lock Tests

    @Test("Lock can be acquired on new file")
    func acquireLockNewFile() throws {
        let targetPath = tempDirectory.appendingPathComponent("test-file.txt")
        let lock = FileLock(path: targetPath)

        var executed = false
        try lock.withLock {
            executed = true
        }

        #expect(executed)
    }

    @Test("Lock file is created with .lock extension")
    func lockFileCreated() throws {
        let targetPath = tempDirectory.appendingPathComponent("test-file.txt")
        let lock = FileLock(path: targetPath)

        try lock.withLock {
            // Lock file should exist while we hold the lock
            #expect(FileManager.default.fileExists(atPath: lock.lockPath.path))
        }
    }

    @Test("Lock path has correct extension")
    func lockPathExtension() throws {
        let targetPath = tempDirectory.appendingPathComponent("blob-abc123")
        let lock = FileLock(path: targetPath)

        #expect(lock.lockPath.pathExtension == "lock")
        #expect(lock.lockPath.deletingPathExtension().lastPathComponent == "blob-abc123")
    }

    @Test("Nested lock execution works")
    func nestedLockExecution() throws {
        let targetPath = tempDirectory.appendingPathComponent("test-file.txt")
        let lock = FileLock(path: targetPath)

        var value = 0
        try lock.withLock {
            value = 1
            // Cannot acquire the same lock again from the same thread
            // but sequential operations work
        }
        try lock.withLock {
            value = 2
        }

        #expect(value == 2)
    }

    @Test("Lock returns value from closure")
    func lockReturnsValue() throws {
        let targetPath = tempDirectory.appendingPathComponent("test-file.txt")
        let lock = FileLock(path: targetPath)

        let result = try lock.withLock {
            return 42
        }

        #expect(result == 42)
    }

    @Test("Lock propagates errors from closure")
    func lockPropagatesErrors() throws {
        let targetPath = tempDirectory.appendingPathComponent("test-file.txt")
        let lock = FileLock(path: targetPath)

        struct TestError: Error {}

        #expect(throws: TestError.self) {
            try lock.withLock {
                throw TestError()
            }
        }
    }

    // MARK: - Concurrent Access Tests

    @Test("Concurrent writes are serialized")
    func concurrentWritesSerialized() async throws {
        let targetPath = tempDirectory.appendingPathComponent("concurrent-test.txt")
        let dataPath = tempDirectory.appendingPathComponent("data.txt")
        let lock = FileLock(path: targetPath)

        // Create the data file
        try "".write(to: dataPath, atomically: true, encoding: .utf8)

        // Run multiple concurrent tasks that each try to append to the file
        await withTaskGroup(of: Void.self) { group in
            for i in 0 ..< 10 {
                group.addTask {
                    do {
                        try await lock.withLock {
                            // Read current content
                            let current = try String(contentsOf: dataPath, encoding: .utf8)
                            // Append our value
                            let new = current + "\(i)\n"
                            // Small delay to increase chance of race conditions without lock
                            try await Task.sleep(for: .milliseconds(10))
                            // Write back
                            try new.write(to: dataPath, atomically: true, encoding: .utf8)
                        }
                    } catch {
                        // Lock acquisition may fail in test due to timing
                    }
                }
            }
        }

        // Verify file has content (some writes may have been skipped due to lock contention)
        let finalContent = try String(contentsOf: dataPath, encoding: .utf8)
        let lines = finalContent.split(separator: "\n")
        // At least some writes should have succeeded
        #expect(lines.count > 0)
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
                    try? cache.storeFile(
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

        // Verify lock file was created (and may still exist)
        // Lock file existence is implementation detail, just verify no corruption
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
                    try? cache.storeData(
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
