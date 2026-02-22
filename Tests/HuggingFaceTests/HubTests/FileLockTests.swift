import Foundation
import Testing

@testable import HuggingFace

#if canImport(Darwin)
    import Darwin
#elseif canImport(Glibc)
    import Glibc
#endif

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

    // MARK: - Basic Locking

    @Test("Basic lock acquisition and release")
    func basicLockAcquisition() async throws {
        let targetPath = tempDirectory.appendingPathComponent("test-file")
        let lock = FileLock(path: targetPath)

        var executed = false
        try await lock.withLock {
            executed = true
        }

        #expect(executed)
        #expect(FileManager.default.fileExists(atPath: lock.lockPath.path))
    }

    @Test("Lock file is created with .lock extension")
    func lockFileCreated() async throws {
        let targetPath = tempDirectory.appendingPathComponent("test-file.txt")
        let lock = FileLock(path: targetPath)

        try await lock.withLock {
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

    @Test("Lock blocks other acquirers")
    func lockBlocksOtherAcquirers() async throws {
        let targetPath = tempDirectory.appendingPathComponent("blocks")
        let lock1 = FileLock(path: targetPath)
        let lock2 = FileLock(path: targetPath, maxRetries: 5, retryDelay: 0.05)

        let lock1Acquired = Expectation()
        let lock1CanRelease = Expectation()
        let lock2Acquired = Expectation()

        Task {
            try await lock1.withLock {
                lock1Acquired.fulfill()
                await lock1CanRelease.wait(timeout: 5)
            }
        }

        await lock1Acquired.wait(timeout: 5)

        Task {
            try await lock2.withLock {
                lock2Acquired.fulfill()
            }
        }

        try await Task.sleep(for: .milliseconds(100))
        #expect(!lock2Acquired.isFulfilled)

        lock1CanRelease.fulfill()
        await lock2Acquired.wait(timeout: 5)
        #expect(lock2Acquired.isFulfilled)
    }

    @Test("Lock protects critical section")
    func lockProtectsCriticalSection() async throws {
        let targetPath = tempDirectory.appendingPathComponent("counter")
        let counterFile = tempDirectory.appendingPathComponent("counter.txt")
        try "0".write(to: counterFile, atomically: true, encoding: .utf8)

        let iterations = 100
        try await withThrowingTaskGroup(of: Void.self) { group in
            for _ in 0 ..< iterations {
                group.addTask {
                    let lock = FileLock(path: targetPath, maxRetries: 500, retryDelay: 0.01)
                    try await lock.withLock {
                        let current = Int(try String(contentsOf: counterFile, encoding: .utf8)) ?? 0
                        try String(current + 1).write(to: counterFile, atomically: true, encoding: .utf8)
                    }
                }
            }
            try await group.waitForAll()
        }

        let finalValue = Int(try String(contentsOf: counterFile, encoding: .utf8)) ?? 0
        #expect(finalValue == iterations)
    }

    // MARK: - Reentrant Locking

    @Test("Reentrant locking with same path")
    func reentrantLocking() async throws {
        let targetPath = tempDirectory.appendingPathComponent("reentrant")
        let lock1 = FileLock(path: targetPath)
        var innerExecuted = false

        try await lock1.withLock {
            let lock2 = FileLock(path: targetPath)
            try await lock2.withLock {
                innerExecuted = true
            }
        }

        #expect(innerExecuted)
    }

    @Test("Nested locking with same lock object")
    func nestedLockingSameObject() async throws {
        let targetPath = tempDirectory.appendingPathComponent("nested-same")
        let lock = FileLock(path: targetPath)
        var level1 = false
        var level2 = false
        var level3 = false

        try await lock.withLock {
            level1 = true
            try await lock.withLock {
                level2 = true
                try await lock.withLock {
                    level3 = true
                }
            }
        }
        #expect(level1 && level2 && level3)
    }

    @Test("Reentrant locking survives nested different lock path")
    func reentrantLockingAcrossDifferentNestedPath() async throws {
        let outerPath = tempDirectory.appendingPathComponent("outer-reentrant")
        let innerPath = tempDirectory.appendingPathComponent("inner-reentrant")
        let outerLock = FileLock(path: outerPath)
        let innerLock = FileLock(path: innerPath)
        var reenteredOuter = false

        try await outerLock.withLock {
            try await innerLock.withLock {
                try await outerLock.withLock(blocking: false) {
                    reenteredOuter = true
                }
            }
        }

        #expect(reenteredOuter)
    }

    @Test("Lock released on exception")
    func lockReleasedOnException() async throws {
        struct TestError: Error {}
        let targetPath = tempDirectory.appendingPathComponent("exception")
        let lock1 = FileLock(path: targetPath)

        do {
            try await lock1.withLock {
                throw TestError()
            }
        } catch is TestError {}

        var acquired = false
        try await FileLock(path: targetPath, maxRetries: 1, retryDelay: 0.01).withLock {
            acquired = true
        }
        #expect(acquired)
    }

    // MARK: - Concurrent Access

    @Test("Concurrent tasks with separate lock instances")
    func concurrentTasksWithSeparateLockInstances() async throws {
        let targetPath = tempDirectory.appendingPathComponent("thrash")
        let dataFile = tempDirectory.appendingPathComponent("thrash.txt")
        let taskCount = 50
        let iterationsPerTask = 3

        try await withThrowingTaskGroup(of: Void.self) { group in
            for taskIndex in 0 ..< taskCount {
                group.addTask {
                    let lock = FileLock(path: targetPath, maxRetries: 500, retryDelay: 0.01)
                    try await lock.withLock {
                        for iteration in 0 ..< iterationsPerTask {
                            let uuid = UUID().uuidString
                            try uuid.write(to: dataFile, atomically: true, encoding: .utf8)
                            let readBack = try String(contentsOf: dataFile, encoding: .utf8)
                            guard readBack == uuid else {
                                throw FileLockTestError.dataMismatch(
                                    expected: uuid,
                                    got: readBack,
                                    iteration: taskIndex * iterationsPerTask + iteration
                                )
                            }
                        }
                    }
                }
            }
            try await group.waitForAll()
        }
    }

    // MARK: - File Permissions

    @Test("Lock file has configured permissions")
    func lockFilePermissions() async throws {
        let targetPath = tempDirectory.appendingPathComponent("mode")
        let lock = FileLock(path: targetPath, mode: 0o666)

        try await lock.withLock {
            let attributes = try FileManager.default.attributesOfItem(atPath: lock.lockPath.path)
            let permissions = (attributes[.posixPermissions] as? Int) ?? 0
            #expect(permissions == 0o666)
        }
    }

    @Test("Default lock file permissions")
    func defaultLockFilePermissions() async throws {
        let targetPath = tempDirectory.appendingPathComponent("default-mode")
        let lock = FileLock(path: targetPath)

        try await lock.withLock {
            let attributes = try FileManager.default.attributesOfItem(atPath: lock.lockPath.path)
            let permissions = (attributes[.posixPermissions] as? Int) ?? 0
            #expect(permissions == 0o644)
        }
    }

    // MARK: - Non-blocking Mode

    @Test("Non-blocking mode fails immediately when lock is held")
    func nonBlockingMode() async throws {
        let targetPath = tempDirectory.appendingPathComponent("nonblocking")
        let lock1 = FileLock(path: targetPath)
        let holdingLock = Expectation()

        Task {
            try await lock1.withLock {
                holdingLock.fulfill()
                try await Task.sleep(for: .seconds(2))
            }
        }

        await holdingLock.wait(timeout: 5)

        let lock2 = FileLock(path: targetPath, blocking: false)
        let startTime = ContinuousClock.now
        var didFail = false
        do {
            _ = try await lock2.withLock {
                Issue.record("non-blocking lock should have failed")
            }
        } catch is FileLockError {
            didFail = true
        }
        let elapsed = ContinuousClock.now - startTime
        #expect(didFail)
        #expect(elapsed < .milliseconds(100))
    }

    @Test("Non-blocking mode succeeds when lock is available")
    func nonBlockingModeSuccess() async throws {
        let targetPath = tempDirectory.appendingPathComponent("nonblocking-success")
        let lock = FileLock(path: targetPath, blocking: false)
        var executed = false
        try await lock.withLock {
            executed = true
        }
        #expect(executed)
    }

    @Test("Blocking parameter override on withLock")
    func blockingOverride() async throws {
        let targetPath = tempDirectory.appendingPathComponent("blocking-override")
        let lock1 = FileLock(path: targetPath)
        let holdingLock = Expectation()

        Task {
            try await lock1.withLock {
                holdingLock.fulfill()
                try await Task.sleep(for: .seconds(2))
            }
        }
        await holdingLock.wait(timeout: 5)

        let lock2 = FileLock(path: targetPath, blocking: true)
        var didFail = false
        do {
            _ = try await lock2.withLock(blocking: false) {
                Issue.record("should have failed with blocking override")
            }
        } catch is FileLockError {
            didFail = true
        }
        #expect(didFail)
    }

    // MARK: - Error Handling

    @Test("Lock fails when path is a directory")
    func lockFailsWhenPathIsDirectory() async throws {
        let targetPath = tempDirectory.appendingPathComponent("somedir")
        // FileLock appends ".lock", so make that resulting lock path a directory.
        try FileManager.default.createDirectory(
            at: targetPath.appendingPathExtension("lock"),
            withIntermediateDirectories: true
        )

        let lock = FileLock(path: targetPath, maxRetries: 0)
        do {
            _ = try await lock.withLock {
                Issue.record("should not acquire lock on a directory")
            }
        } catch let error as FileLockError {
            switch error {
            case .openFailed(_, let err):
                #expect(err == EISDIR)
            default:
                Issue.record("expected openFailed, got \(error)")
            }
        }
    }

    @Test("Lock succeeds with nested non-existent directories")
    func lockSucceedsWithNestedDirectories() async throws {
        let deepPath =
            tempDirectory
            .appendingPathComponent("a")
            .appendingPathComponent("b")
            .appendingPathComponent("c")
            .appendingPathComponent("resource")
        let lock = FileLock(path: deepPath)
        var executed = false
        try await lock.withLock {
            executed = true
        }
        #expect(executed)
        #expect(FileManager.default.fileExists(atPath: lock.lockPath.path))
    }

    @Test("Lock acquisition timeout")
    func lockAcquisitionTimeout() async throws {
        let targetPath = tempDirectory.appendingPathComponent("timeout")
        let lock1 = FileLock(path: targetPath)
        let holdingLock = Expectation()
        let released = Expectation()

        Task {
            try await lock1.withLock {
                holdingLock.fulfill()
                try? await Task.sleep(for: .seconds(2))
            }
            released.fulfill()
        }

        await holdingLock.wait(timeout: 5)

        do {
            _ = try await FileLock(path: targetPath, maxRetries: 2, retryDelay: 0.1).withLock {
                Issue.record("should not have acquired lock")
            }
        } catch is FileLockError {}

        await released.wait(timeout: 5)
    }

    @Test("Lock fails on read-only directory")
    func lockFailsOnReadOnlyDirectory() async throws {
        if geteuid() == 0 {
            return
        }

        let readOnlyDir = tempDirectory.appendingPathComponent("readonly")
        try FileManager.default.createDirectory(at: readOnlyDir, withIntermediateDirectories: true)
        try FileManager.default.setAttributes([.posixPermissions: 0o555], ofItemAtPath: readOnlyDir.path)
        defer {
            try? FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: readOnlyDir.path)
        }

        let targetPath = readOnlyDir.appendingPathComponent("test")
        let lock = FileLock(path: targetPath, maxRetries: 0)

        do {
            _ = try await lock.withLock {
                Issue.record("should not acquire lock in read-only directory")
            }
        } catch let error as FileLockError {
            switch error {
            case .openFailed(_, let err):
                #expect(err == EACCES || err == EPERM)
            default:
                Issue.record("expected openFailed, got \(error)")
            }
        }
    }

    // MARK: - HubCache Integration

    @Test("HubCache storeFile uses locking")
    func hubCacheStoreFileUsesLocking() async throws {
        let cache = HubCache(cacheDirectory: tempDirectory)
        let repoID: Repo.ID = "user/repo"
        let commitHash = "abc123def456789012345678901234567890abcd"
        let etag = "shared-etag"
        let content = "test content"

        let sourceFile1 = tempDirectory.appendingPathComponent("source1.txt")
        let sourceFile2 = tempDirectory.appendingPathComponent("source2.txt")
        try content.write(to: sourceFile1, atomically: true, encoding: .utf8)
        try content.write(to: sourceFile2, atomically: true, encoding: .utf8)

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

        let blobPath = cache.blobsDirectory(repo: repoID, kind: .model)
            .appendingPathComponent(etag)
        #expect(FileManager.default.fileExists(atPath: blobPath.path))

        let storedContent = try String(contentsOf: blobPath, encoding: .utf8)
        #expect(storedContent == content)
    }
}

enum FileLockTestError: Error, CustomStringConvertible {
    case dataMismatch(expected: String, got: String, iteration: Int)

    var description: String {
        switch self {
        case .dataMismatch(let expected, let got, let iteration):
            return "Data mismatch at iteration \(iteration): expected '\(expected)', got '\(got)'"
        }
    }
}

final class Expectation: Sendable {
    private let fulfilled = Mutex(false)

    var isFulfilled: Bool {
        fulfilled.withLock { $0 }
    }

    func fulfill() {
        fulfilled.withLock { $0 = true }
    }

    func wait(timeout: TimeInterval) async {
        let deadline = ContinuousClock.now + .seconds(timeout)
        while !fulfilled.withLock({ $0 }) {
            if ContinuousClock.now >= deadline {
                return
            }
            try? await Task.sleep(for: .milliseconds(10))
        }
    }
}

private final class Mutex<Value>: @unchecked Sendable {
    private var value: Value
    private let lock = NSLock()

    init(_ value: Value) {
        self.value = value
    }

    func withLock<T>(_ body: (inout Value) -> T) -> T {
        lock.lock()
        defer { lock.unlock() }
        return body(&value)
    }
}
