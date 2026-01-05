// TODO: Delete this file before merging PR - kept only to demonstrate unreliable concurrent file access

import Foundation
import os

private let logger = Logger()

/// Original file lock implementation kept for comparison testing.
///
/// This implementation has a bug where multiple `FileLockOriginal` instances
/// for the same path each open their own file descriptor. Since `flock(2)` locks
/// are per file descriptor (not per file), each instance can acquire the "same" lock
/// simultaneously, defeating the purpose of locking.
///
/// Use `FileLock` from swift-filelock instead.
struct FileLockOriginal: Sendable {
    /// The path to the lock file.
    public let lockPath: URL

    /// Maximum number of lock acquisition attempts.
    public let maxRetries: Int

    /// Delay between retry attempts in seconds.
    public let retryDelay: TimeInterval

    /// Interval between log messages while waiting for a lock.
    private static let logInterval: Int = 10

    /// Creates a file lock for the specified path.
    ///
    /// - Parameters:
    ///   - path: The base path for the lock file. A `.lock` extension will be appended.
    ///   - maxRetries: Maximum number of lock acquisition attempts. Defaults to 600 (10 minutes with 1s delay).
    ///   - retryDelay: Delay between retry attempts in seconds. Defaults to 1.0.
    ///
    /// The default timeout is generous because the lock is held during file downloads,
    /// which can take minutes for large models. Python's `huggingface_hub` uses `WeakFileLock`
    /// which waits indefinitely by default and logs every 10 seconds while waiting. We use a
    /// 10-minute timeout as a practical upper bound while matching the logging behavior.
    public init(path: URL, maxRetries: Int = 600, retryDelay: TimeInterval = 1.0) {
        self.lockPath = path.appendingPathExtension("lock")
        self.maxRetries = maxRetries
        self.retryDelay = retryDelay
    }

    /// Executes the given closure while holding an exclusive lock.
    ///
    /// This method acquires an exclusive lock on the lock file, executes the closure,
    /// and then releases the lock. If the lock cannot be acquired after the maximum
    /// number of retries, an error is thrown.
    ///
    /// - Parameter body: The closure to execute while holding the lock.
    /// - Returns: The value returned by the closure.
    /// - Throws: `FileLockOriginalError.acquisitionFailed` if the lock cannot be acquired,
    ///           or any error thrown by the closure.
    public func withLockSync<T>(_ body: () throws -> T) throws -> T {
        let handle = try acquireLock()
        defer { releaseLock(handle) }
        return try body()
    }

    /// Executes the given async closure while holding an exclusive lock.
    ///
    /// - Parameter body: The async closure to execute while holding the lock.
    /// - Returns: The value returned by the closure.
    /// - Throws: `FileLockOriginalError.acquisitionFailed` if the lock cannot be acquired,
    ///           or any error thrown by the closure.
    public func withLock<T>(_ body: () async throws -> T) async throws -> T {
        let handle = try await acquireLockAsync()
        defer { releaseLock(handle) }
        return try await body()
    }

    // MARK: -

    private func prepareLockFile() throws -> FileHandle {
        try FileManager.default.createDirectory(
            at: lockPath.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )

        if !FileManager.default.fileExists(atPath: lockPath.path) {
            guard FileManager.default.createFile(atPath: lockPath.path, contents: nil) else {
                throw FileLockError.acquisitionFailed(lockPath)
            }
        }

        guard let handle = FileHandle(forWritingAtPath: lockPath.path) else {
            throw FileLockOriginalError.acquisitionFailed(lockPath)
        }

        return handle
    }

    private func tryLock(_ handle: FileHandle) -> Bool {
        flock(handle.fileDescriptor, LOCK_EX | LOCK_NB) == 0
    }

    private func acquireLock() throws -> FileHandle {
        let handle = try prepareLockFile()
        let startTime = CFAbsoluteTimeGetCurrent()

        for attempt in 0 ... maxRetries {
            if tryLock(handle) { return handle }
            if attempt < maxRetries {
                if attempt > 0, attempt % Self.logInterval == 0 {
                    let elapsed = CFAbsoluteTimeGetCurrent() - startTime
                    logger.info(
                        "Waiting for lock on \(self.lockPath.lastPathComponent) (elapsed: \(String(format: "%.1f", elapsed))s)"
                    )
                }
                Thread.sleep(forTimeInterval: retryDelay)
            }
        }

        try? handle.close()
        throw FileLockOriginalError.acquisitionFailed(lockPath)
    }

    private func acquireLockAsync() async throws -> FileHandle {
        let handle = try prepareLockFile()
        let startTime = CFAbsoluteTimeGetCurrent()

        for attempt in 0 ... maxRetries {
            if tryLock(handle) { return handle }
            if attempt < maxRetries {
                if attempt > 0, attempt % Self.logInterval == 0 {
                    let elapsed = CFAbsoluteTimeGetCurrent() - startTime
                    logger.info(
                        "Waiting for lock on \(self.lockPath.lastPathComponent) (elapsed: \(String(format: "%.1f", elapsed))s)"
                    )
                }
                try await Task.sleep(for: .seconds(retryDelay))
            }
        }

        try? handle.close()
        throw FileLockOriginalError.acquisitionFailed(lockPath)
    }

    private func releaseLock(_ handle: FileHandle) {
        flock(handle.fileDescriptor, LOCK_UN)
        try? handle.close()
        // Lock file left in place to avoid race conditions (see tox-dev/filelock#31).
    }
}

/// Errors that can occur during file locking operations.
public enum FileLockOriginalError: Error, LocalizedError {
    /// The lock could not be acquired after the maximum number of retries.
    case acquisitionFailed(URL)

    public var errorDescription: String? {
        switch self {
        case .acquisitionFailed(let path):
            return "Failed to acquire file lock at: \(path.path)"
        }
    }
}
