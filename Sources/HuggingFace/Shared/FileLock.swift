import Foundation
import os

private let logger = Logger()

/// A file-based lock for coordinating access to shared resources.
///
/// `FileLock` provides file locking using `flock(2)` to enable safe
/// concurrent access to cache files from multiple processes.
///
/// ## Usage
///
/// ```swift
/// let lock = FileLock(path: metadataDir.appending(path: "file"))
/// try await lock.withLock {
///     // Exclusive access to the resource
///     try data.write(to: targetPath)
/// }
/// ```
///
/// The lock is automatically released when the closure completes or throws.
///
/// ## Lock File Location
///
/// The lock file is created at the specified path with a `.lock` extension.
/// Callers typically pass a path in a metadata directory to keep lock files hidden from users.
public struct FileLock: Sendable {
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
    /// - Throws: `FileLockError.acquisitionFailed` if the lock cannot be acquired,
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
    /// - Throws: `FileLockError.acquisitionFailed` if the lock cannot be acquired,
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
            throw FileLockError.acquisitionFailed(lockPath)
        }

        return handle
    }

    private func tryLock(_ handle: FileHandle) -> Bool {
        flock(handle.fileDescriptor, LOCK_EX | LOCK_NB) == 0
    }

    private func acquireLock() throws -> FileHandle {
        let handle = try prepareLockFile()
        let startTime = CFAbsoluteTimeGetCurrent()

        for attempt in 0...maxRetries {
            if tryLock(handle) { return handle }
            if attempt < maxRetries {
                if attempt > 0, attempt % Self.logInterval == 0 {
                    let elapsed = CFAbsoluteTimeGetCurrent() - startTime
                    logger.info("Waiting for lock on \(self.lockPath.lastPathComponent) (elapsed: \(String(format: "%.1f", elapsed))s)")
                }
                Thread.sleep(forTimeInterval: retryDelay)
            }
        }

        try? handle.close()
        throw FileLockError.acquisitionFailed(lockPath)
    }

    private func acquireLockAsync() async throws -> FileHandle {
        let handle = try prepareLockFile()
        let startTime = CFAbsoluteTimeGetCurrent()

        for attempt in 0...maxRetries {
            if tryLock(handle) { return handle }
            if attempt < maxRetries {
                if attempt > 0, attempt % Self.logInterval == 0 {
                    let elapsed = CFAbsoluteTimeGetCurrent() - startTime
                    logger.info("Waiting for lock on \(self.lockPath.lastPathComponent) (elapsed: \(String(format: "%.1f", elapsed))s)")
                }
                try await Task.sleep(for: .seconds(retryDelay))
            }
        }

        try? handle.close()
        throw FileLockError.acquisitionFailed(lockPath)
    }

    private func releaseLock(_ handle: FileHandle) {
        flock(handle.fileDescriptor, LOCK_UN)
        try? handle.close()
        // Lock file left in place to avoid race conditions (see tox-dev/filelock#31).
    }
}

/// Errors that can occur during file locking operations.
public enum FileLockError: Error, LocalizedError {
    /// The lock could not be acquired after the maximum number of retries.
    case acquisitionFailed(URL)

    public var errorDescription: String? {
        switch self {
        case .acquisitionFailed(let path):
            return "Failed to acquire file lock at: \(path.path)"
        }
    }
}
