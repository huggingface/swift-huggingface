import Foundation

/// A file-based lock for coordinating access to shared resources.
///
/// `FileLock` provides file locking using `flock(2)` to enable safe
/// concurrent access to cache files from multiple processes.
///
/// ## Usage
///
/// ```swift
/// let lock = FileLock(path: blobPath)
/// try lock.withLock {
///     // Exclusive access to the resource
///     try data.write(to: blobPath)
/// }
/// ```
///
/// The lock is automatically released when the closure completes or throws.
///
/// ## Lock File Location
///
/// The lock file is created alongside the target path with a `.lock` extension.
/// For example, locking `/cache/blobs/abc123` creates `/cache/blobs/abc123.lock`.
public struct FileLock: Sendable {
    /// The path to the lock file.
    public let lockPath: URL

    /// Maximum number of lock acquisition attempts.
    public let maxRetries: Int

    /// Delay between retry attempts in seconds.
    public let retryDelay: TimeInterval

    /// Creates a file lock for the specified path.
    ///
    /// - Parameters:
    ///   - path: The path to the resource being protected.
    ///   - maxRetries: Maximum number of lock acquisition attempts. Defaults to 5.
    ///   - retryDelay: Delay between retry attempts in seconds. Defaults to 1.0.
    public init(path: URL, maxRetries: Int = 5, retryDelay: TimeInterval = 1.0) {
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

        for attempt in 0 ... maxRetries {
            if tryLock(handle) { return handle }
            if attempt < maxRetries {
                Thread.sleep(forTimeInterval: retryDelay)
            }
        }

        try? handle.close()
        throw FileLockError.acquisitionFailed(lockPath)
    }

    private func acquireLockAsync() async throws -> FileHandle {
        let handle = try prepareLockFile()

        for attempt in 0 ... maxRetries {
            if tryLock(handle) { return handle }
            if attempt < maxRetries {
                try await Task.sleep(for: .seconds(retryDelay))
            }
        }

        try? handle.close()
        throw FileLockError.acquisitionFailed(lockPath)
    }

    private func releaseLock(_ handle: FileHandle) {
        flock(handle.fileDescriptor, LOCK_UN)
        try? handle.close()
        // Clean up the lock file after releasing
        try? FileManager.default.removeItem(at: lockPath)
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
