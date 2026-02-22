import Foundation

#if canImport(Darwin)
    import Darwin
#elseif canImport(Glibc)
    import Glibc
#endif

/// POSIX file permission bits,
/// expressed as an octal value (e.g. `0o644`).
///
/// This is a thin wrapper around the C `mode_t` type
/// used by `open(2)` and `fchmod(2)`.
public typealias FilePermissions = mode_t

/// A file-based lock for coordinating access to shared resources
/// across concurrent tasks and processes.
///
/// `FileLock` creates a `.lock` companion file next to the protected path
/// and uses `flock(2)` for mutual exclusion.
/// Nested ``withLock(blocking:_:)`` calls within the same task hierarchy
/// are reentrant and do not deadlock.
public struct FileLock: Sendable {
    /// The path to the `.lock` file.
    public let lockPath: URL

    /// Maximum number of acquisition attempts after the initial try,
    /// or `nil` to retry indefinitely.
    public let maxRetries: Int?

    /// Seconds to wait between retry attempts.
    public let retryDelay: TimeInterval

    /// POSIX permissions applied when creating the lock file.
    public let mode: FilePermissions

    /// When `true`, acquisition retries up to ``maxRetries`` times;
    /// when `false`, a single failed attempt throws immediately.
    public let blocking: Bool

    // Reentrant locking uses @TaskLocal ownership tracking:
    // nested withLock calls increment a counter rather than
    // re-acquiring the flock, and child tasks inherit the owner ID.
    @TaskLocal private static var currentOwnerID: UInt64?

    private static let ownerIDGenerator = OwnerIDGenerator()

    /// Creates a file lock that protects the resource at `path`.
    ///
    /// The lock file is placed at `path` with an appended `.lock` extension.
    ///
    /// - Parameters:
    ///   - path: The path to the resource being protected.
    ///   - maxRetries: Maximum retry attempts before giving up,
    ///     or `nil` to retry indefinitely.
    ///     Defaults to `5`.
    ///   - retryDelay: Seconds between retries.
    ///     Defaults to `1.0`.
    ///   - mode: POSIX permissions for the lock file.
    ///     Defaults to `0o644`.
    ///   - blocking: Whether to retry on contention.
    ///     Defaults to `true`.
    public init(
        path: URL,
        maxRetries: Int? = 5,
        retryDelay: TimeInterval = 1.0,
        mode: FilePermissions = 0o644,
        blocking: Bool = true
    ) {
        self.lockPath = path.appendingPathExtension("lock")
        self.maxRetries = maxRetries
        self.retryDelay = retryDelay
        self.mode = mode
        self.blocking = blocking
    }

    /// Acquires an exclusive lock, executes `body`, then releases.
    ///
    /// Nested calls within the same task hierarchy are reentrant
    /// and do not block.
    ///
    /// - Parameters:
    ///   - blocking: Overrides the instance-level ``blocking`` setting
    ///     for this single invocation.
    ///   - body: The work to perform under the lock.
    /// - Returns: The value returned by `body`.
    /// - Throws: ``FileLockError/acquisitionFailed(_:attempts:totalWaitTime:)``
    ///   if the lock cannot be obtained,
    ///   or any error thrown by `body`.
    public func withLock<T>(
        blocking: Bool? = nil,
        _ body: () async throws -> T
    ) async throws -> T {
        let key = lockPath.path(percentEncoded: false)
        let context = await FileLockContext.shared(for: key)

        // Fast path: reentrant acquisition within the same task hierarchy.
        if let ownerID = Self.currentOwnerID,
            await context.tryReentrantAcquire(ownerID: ownerID)
        {
            do {
                let result = try await body()
                await context.decrementCounter()
                return result
            } catch {
                await context.decrementCounter()
                throw error
            }
        }

        // Slow path: acquire the underlying flock.
        let ownerID = await Self.ownerIDGenerator.nextID()
        let shouldBlock = blocking ?? self.blocking
        try await acquireLock(context: context, ownerID: ownerID, blocking: shouldBlock)

        do {
            let result = try await Self.$currentOwnerID.withValue(ownerID) {
                try await body()
            }
            await releaseLock(context: context, force: false)
            return result
        } catch {
            await releaseLock(context: context, force: false)
            throw error
        }
    }

    /// Opens (or creates) the lock file, returning its file descriptor.
    ///
    /// Parent directories are created as needed.
    /// `O_NOFOLLOW` prevents symlink-based attacks on non-Windows platforms.
    private func openLockFile() throws -> Int32 {
        try FileManager.default.createDirectory(
            at: lockPath.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )

        var flags: Int32 = O_RDWR | O_CREAT | O_TRUNC
        #if !os(Windows)
            flags |= O_NOFOLLOW
        #endif

        let fd = open(lockPath.path(percentEncoded: false), flags, mode)
        guard fd >= 0 else {
            throw FileLockError.openFailed(lockPath, errno: errno)
        }

        // Apply permissions (may silently fail if not the file owner).
        _ = fchmod(fd, mode)
        return fd
    }

    /// Result of a single non-blocking `flock(2)` attempt.
    private enum LockResult {
        case success
        case wouldBlock
        case notSupported
        case error(Int32)
    }

    /// Attempts a non-blocking exclusive `flock(2)`,
    /// automatically retrying on `EINTR`.
    private func tryLock(_ fd: Int32) -> LockResult {
        var result: Int32
        repeat {
            result = flock(fd, LOCK_EX | LOCK_NB)
        } while result == -1 && errno == EINTR

        if result == 0 { return .success }

        switch errno {
        case ENOSYS: return .notSupported
        case EWOULDBLOCK: return .wouldBlock
        default: return .error(errno)
        }
    }

    /// Polls `tryLock` in a retry loop with `Task.sleep` between attempts.
    ///
    /// The file descriptor is closed on any error path
    /// (including `CancellationError` from `Task.sleep`).
    private func acquireLock(
        context: FileLockContext,
        ownerID: UInt64,
        blocking: Bool
    ) async throws {
        let fd = try openLockFile()
        let startTime = Date()
        var attempt = 0

        do {
            while true {
                switch tryLock(fd) {
                case .success:
                    await context.recordAcquisition(fd: fd, owner: ownerID)
                    return
                case .notSupported:
                    throw FileLockError.notSupported(lockPath)
                case .error(let errnum):
                    throw FileLockError.lockFailed(lockPath, errno: errnum)
                case .wouldBlock:
                    if !blocking {
                        throw FileLockError.acquisitionFailed(
                            lockPath,
                            attempts: 1,
                            totalWaitTime: 0
                        )
                    }
                    if let maxRetries, attempt >= maxRetries {
                        throw FileLockError.acquisitionFailed(
                            lockPath,
                            attempts: attempt + 1,
                            totalWaitTime: Date().timeIntervalSince(startTime)
                        )
                    }
                    attempt += 1
                    try await Task.sleep(for: .seconds(retryDelay))
                }
            }
        } catch {
            close(fd)
            throw error
        }
    }

    /// Decrements the reentrant lock counter and,
    /// if it reaches zero (or `force` is `true`),
    /// unlocks and closes the file descriptor.
    ///
    /// Lock files are intentionally retained after release
    /// to avoid race conditions with concurrent acquirers.
    private func releaseLock(context: FileLockContext, force: Bool) async {
        if let fd = await context.releaseIfReady(force: force) {
            flock(fd, LOCK_UN)
            close(fd)
        }
    }
}

/// Monotonically increasing counter for unique lock-owner identifiers.
private actor OwnerIDGenerator {
    private var nextOwnerID: UInt64 = 0

    func nextID() -> UInt64 {
        nextOwnerID &+= 1
        return nextOwnerID
    }
}

// Multiple FileLock instances targeting the same path share a single
// FileLockContext through this registry,
// so the underlying file descriptor is opened only once per path.
// Weak references allow contexts to be deallocated
// when no FileLock is actively using them.
private actor WeakContextRegistry {
    private final class WeakBox {
        weak var value: FileLockContext?
        init(_ value: FileLockContext) { self.value = value }
    }

    private var contexts: [String: WeakBox] = [:]

    /// Returns an existing context for `path`,
    /// or creates and caches a new one.
    func getOrCreate(_ path: String) -> FileLockContext {
        if contexts.count > 100 {
            contexts = contexts.filter { $0.value.value != nil }
        }

        if let existing = contexts[path]?.value {
            return existing
        }

        let context = FileLockContext()
        contexts[path] = WeakBox(context)
        return context
    }
}

private let contextRegistry = WeakContextRegistry()

/// Per-path mutable state backing one or more ``FileLock`` instances.
///
/// Tracks the open file descriptor, the owning task's identifier,
/// and a reentrant lock counter.
/// All mutations are serialized by the actor,
/// so compound check-and-mutate operations are atomic.
private actor FileLockContext {
    private var fileDescriptor: Int32?
    private var ownerID: UInt64?
    private var lockCounter: Int = 0

    /// Releases the file descriptor on deallocation
    /// to prevent leaked descriptors if the context
    /// is collected while still holding a lock.
    deinit {
        if let fd = fileDescriptor {
            flock(fd, LOCK_UN)
            close(fd)
        }
    }

    static func shared(for path: String) async -> FileLockContext {
        await contextRegistry.getOrCreate(path)
    }

    /// Increments the lock counter if `ownerID` matches the current owner,
    /// enabling reentrant acquisition without a second `flock(2)` call.
    ///
    /// - Returns: `true` if the caller is the current owner
    ///   and the counter was incremented.
    func tryReentrantAcquire(ownerID: UInt64) -> Bool {
        if self.ownerID == ownerID {
            lockCounter += 1
            return true
        }
        return false
    }

    /// Stores the file descriptor and owner after a successful `flock(2)`,
    /// resetting the counter to `1`.
    func recordAcquisition(fd: Int32, owner: UInt64) {
        fileDescriptor = fd
        ownerID = owner
        lockCounter = 1
    }

    /// Decrements the lock counter (floored at zero)
    /// without releasing the underlying file descriptor.
    func decrementCounter() {
        lockCounter = max(0, lockCounter - 1)
    }

    /// Decrements the counter and returns the file descriptor to close
    /// when the counter reaches zero (or `force` is `true`).
    ///
    /// - Returns: The file descriptor to unlock and close,
    ///   or `nil` if the lock should be retained.
    func releaseIfReady(force: Bool) -> Int32? {
        guard fileDescriptor != nil else { return nil }
        lockCounter -= 1

        if lockCounter <= 0 || force {
            let fd = fileDescriptor
            fileDescriptor = nil
            ownerID = nil
            lockCounter = 0
            return fd
        }

        return nil
    }
}

/// Errors that can occur during file locking operations.
public enum FileLockError: Error, LocalizedError, Sendable {
    /// The lock could not be acquired within the allowed attempts.
    case acquisitionFailed(URL, attempts: Int, totalWaitTime: TimeInterval)

    /// The lock file could not be opened.
    case openFailed(URL, errno: Int32)

    /// `flock(2)` failed with an unexpected (non-retryable) error.
    case lockFailed(URL, errno: Int32)

    /// The filesystem does not support `flock(2)` (`ENOSYS`).
    case notSupported(URL)

    public var errorDescription: String? {
        switch self {
        case .acquisitionFailed(let path, let attempts, let totalWaitTime):
            "Failed to acquire file lock at \(path.path(percentEncoded: false)) "
                + "after \(attempts) attempt\(attempts == 1 ? "" : "s") "
                + "(\(String(format: "%.1f", totalWaitTime))s)"
        case .openFailed(let path, let errno):
            "Failed to open lock file at \(path.path(percentEncoded: false)): "
                + String(cString: strerror(errno))
        case .lockFailed(let path, let errno):
            "Failed to lock file at \(path.path(percentEncoded: false)): "
                + String(cString: strerror(errno))
        case .notSupported(let path):
            "Filesystem does not support flock at: "
                + path.path(percentEncoded: false)
        }
    }
}
