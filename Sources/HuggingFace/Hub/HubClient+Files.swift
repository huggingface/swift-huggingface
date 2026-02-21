import Crypto
import FileLock
import Foundation

#if canImport(UniformTypeIdentifiers)
    import UniformTypeIdentifiers
#endif

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

import Xet

private let xetMinimumFileSizeBytes = 16 * 1024 * 1024  // 16MiB

/// Controls which transport is used for file downloads.
public enum FileDownloadTransport: Hashable, CaseIterable, Sendable {
    /// Automatically select the best transport (Xet for large files, LFS otherwise).
    case automatic

    /// Force classic LFS download.
    case lfs

    /// Force Xet download (requires Xet support).
    case xet

    var shouldAttemptXet: Bool {
        switch self {
        case .automatic, .xet:
            return true
        case .lfs:
            return false
        }
    }

    func shouldUseXet(fileSizeBytes: Int?, minimumFileSizeBytes: Int?) -> Bool {
        switch self {
        case .xet:
            return true
        case .lfs:
            return false
        case .automatic:
            guard let minimumFileSizeBytes, let fileSizeBytes else {
                return true
            }
            return fileSizeBytes >= minimumFileSizeBytes
        }
    }
}

/// Controls which endpoint is used for file downloads.
public enum FileDownloadEndpoint: String, Hashable, CaseIterable, Sendable {
    /// Resolve endpoint (default behavior).
    case resolve

    /// Raw endpoint (bypass resolve redirects).
    case raw

    var pathComponent: String { rawValue }
}

// MARK: - Upload Operations

public extension HubClient {
    /// Upload a single file to a repository
    /// - Parameters:
    ///   - filePath: Local file path to upload
    ///   - repoPath: Destination path in repository
    ///   - repo: Repository identifier
    ///   - kind: Kind of repository (model, dataset, or space)
    ///   - branch: Target branch (default: "main")
    ///   - message: Commit message
    /// - Returns: Tuple of (path, commit) where commit may be nil
    func uploadFile(
        _ filePath: String,
        to repoPath: String,
        in repo: Repo.ID,
        kind: Repo.Kind = .model,
        branch: String = "main",
        message: String? = nil
    ) async throws -> (path: String, commit: String?) {
        let fileURL = URL(fileURLWithPath: filePath)
        return try await uploadFile(fileURL, to: repoPath, in: repo, kind: kind, branch: branch, message: message)
    }

    /// Upload a single file to a repository
    /// - Parameters:
    ///   - fileURL: Local file URL to upload
    ///   - path: Destination path in repository
    ///   - repo: Repository identifier
    ///   - kind: Kind of repository (model, dataset, or space)
    ///   - branch: Target branch (default: "main")
    ///   - message: Commit message
    /// - Returns: Tuple of (path, commit) where commit may be nil
    func uploadFile(
        _ fileURL: URL,
        to repoPath: String,
        in repo: Repo.ID,
        kind: Repo.Kind = .model,
        branch: String = "main",
        message: String? = nil
    ) async throws -> (path: String, commit: String?) {
        let url = httpClient.host
            .appending(path: "api")
            .appending(path: kind.pluralized)
            .appending(path: repo.namespace)
            .appending(path: repo.name)
            .appending(path: "upload")
            .appending(component: branch)
        var request = try await httpClient.createRequest(.post, url: url)

        let boundary = "----hf-\(UUID().uuidString)"
        request.setValue(
            "multipart/form-data; boundary=\(boundary)",
            forHTTPHeaderField: "Content-Type"
        )

        // Determine file size for streaming decision
        let fileSize = try fileURL.resourceValues(forKeys: [.fileSizeKey]).fileSize ?? 0
        let threshold = 10 * 1024 * 1024  // 10MB
        let shouldStream = fileSize >= threshold

        let mimeType = fileURL.mimeType

        if shouldStream {
            // Large file: stream from disk using URLSession.uploadTask
            request.setValue("100-continue", forHTTPHeaderField: "Expect")
            let tempFile = try MultipartBuilder(boundary: boundary)
                .addText(name: "path", value: repoPath)
                .addOptionalText(name: "message", value: message)
                .addFileStreamed(name: "file", fileURL: fileURL, mimeType: mimeType)
                .buildToTempFile()
            defer { try? FileManager.default.removeItem(at: tempFile) }

            let (data, response) = try await session.upload(for: request, fromFile: tempFile)
            _ = try httpClient.validateResponse(response, data: data)

            if data.isEmpty {
                return (path: repoPath, commit: nil)
            }

            let result = try JSONDecoder().decode(UploadResponse.self, from: data)
            return (path: result.path, commit: result.commit)
        } else {
            // Small file: build in memory
            let body = try MultipartBuilder(boundary: boundary)
                .addText(name: "path", value: repoPath)
                .addOptionalText(name: "message", value: message)
                .addFile(name: "file", fileURL: fileURL, mimeType: mimeType)
                .buildInMemory()

            let (data, response) = try await session.upload(for: request, from: body)
            _ = try httpClient.validateResponse(response, data: data)

            if data.isEmpty {
                return (path: repoPath, commit: nil)
            }

            let result = try JSONDecoder().decode(UploadResponse.self, from: data)
            return (path: result.path, commit: result.commit)
        }
    }

    /// Upload multiple files to a repository
    /// - Parameters:
    ///   - batch: Batch of files to upload (path: URL dictionary)
    ///   - repo: Repository identifier
    ///   - kind: Kind of repository
    ///   - branch: Target branch
    ///   - message: Commit message
    ///   - maxConcurrent: Maximum concurrent uploads
    /// - Returns: Array of (path, commit) tuples
    func uploadFiles(
        _ batch: FileBatch,
        to repo: Repo.ID,
        kind: Repo.Kind = .model,
        branch: String = "main",
        message: String,
        maxConcurrent: Int = 3
    ) async throws -> [(path: String, commit: String?)] {
        let entries = Array(batch)

        return try await withThrowingTaskGroup(
            of: (Int, (path: String, commit: String?)).self
        ) { group in
            var results: [(path: String, commit: String?)?] = Array(
                repeating: nil,
                count: entries.count
            )
            var activeCount = 0

            for (index, (path, entry)) in entries.enumerated() {
                // Limit concurrency
                while activeCount >= maxConcurrent {
                    if let (idx, result) = try await group.next() {
                        results[idx] = result
                        activeCount -= 1
                    }
                }

                group.addTask {
                    let result = try await self.uploadFile(
                        entry.url,
                        to: path,
                        in: repo,
                        kind: kind,
                        branch: branch,
                        message: message
                    )
                    return (index, result)
                }
                activeCount += 1
            }

            // Collect remaining results
            for try await (index, result) in group {
                results[index] = result
            }

            return results.compactMap { $0 }
        }
    }
}

// MARK: - Download Operations

/// Constants for download operations.
private enum DownloadConstants {
    /// Size of the buffer for streaming downloads (64 KB).
    static let bufferSize = 65536
    /// Interval between speed updates during downloads (250 ms).
    static let speedUpdateIntervalNanoseconds: UInt64 = 250_000_000
}

public extension HubClient {
    /// Download file data using URLSession.dataTask
    /// - Parameters:
    ///   - repoPath: Path to file in repository
    ///   - repo: Repository identifier
    ///   - kind: Kind of repository
    ///   - revision: Git revision (branch, tag, or commit)
    ///   - endpoint: Select resolve or raw endpoint
    ///   - cachePolicy: Cache policy for the request
    /// - Returns: File data
    func downloadContentsOfFile(
        at repoPath: String,
        from repo: Repo.ID,
        kind: Repo.Kind = .model,
        revision: String = "main",
        endpoint: FileDownloadEndpoint = .resolve,
        transport: FileDownloadTransport = .automatic,
        cachePolicy: URLRequest.CachePolicy = .useProtocolCachePolicy
    ) async throws -> Data {
        // Check cache first
        if let cachedPath = cache.cachedFilePath(
            repo: repo,
            kind: kind,
            revision: revision,
            filename: repoPath
        ) {
            return try Data(contentsOf: cachedPath)
        }

        if endpoint == .resolve, transport.shouldAttemptXet {
            do {
                if let data = try await downloadDataWithXet(
                    repoPath: repoPath,
                    repo: repo,
                    kind: kind,
                    revision: revision,
                    transport: transport
                ) {
                    return data
                }
            } catch {
                if transport == .xet {
                    throw error
                }
            }
        }

        // Fallback to existing LFS download method
        let endpoint = endpoint.pathComponent
        let url = httpClient.host
            .appending(path: repo.namespace)
            .appending(path: repo.name)
            .appending(path: endpoint)
            .appending(component: revision)
            .appending(path: repoPath)

        // Fetch metadata first (without following redirects) to get X-Linked-Etag for LFS/xet files
        let metadata = try await fetchFileMetadata(url: url)

        var request = try await httpClient.createRequest(.get, url: url)
        request.cachePolicy = cachePolicy

        let (data, response) = try await session.data(for: request)
        _ = try httpClient.validateResponse(response, data: data)

        // Store in cache if we have etag and commit info
        let etag = metadata.etag ?? (response as? HTTPURLResponse)?.value(forHTTPHeaderField: "ETag")
        let commitHash =
            metadata.commitHash ?? (response as? HTTPURLResponse)?.value(forHTTPHeaderField: "X-Repo-Commit")

        if let etag, let commitHash {
            try? await cache.storeData(
                data,
                repo: repo,
                kind: kind,
                revision: commitHash,
                filename: repoPath,
                etag: etag,
                ref: revision != commitHash ? revision : nil
            )
        }

        return data
    }

    /// Download a file to the cache with automatic resume support.
    ///
    /// Downloads the file to the blob cache and creates a snapshot symlink.
    /// Returns the snapshot symlink path, which resolves through to the blob.
    /// If the file is already cached, returns immediately with no network calls.
    ///
    /// If a previous download was interrupted, this method automatically resumes
    /// from where it left off using HTTP Range headers.
    ///
    /// - Parameters:
    ///   - repoPath: Path to file in repository
    ///   - repo: Repository identifier
    ///   - kind: Kind of repository
    ///   - revision: Git revision
    ///   - endpoint: Select resolve or raw endpoint
    ///   - cachePolicy: Cache policy for the request
    ///   - progress: Optional Progress object to track download progress
    ///   - transport: Download transport selection
    /// - Returns: Path to the cached file (snapshot symlink)
    func downloadFile(
        at repoPath: String,
        from repo: Repo.ID,
        kind: Repo.Kind = .model,
        revision: String = "main",
        to destination: URL? = nil,
        localFilesOnly: Bool = false,
        endpoint: FileDownloadEndpoint = .resolve,
        cachePolicy: URLRequest.CachePolicy = .useProtocolCachePolicy,
        progress: Progress? = nil,
        transport: FileDownloadTransport = .automatic
    ) async throws -> URL {
        // Check cache first
        if let cachedPath = cache.cachedFilePath(
            repo: repo,
            kind: kind,
            revision: revision,
            filename: repoPath
        ) {
            if let progress {
                progress.completedUnitCount = progress.totalUnitCount
            }
            return try copyToLocalDirectoryIfNeeded(
                cachedPath, repoPath: repoPath, localDirectory: destination
            )
        }

        if localFilesOnly {
            throw HubCacheError.offlineModeError(
                "File '\(repoPath)' not available in cache"
            )
        }

        if endpoint == .resolve, transport.shouldAttemptXet {
            do {
                if let downloaded = try await downloadFileWithXet(
                    repoPath: repoPath,
                    repo: repo,
                    kind: kind,
                    revision: revision,
                    progress: progress,
                    transport: transport
                ) {
                    return try copyToLocalDirectoryIfNeeded(
                        downloaded, repoPath: repoPath, localDirectory: destination
                    )
                }
            } catch {
                if transport == .xet {
                    throw error
                }
            }
        }

        // Fallback to existing LFS download method
        let endpoint = endpoint.pathComponent
        let url = httpClient.host
            .appending(path: repo.namespace)
            .appending(path: repo.name)
            .appending(path: endpoint)
            .appending(component: revision)
            .appending(path: repoPath)

        // Fetch metadata first (without following redirects) to get X-Linked-Etag for LFS/xet files
        let metadata = try await fetchFileMetadata(url: url)

        var request = try await httpClient.createRequest(.get, url: url)
        request.cachePolicy = cachePolicy

        // Use shared cache coordination (blob check, locking) for both platforms
        let cachedPath = try await downloadWithCacheCoordination(
            request: request,
            metadata: metadata,
            repo: repo,
            kind: kind,
            revision: revision,
            repoPath: repoPath,
            progress: progress
        )
        return try copyToLocalDirectoryIfNeeded(
            cachedPath, repoPath: repoPath, localDirectory: destination
        )
    }

    /// Downloads a file with cache coordination (blob check, locking) shared across platforms.
    ///
    /// This method handles:
    /// 1. Check if blob already exists → skip download
    /// 2. Acquire file lock to prevent parallel downloads
    /// 3. Platform-specific download (with resume on Apple, without on Linux)
    /// 4. Create snapshot symlink
    ///
    /// Returns the snapshot symlink path, which resolves through to the blob.
    /// Requires a cache to be configured.
    private func downloadWithCacheCoordination(
        request: URLRequest,
        metadata: FileMetadata?,
        repo: Repo.ID,
        kind: Repo.Kind,
        revision: String,
        repoPath: String,
        progress: Progress?
    ) async throws -> URL {
        let fileManager = FileManager.default

        // Etag is required for blob-based cache storage
        // Note: metadata.etag is already normalized by fetchFileMetadata
        guard let normalizedEtag = metadata?.etag else {
            throw HubCacheError.unexpectedAPIResponse("Server did not return an ETag header for '\(repoPath)'")
        }
        let blobsDir = cache.blobsDirectory(repo: repo, kind: kind)
        let blobPath = blobsDir.appendingPathComponent(normalizedEtag)
        let commitHash = metadata?.commitHash ?? revision

        // Check if blob already exists (skip download)
        if fileManager.fileExists(atPath: blobPath.path) {
            return try createCacheEntries(
                cache: cache,
                repo: repo,
                kind: kind,
                revision: revision,
                commitHash: commitHash,
                repoPath: repoPath,
                etag: normalizedEtag
            )
        }

        // Acquire lock to prevent parallel downloads of the same blob
        let locksDir = cache.locksDirectory(repo: repo, kind: kind)
        let lockPath = locksDir.appendingPathComponent(normalizedEtag)
        let lock = await FileLock(lockPath: lockPath.appendingPathExtension("lock"))
        return try await lock.withLock {
            // Double-check blob doesn't exist after acquiring lock
            if fileManager.fileExists(atPath: blobPath.path) {
                return try createCacheEntries(
                    cache: cache,
                    repo: repo,
                    kind: kind,
                    revision: revision,
                    commitHash: commitHash,
                    repoPath: repoPath,
                    etag: normalizedEtag
                )
            }

            // Create blobs directory
            try fileManager.createDirectory(at: blobsDir, withIntermediateDirectories: true)

            // Platform-specific download to cache
            #if canImport(FoundationNetworking)
                try await downloadToCacheLinux(
                    request: request,
                    blobPath: blobPath,
                    progress: progress
                )
            #else
                try await downloadToCacheApple(
                    request: request,
                    blobPath: blobPath,
                    etag: normalizedEtag,
                    blobsDir: blobsDir,
                    progress: progress
                )
            #endif

            // Create snapshot symlink and return path
            return try createCacheEntries(
                cache: cache,
                repo: repo,
                kind: kind,
                revision: revision,
                commitHash: commitHash,
                repoPath: repoPath,
                etag: normalizedEtag
            )
        }
    }

    /// Creates cache entries (snapshot symlink and ref) for a downloaded blob.
    ///
    /// Returns the snapshot symlink path, which resolves through to the blob.
    private func createCacheEntries(
        cache: HubCache,
        repo: Repo.ID,
        kind: Repo.Kind,
        revision: String,
        commitHash: String,
        repoPath: String,
        etag: String
    ) throws -> URL {
        // Create symlink in snapshots
        try cache.createSnapshotSymlink(
            repo: repo,
            kind: kind,
            revision: commitHash,
            filename: repoPath,
            etag: etag
        )

        // Update ref if needed
        if revision != commitHash {
            try? cache.updateRef(repo: repo, kind: kind, ref: revision, commit: commitHash)
        }

        return cache.snapshotsDirectory(repo: repo, kind: kind)
            .appendingPathComponent(commitHash)
            .appendingPathComponent(repoPath)
    }


    /// Download a file to the cache using a tree entry (uses file size for transport selection).
    ///
    /// Returns the snapshot symlink path, which resolves through to the blob.
    func downloadFile(
        _ entry: Git.TreeEntry,
        from repo: Repo.ID,
        kind: Repo.Kind = .model,
        revision: String = "main",
        endpoint: FileDownloadEndpoint = .resolve,
        cachePolicy: URLRequest.CachePolicy = .useProtocolCachePolicy,
        progress: Progress? = nil,
        transport: FileDownloadTransport = .automatic
    ) async throws -> URL {
        if transport == .automatic,
            let fileSizeBytes = entry.size,
            fileSizeBytes < xetMinimumFileSizeBytes
        {
            return try await downloadFile(
                at: entry.path,
                from: repo,
                kind: kind,
                revision: revision,
                endpoint: endpoint,
                cachePolicy: cachePolicy,
                progress: progress,
                transport: .lfs
            )
        }

        return try await downloadFile(
            at: entry.path,
            from: repo,
            kind: kind,
            revision: revision,
            endpoint: endpoint,
            cachePolicy: cachePolicy,
            progress: progress,
            transport: transport
        )
    }

    #if canImport(FoundationNetworking)
        /// Linux: Downloads file to cache blob path.
        private func downloadToCacheLinux(
            request: URLRequest,
            blobPath: URL,
            progress: Progress?
        ) async throws {
            let (tempURL, response) = try await session.asyncDownload(for: request, progress: progress)
            _ = try httpClient.validateResponse(response, data: nil)

            // Move temp file to blob path
            try? FileManager.default.removeItem(at: blobPath)
            try FileManager.default.moveItem(at: tempURL, to: blobPath)
        }
    #else
        /// Apple: Downloads file to cache with resume support.
        ///
        /// Uses `URLSession.download(for:delegate:)` for efficient OS-level streaming to disk.
        /// For resume, downloads the remainder to a temp file and appends it to the
        /// `.incomplete` file, then moves to the final blob path.
        private func downloadToCacheApple(
            request: URLRequest,
            blobPath: URL,
            etag: String,
            blobsDir: URL,
            progress: Progress?
        ) async throws {
            let fileManager = FileManager.default
            let incompletePath = blobsDir.appendingPathComponent("\(etag).incomplete")

            // Track whether we should ignore any existing incomplete file and start fresh.
            // This is set to true after receiving a 416 response, which indicates the
            // Range header we sent was invalid (e.g., incomplete file is larger than
            // the actual file, or the file changed on the server).
            var shouldStartFresh = false

            while true {
                // Check for incomplete file to resume (skip if we're retrying after 416)
                var resumeSize: Int64 = 0
                if !shouldStartFresh,
                    fileManager.fileExists(atPath: incompletePath.path),
                    let attrs = try? fileManager.attributesOfItem(atPath: incompletePath.path),
                    let size = attrs[.size] as? Int64
                {
                    resumeSize = size
                } else {
                    try? fileManager.removeItem(at: incompletePath)
                }

                // Add Range header if resuming
                var resumeRequest = request
                if resumeSize > 0 {
                    resumeRequest.setValue("bytes=\(resumeSize)-", forHTTPHeaderField: "Range")
                }

                let delegate = progress.map {
                    DownloadProgressDelegate(progress: $0, resumeOffset: resumeSize)
                }
                let (tempURL, response) = try await session.download(
                    for: resumeRequest,
                    delegate: delegate
                )

                guard let httpResponse = response as? HTTPURLResponse else {
                    throw HTTPClientError.unexpectedError("Invalid HTTP response")
                }

                let statusCode = httpResponse.statusCode

                // Handle 416 Range Not Satisfiable: the Range header we sent was invalid.
                // This typically happens when:
                // 1. The incomplete file is larger than the actual file on the server
                // 2. The file changed on the server since we started downloading
                // 3. The incomplete file contains corrupted/invalid data
                // Solution: delete the incomplete file and retry once without a Range header.
                if statusCode == 416 {
                    guard !shouldStartFresh else {
                        throw HTTPClientError.responseError(
                            response: httpResponse,
                            detail: "Download failed: server returned 416 after fresh retry"
                        )
                    }
                    shouldStartFresh = true
                    continue
                }

                guard (200 ..< 300).contains(statusCode) else {
                    throw HTTPClientError.responseError(
                        response: httpResponse,
                        detail: "Download failed with status \(statusCode)"
                    )
                }

                if statusCode == 206, resumeSize > 0 {
                    // Partial content: append the downloaded remainder to the incomplete file
                    try appendFile(from: tempURL, to: incompletePath)
                    try? fileManager.removeItem(at: blobPath)
                    try fileManager.moveItem(at: incompletePath, to: blobPath)
                } else {
                    // Full download (200): server ignored Range or fresh download.
                    try? fileManager.removeItem(at: incompletePath)
                    try? fileManager.removeItem(at: blobPath)
                    try fileManager.moveItem(at: tempURL, to: blobPath)
                }

                return
            }
        }

        /// Appends the contents of one file to another using chunked reads.
        private func appendFile(from source: URL, to destination: URL) throws {
            let sourceHandle = try FileHandle(forReadingFrom: source)
            defer { try? sourceHandle.close() }
            let destHandle = try FileHandle(forWritingTo: destination)
            defer { try? destHandle.close() }

            try destHandle.seekToEnd()

            let chunkSize = 1_048_576 // 1 MB
            while let chunk = try sourceHandle.read(upToCount: chunkSize), !chunk.isEmpty {
                try destHandle.write(contentsOf: chunk)
            }
        }
    #endif

}

// MARK: - Progress Delegate

#if !canImport(FoundationNetworking)
    /// Delegate for tracking download progress with throughput calculation.
    ///
    /// When resuming a partial download, `resumeOffset` accounts for bytes already on disk
    /// so that progress reports the true total (offset + newly downloaded bytes). Without this,
    /// Foundation's Progress parent-child tree would scale the partial download to the full
    /// file weight, causing the progress bar to advance at inflated speed.
    ///
    /// Safe to mark @unchecked Sendable: mutable state is only accessed from URLSession's
    /// delegate queue, which serializes all callbacks.
    private final class DownloadProgressDelegate: NSObject, URLSessionDownloadDelegate, @unchecked Sendable {
        private let progress: Progress
        private let resumeOffset: Int64
        private let startTime: CFAbsoluteTime

        init(progress: Progress, resumeOffset: Int64 = 0) {
            self.progress = progress
            self.resumeOffset = resumeOffset
            self.startTime = CFAbsoluteTimeGetCurrent()
        }

        func urlSession(
            _: URLSession,
            downloadTask _: URLSessionDownloadTask,
            didWriteData _: Int64,
            totalBytesWritten: Int64,
            totalBytesExpectedToWrite: Int64
        ) {
            progress.totalUnitCount = resumeOffset + totalBytesExpectedToWrite
            progress.completedUnitCount = resumeOffset + totalBytesWritten

            let elapsed = CFAbsoluteTimeGetCurrent() - startTime
            if elapsed > 0 {
                let bytesPerSecond = Double(totalBytesWritten) / elapsed
                progress.setUserInfoObject(bytesPerSecond, forKey: .throughputKey)
            }
        }

        func urlSession(
            _: URLSession,
            downloadTask _: URLSessionDownloadTask,
            didFinishDownloadingTo _: URL
        ) {
            // File handling is done in the async/await layer
        }
    }
#endif

// MARK: - Delete Operations

public extension HubClient {
    /// Delete a file from a repository
    /// - Parameters:
    ///   - repoPath: Path to file to delete
    ///   - repo: Repository identifier
    ///   - kind: Kind of repository
    ///   - branch: Target branch
    ///   - message: Commit message
    func deleteFile(
        at repoPath: String,
        from repo: Repo.ID,
        kind: Repo.Kind = .model,
        branch: String = "main",
        message: String
    ) async throws {
        try await deleteFiles(at: [repoPath], from: repo, kind: kind, branch: branch, message: message)
    }

    /// Delete multiple files from a repository
    /// - Parameters:
    ///   - paths: Paths to files to delete
    ///   - repo: Repository identifier
    ///   - kind: Kind of repository
    ///   - branch: Target branch
    ///   - message: Commit message
    func deleteFiles(
        at repoPaths: [String],
        from repo: Repo.ID,
        kind: Repo.Kind = .model,
        branch: String = "main",
        message: String
    ) async throws {
        let url = httpClient.host
            .appending(path: "api")
            .appending(path: kind.pluralized)
            .appending(path: repo.namespace)
            .appending(path: repo.name)
            .appending(path: "commit")
            .appending(component: branch)
        let operations = repoPaths.map { path in
            Value.object(["op": .string("delete"), "path": .string(path)])
        }
        let params: [String: Value] = [
            "title": .string(message),
            "operations": .array(operations),
        ]

        let _: Bool = try await httpClient.fetch(.post, url: url, params: params)
    }
}

// MARK: - Query Operations

public extension HubClient {
    /// Check if a file exists in a repository
    /// - Parameters:
    ///   - repoPath: Path to file
    ///   - repo: Repository identifier
    ///   - kind: Kind of repository
    ///   - revision: Git revision
    /// - Returns: True if file exists
    func fileExists(
        at repoPath: String,
        in repo: Repo.ID,
        kind: Repo.Kind = .model,
        revision: String = "main"
    ) async -> Bool {
        do {
            let info = try await getFile(at: repoPath, in: repo, kind: kind, revision: revision)
            return info.exists
        } catch {
            return false
        }
    }

    /// List files in a repository
    /// - Parameters:
    ///   - repo: Repository identifier
    ///   - kind: Kind of repository
    ///   - revision: Git revision
    ///   - recursive: List files recursively
    /// - Returns: Array of tree entries
    func listFiles(
        in repo: Repo.ID,
        kind: Repo.Kind = .model,
        revision: String = "main",
        recursive: Bool = true
    ) async throws -> [Git.TreeEntry] {
        let url = httpClient.host
            .appending(path: "api")
            .appending(path: kind.pluralized)
            .appending(path: repo.namespace)
            .appending(path: repo.name)
            .appending(path: "tree")
            .appending(component: revision)
        let params: [String: Value]? = recursive ? ["recursive": .bool(true)] : nil

        return try await httpClient.fetch(.get, url: url, params: params)
    }

    /// Get file information
    /// - Parameters:
    ///   - repoPath: Path to file
    ///   - repo: Repository identifier
    ///   - kind: Kind of repository
    ///   - revision: Git revision
    /// - Returns: File information
    func getFile(
        at repoPath: String,
        in repo: Repo.ID,
        kind _: Repo.Kind = .model,
        revision: String = "main"
    ) async throws -> File {
        let url = httpClient.host
            .appending(path: repo.namespace)
            .appending(path: repo.name)
            .appending(path: "resolve")
            .appending(component: revision)
            .appending(path: repoPath)
        var request = try await httpClient.createRequest(.head, url: url)
        request.setValue("bytes=0-0", forHTTPHeaderField: "Range")

        do {
            let (_, response) = try await session.data(for: request)
            guard let httpResponse = response as? HTTPURLResponse else {
                return File(exists: false)
            }

            let exists = httpResponse.statusCode == 200 || httpResponse.statusCode == 206
            let size = httpResponse.value(forHTTPHeaderField: "Content-Length")
                .flatMap { Int64($0) }
            let etag = httpResponse.value(forHTTPHeaderField: "ETag")
            let revision = httpResponse.value(forHTTPHeaderField: "X-Repo-Commit")
            let isLFS =
                httpResponse.value(forHTTPHeaderField: "X-Linked-Size") != nil
                || httpResponse.value(forHTTPHeaderField: "Link")?.contains("lfs") == true

            return File(
                exists: exists,
                size: size,
                etag: etag,
                revision: revision,
                isLFS: isLFS
            )
        } catch {
            return File(exists: false)
        }
    }
}

// MARK: - Snapshot Cache Lookup

public extension HubClient {
    /// Returns the cached snapshot path if all files matching the given globs are present.
    ///
    /// This method resolves the revision (commit hash or branch name) and checks
    /// cached repo info to verify that all matching files have been downloaded.
    /// Returns `nil` if the snapshot is missing, incomplete, or has no cached repo info.
    ///
    /// - Parameters:
    ///   - repo: Repository identifier
    ///   - kind: Kind of repository
    ///   - revision: Git revision (branch, tag, or commit hash)
    ///   - globs: Glob patterns to filter files (empty array checks all files)
    /// - Returns: Path to the verified snapshot directory, or `nil`.
    func cachedSnapshotPath(
        repo: Repo.ID,
        kind: Repo.Kind = .model,
        revision: String,
        matching globs: [String] = []
    ) -> URL? {
        let commit: String
        if isCommitHash(revision) {
            commit = revision
        } else if let resolved = cache.resolveRevision(repo: repo, kind: kind, ref: revision) {
            commit = resolved
        } else {
            return nil
        }

        return verifiedSnapshotPath(
            cache: cache, repo: repo, kind: kind, commit: commit, matching: globs
        )
    }
}

// MARK: - Snapshot Download

public extension HubClient {
    /// Download a repository snapshot to a local directory.
    ///
    /// This method downloads all files from a repository to the specified destination.
    /// Files are automatically cached in the Python-compatible cache directory,
    /// allowing cache reuse between Swift and Python Hugging Face clients.
    ///
    /// Files are downloaded in parallel (up to `maxConcurrent` simultaneous downloads)
    /// and progress is weighted by file size for accurate reporting.
    ///
    /// In offline mode (explicit or auto-detected), this method returns cached files
    /// without making network requests. An error is thrown if required files are not cached.
    ///
    /// Downloads can be cancelled by cancelling the enclosing task:
    /// ```swift
    /// let task = Task {
    ///     try await client.downloadSnapshot(of: "repo/id")
    /// }
    /// // Later:
    /// task.cancel()
    /// ```
    ///
    /// - Parameters:
    ///   - repo: Repository identifier
    ///   - kind: Kind of repository
    ///   - destination: Local destination directory
    ///   - revision: Git revision (branch, tag, or commit)
    ///   - matching: Glob patterns to filter files (empty array downloads all files)
    ///   - maxConcurrent: Maximum number of concurrent downloads (default: 8)
    ///   - progressHandler: Optional closure called with progress updates
    /// - Returns: URL to the local snapshot directory
    func downloadSnapshot(
        of repo: Repo.ID,
        kind: Repo.Kind = .model,
        revision: String = "main",
        matching globs: [String] = [],
        to destination: URL? = nil,
        localFilesOnly: Bool = false,
        maxConcurrent: Int = 8,
        progressHandler: (@Sendable (Progress) -> Void)? = nil
    ) async throws -> URL {
        // When localFilesOnly is set or the device is offline, return cached
        // files without making any network requests.
        if localFilesOnly {
            let snapshotPath = try downloadSnapshotOffline(
                cache: cache, repo: repo, kind: kind, revision: revision
            )
            return try copySnapshotToLocalDirectoryIfNeeded(
                from: snapshotPath, localDirectory: destination
            )
        }
        if await shouldUseOfflineMode() {
            let snapshotPath = try downloadSnapshotOffline(
                cache: cache, repo: repo, kind: kind, revision: revision
            )
            return try copySnapshotToLocalDirectoryIfNeeded(
                from: snapshotPath, localDirectory: destination
            )
        }

        // Fast path: when revision is already a commit hash (immutable), use cached
        // repo info to verify all matching files are present before returning.
        //
        // Python's snapshot_download acknowledges this limitation in its offline path:
        //   "we can't check if all the files are actually there"
        //   (huggingface_hub/_snapshot_download.py:293)
        // We improve on this by caching the API response after the first download and
        // verifying each file's presence in the snapshot on subsequent calls.
        if isCommitHash(revision),
            let snapshotPath = verifiedSnapshotPath(
                cache: cache, repo: repo, kind: kind, commit: revision, matching: globs)
        {
            let totalProgress = Progress(totalUnitCount: 1)
            totalProgress.completedUnitCount = 1
            progressHandler?(totalProgress)
            return try copySnapshotToLocalDirectoryIfNeeded(
                from: snapshotPath, localDirectory: destination
            )
        }

        // Fetch repo info from the server. If the network call fails, fall back to the
        // local cache (matching huggingface_hub's try/except → local_files_only pattern).
        let repoInfo: RepoInfoForDownload
        do {
            repoInfo = try await getRepoInfo(for: repo, kind: kind, revision: revision)
        } catch {
            if let cached = try? downloadSnapshotOffline(
                cache: cache, repo: repo, kind: kind, revision: revision
            ) {
                return try copySnapshotToLocalDirectoryIfNeeded(
                    from: cached, localDirectory: destination
                )
            }
            throw error
        }
        let commitHash = repoInfo.commitHash

        guard let siblings = repoInfo.siblings else {
            throw HubCacheError.unexpectedAPIResponse("Could not get file list for repository '\(repo)'")
        }

        let entries = siblings.filter { entry in
            guard !globs.isEmpty else { return true }
            return globs.contains { glob in
                fnmatch(glob, entry.path, 0) == 0
            }
        }

        let snapshotPath = cache.snapshotsDirectory(repo: repo, kind: kind)
            .appendingPathComponent(commitHash)

        // Cache repo info for future fast path lookups
        saveCachedRepoInfo(
            repoInfo, cache: cache, repo: repo, kind: kind, commit: commitHash
        )

        // Size-weighted progress: total is sum of file sizes (bytes)
        let totalBytes = entries.reduce(Int64(0)) { $0 + Int64($1.size ?? 1) }
        let totalProgress = Progress(totalUnitCount: totalBytes)
        totalProgress.kind = .file
        totalProgress.fileOperationKind = .downloading
        let startTime = CFAbsoluteTimeGetCurrent()
        progressHandler?(totalProgress)

        // Track speed updates - updated periodically, handler called after file completions
        let speedUpdateTask = Task {
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: DownloadConstants.speedUpdateIntervalNanoseconds)
                let elapsed = CFAbsoluteTimeGetCurrent() - startTime
                let bytesCompleted = totalProgress.completedUnitCount
                if elapsed > 0 && bytesCompleted > 0 {
                    let speed = Double(bytesCompleted) / elapsed
                    totalProgress.setUserInfoObject(speed, forKey: .throughputKey)
                }
            }
        }
        defer { speedUpdateTask.cancel() }

        // Parallel downloads with concurrency limiting
        try await withThrowingTaskGroup(of: Void.self) { group in
            var activeCount = 0

            for entry in entries {
                while activeCount >= maxConcurrent {
                    try await group.next()
                    activeCount -= 1
                    progressHandler?(totalProgress)
                }

                if Task.isCancelled {
                    break
                }

                let fileSize = Int64(entry.size ?? 1)
                let fileProgress = Progress(totalUnitCount: fileSize, parent: totalProgress, pendingUnitCount: fileSize)

                group.addTask {
                    _ = try await self.downloadFile(
                        entry,
                        from: repo,
                        kind: kind,
                        revision: commitHash,
                        progress: fileProgress
                    )
                    fileProgress.completedUnitCount = fileProgress.totalUnitCount
                }
                activeCount += 1
            }

            for try await _ in group {
                progressHandler?(totalProgress)
            }
        }

        // Update ref mapping if we resolved a branch/tag to commit hash
        if revision != commitHash {
            try? cache.updateRef(repo: repo, kind: kind, ref: revision, commit: commitHash)
        }

        // Compute final speed before last handler call
        let elapsed = CFAbsoluteTimeGetCurrent() - startTime
        if elapsed > 0 && totalProgress.completedUnitCount > 0 {
            let finalSpeed = Double(totalProgress.completedUnitCount) / elapsed
            totalProgress.setUserInfoObject(finalSpeed, forKey: .throughputKey)
        }

        progressHandler?(totalProgress)
        return try copySnapshotToLocalDirectoryIfNeeded(
            from: snapshotPath, localDirectory: destination
        )
    }

    /// Download a repository snapshot with aggregate speed reporting.
    @discardableResult
    func downloadSnapshot(
        of repo: Repo.ID,
        kind: Repo.Kind = .model,
        revision: String = "main",
        matching globs: [String] = [],
        to destination: URL? = nil,
        localFilesOnly: Bool = false,
        maxConcurrent: Int = 8,
        progressHandler: @Sendable @escaping (Progress, Double?) -> Void
    ) async throws -> URL {
        try await downloadSnapshot(
            of: repo,
            kind: kind,
            revision: revision,
            matching: globs,
            to: destination,
            localFilesOnly: localFilesOnly,
            maxConcurrent: maxConcurrent
        ) { progress in
            let speed = progress.userInfo[.throughputKey] as? Double
            progressHandler(progress, speed)
        }
    }

    /// Returns the snapshot cache path in offline mode.
    ///
    /// Matches huggingface_hub behavior: returns cached snapshot path.
    /// As huggingface_hub notes: "we can't check if all the files are actually there" —
    /// this is a best-effort approach.
    private func downloadSnapshotOffline(
        cache: HubCache,
        repo: Repo.ID,
        kind: Repo.Kind,
        revision: String
    ) throws -> URL {
        guard let snapshotPath = cache.snapshotPath(repo: repo, kind: kind, revision: revision) else {
            throw HubCacheError.offlineModeError("Repository '\(repo)' not available in cache")
        }

        guard FileManager.default.fileExists(atPath: snapshotPath.path) else {
            throw HubCacheError.offlineModeError("Repository '\(repo)' not available in cache")
        }

        return snapshotPath
    }

    /// If `localDirectory` is set, copies a single cached file there and returns the
    /// local path. Otherwise returns the cached path unchanged.
    private func copyToLocalDirectoryIfNeeded(
        _ cachedPath: URL, repoPath: String, localDirectory: URL?
    ) throws -> URL {
        guard let localDirectory else { return cachedPath }
        let localPath = localDirectory.appendingPathComponent(repoPath)
        let fileManager = FileManager.default
        try fileManager.createDirectory(
            at: localPath.deletingLastPathComponent(), withIntermediateDirectories: true
        )
        try? fileManager.removeItem(at: localPath)
        try fileManager.copyItem(at: cachedPath, to: localPath)
        return localPath
    }

    /// If `localDirectory` is set, copies all files from a snapshot directory there
    /// and returns the local directory. Otherwise returns the snapshot path unchanged.
    private func copySnapshotToLocalDirectoryIfNeeded(
        from snapshotPath: URL, localDirectory: URL?
    ) throws -> URL {
        guard let localDirectory else { return snapshotPath }
        let fileManager = FileManager.default
        try fileManager.createDirectory(at: localDirectory, withIntermediateDirectories: true)

        guard let enumerator = fileManager.enumerator(
            at: snapshotPath,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles]
        ) else {
            return localDirectory
        }

        for case let fileURL as URL in enumerator {
            let snapshotComponents = snapshotPath.standardized.pathComponents
            let fileComponents = fileURL.standardized.pathComponents
            let relativeComponents = fileComponents.dropFirst(snapshotComponents.count)
            let destURL = relativeComponents.reduce(localDirectory) { $0.appendingPathComponent($1) }
            try fileManager.createDirectory(
                at: destURL.deletingLastPathComponent(), withIntermediateDirectories: true
            )
            try? fileManager.removeItem(at: destURL)
            try fileManager.copyItem(at: fileURL, to: destURL)
        }

        return localDirectory
    }
}

// MARK: - Xet Operations

/// Metadata returned from a Xet HEAD request, combining the Xet file ID
/// with standard cache metadata (etag, commit hash) from the same response.
private struct XetFileInfo {
    let fileID: String
    let etag: String?
    let commitHash: String?
}

private extension HubClient {
    /// Downloads file data using Xet's content-addressable storage system.
    func downloadDataWithXet(
        repoPath: String,
        repo: Repo.ID,
        kind: Repo.Kind,
        revision: String,
        transport: FileDownloadTransport
    ) async throws -> Data? {
        guard
            let xetInfo = try await fetchXetFileInfo(
                repoPath: repoPath,
                repo: repo,
                revision: revision,
                transport: transport
            )
        else {
            return nil
        }

        return try await Xet.withDownloader(
            refreshURL: xetRefreshURL(for: repo, kind: kind, revision: revision),
            hubToken: try? await httpClient.tokenProvider.getToken()
        ) { downloader in
            try await downloader.data(for: xetInfo.fileID)
        }
    }

    /// Downloads a file using Xet's content-addressable storage system.
    ///
    /// Downloads to the blob cache and creates a snapshot symlink.
    /// Returns the snapshot symlink path, or nil if the file doesn't support Xet.
    /// Requires a cache to be configured.
    @discardableResult
    func downloadFileWithXet(
        repoPath: String,
        repo: Repo.ID,
        kind: Repo.Kind,
        revision: String,
        progress: Progress?,
        transport: FileDownloadTransport
    ) async throws -> URL? {
        guard
            let xetInfo = try await fetchXetFileInfo(
                repoPath: repoPath,
                repo: repo,
                revision: revision,
                transport: transport
            )
        else {
            return nil
        }

        guard let normalizedEtag = xetInfo.etag else {
            throw HubCacheError.unexpectedAPIResponse("Server did not return an ETag header for '\(repoPath)'")
        }

        let fileManager = FileManager.default
        let blobsDir = cache.blobsDirectory(repo: repo, kind: kind)
        let blobPath = blobsDir.appendingPathComponent(normalizedEtag)
        let commitHash = xetInfo.commitHash ?? revision

        if !fileManager.fileExists(atPath: blobPath.path) {
            let locksDir = cache.locksDirectory(repo: repo, kind: kind)
            let lockPath = locksDir.appendingPathComponent(normalizedEtag)
            let lock = await FileLock(lockPath: lockPath.appendingPathExtension("lock"))
            try await lock.withLock {
                // Double-check after acquiring lock
                if !fileManager.fileExists(atPath: blobPath.path) {
                    try fileManager.createDirectory(at: blobsDir, withIntermediateDirectories: true)
                    _ = try await Xet.withDownloader(
                        refreshURL: xetRefreshURL(for: repo, kind: kind, revision: revision),
                        hubToken: try? await httpClient.tokenProvider.getToken()
                    ) { downloader in
                        try await downloader.download(xetInfo.fileID, to: blobPath)
                    }
                }
            }
        }

        let result = try createCacheEntries(
            cache: cache,
            repo: repo,
            kind: kind,
            revision: revision,
            commitHash: commitHash,
            repoPath: repoPath,
            etag: normalizedEtag
        )

        progress?.totalUnitCount = 100
        progress?.completedUnitCount = 100

        return result
    }

    func fetchXetFileInfo(
        repoPath: String,
        repo: Repo.ID,
        revision: String,
        transport: FileDownloadTransport
    ) async throws -> XetFileInfo? {
        let urlPath = "/\(repo)/resolve/\(revision)/\(repoPath)"
        var request = try await httpClient.createRequest(.head, urlPath)
        request.cachePolicy = .reloadIgnoringLocalCacheData

        let (_, response) = try await session.data(
            for: request,
            delegate: NoRedirectDelegate()
        )
        guard let httpResponse = response as? HTTPURLResponse else {
            return nil
        }

        let rawFileID = httpResponse.value(forHTTPHeaderField: "X-Xet-Hash")?
            .trimmingCharacters(in: .whitespacesAndNewlines)
        guard let fileID = rawFileID, !fileID.isEmpty else {
            return nil
        }

        let rawSize =
            httpResponse.value(forHTTPHeaderField: "X-Linked-Size")
            ?? ((200 ... 299).contains(httpResponse.statusCode)
                ? httpResponse.value(forHTTPHeaderField: "Content-Length")
                : nil)
        let fileSizeBytes = rawSize.flatMap(Int.init)
        if !transport.shouldUseXet(
            fileSizeBytes: fileSizeBytes,
            minimumFileSizeBytes: xetMinimumFileSizeBytes
        ) {
            return nil
        }

        guard isValidHash(fileID, pattern: sha256Pattern) else {
            return nil
        }

        // Extract cache metadata from the same HEAD response to avoid a second request.
        // Uses X-Linked-Etag (for LFS/Xet files) with ETag fallback, matching fetchFileMetadata.
        let linkedEtag = httpResponse.value(forHTTPHeaderField: "X-Linked-Etag")
        let etag = linkedEtag ?? httpResponse.value(forHTTPHeaderField: "ETag")
        let commitHash = httpResponse.value(forHTTPHeaderField: "X-Repo-Commit")

        return XetFileInfo(
            fileID: fileID,
            etag: etag.map { HubCache.normalizeEtag($0) },
            commitHash: commitHash
        )
    }

    func xetRefreshURL(for repo: Repo.ID, kind: Repo.Kind, revision: String) -> URL {
        let url = httpClient.host.appendingPathComponent(
            "api/\(kind.pluralized)/\(repo)/xet-read-token/\(revision)"
        )
        return url
    }
}

private final class NoRedirectDelegate: NSObject, URLSessionTaskDelegate {
    func urlSession(
        _: URLSession,
        task _: URLSessionTask,
        willPerformHTTPRedirection _: HTTPURLResponse,
        newRequest _: URLRequest,
        completionHandler: @escaping (URLRequest?) -> Void
    ) {
        completionHandler(nil)
    }
}

// MARK: - Metadata Helpers

extension HubClient {
    private var sha256Pattern: String { "^[0-9a-f]{64}$" }
    private var commitHashPattern: String { "^[0-9a-f]{40}$" }

    /// Read metadata about a file in the local directory.
    func readDownloadMetadata(at metadataPath: URL) -> LocalDownloadFileMetadata? {
        FileManager.default.readDownloadMetadata(at: metadataPath)
    }

    /// Write metadata about a downloaded file.
    func writeDownloadMetadata(commitHash: String, etag: String, to metadataPath: URL) throws {
        try FileManager.default.writeDownloadMetadata(
            commitHash: commitHash,
            etag: etag,
            to: metadataPath
        )
    }

    /// Check if a hash matches the expected pattern.
    func isValidHash(_ hash: String, pattern: String) -> Bool {
        guard let regex = try? NSRegularExpression(pattern: pattern) else {
            return false
        }
        let range = NSRange(location: 0, length: hash.utf16.count)
        return regex.firstMatch(in: hash, options: [], range: range) != nil
    }

    /// Compute SHA256 hash of a file.
    func computeFileHash(at url: URL) throws -> String {
        try FileManager.default.computeFileHash(at: url)
    }
}

// MARK: -

private struct UploadResponse: Codable {
    let path: String
    let commit: String?
}

// MARK: -

private extension FileManager {
    /// Read metadata about a file in the local directory.
    func readDownloadMetadata(at metadataPath: URL) -> LocalDownloadFileMetadata? {
        guard fileExists(atPath: metadataPath.path) else {
            return nil
        }

        do {
            let contents = try String(contentsOf: metadataPath, encoding: .utf8)
            let lines = contents.components(separatedBy: .newlines)

            guard lines.count >= 3 else {
                try? removeItem(at: metadataPath)
                return nil
            }

            let commitHash = lines[0].trimmingCharacters(in: .whitespacesAndNewlines)
            let etag = lines[1].trimmingCharacters(in: .whitespacesAndNewlines)

            guard let timestamp = Double(lines[2].trimmingCharacters(in: .whitespacesAndNewlines))
            else {
                try? removeItem(at: metadataPath)
                return nil
            }

            let timestampDate = Date(timeIntervalSince1970: timestamp)
            let filename = metadataPath.lastPathComponent.replacingOccurrences(
                of: ".metadata",
                with: ""
            )

            return LocalDownloadFileMetadata(
                commitHash: commitHash,
                etag: etag,
                filename: filename,
                timestamp: timestampDate
            )
        } catch {
            try? removeItem(at: metadataPath)
            return nil
        }
    }

    /// Write metadata about a downloaded file.
    func writeDownloadMetadata(commitHash: String, etag: String, to metadataPath: URL) throws {
        let metadataContent = "\(commitHash)\n\(etag)\n\(Date().timeIntervalSince1970)\n"
        try createDirectory(
            at: metadataPath.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try metadataContent.write(to: metadataPath, atomically: true, encoding: .utf8)
    }

    /// Compute SHA256 hash of a file.
    func computeFileHash(at url: URL) throws -> String {
        guard let fileHandle = try? FileHandle(forReadingFrom: url) else {
            throw HTTPClientError.unexpectedError("Unable to open file: \(url.path)")
        }

        defer {
            try? fileHandle.close()
        }

        var hasher = SHA256()
        let chunkSize = 1024 * 1024

        #if canImport(Darwin)
            while autoreleasepool(invoking: {
                guard let nextChunk = try? fileHandle.read(upToCount: chunkSize),
                    !nextChunk.isEmpty
                else {
                    return false
                }

                hasher.update(data: nextChunk)
                return true
            }) {}
        #else
            while true {
                guard let nextChunk = try? fileHandle.read(upToCount: chunkSize),
                    !nextChunk.isEmpty
                else {
                    break
                }

                hasher.update(data: nextChunk)
            }
        #endif

        let digest = hasher.finalize()
        return digest.map { String(format: "%02x", $0) }.joined()
    }
}

// MARK: -

private extension URL {
    var mimeType: String? {
        #if canImport(UniformTypeIdentifiers)
            guard let uti = UTType(filenameExtension: pathExtension) else {
                return nil
            }
            return uti.preferredMIMEType
        #else
            // Fallback MIME type lookup for Linux
            let ext = pathExtension.lowercased()
            switch ext {
            // MARK: - JSON
            case "json":
                return "application/json"
            // MARK: - Text
            case "txt":
                return "text/plain"
            case "md":
                return "text/markdown"
            case "csv":
                return "text/csv"
            case "tsv":
                return "text/tab-separated-values"
            // MARK: - HTML and Markup
            case "html", "htm":
                return "text/html"
            case "xml":
                return "application/xml"
            case "svg":
                return "image/svg+xml"
            case "yaml", "yml":
                return "application/x-yaml"
            case "toml":
                return "application/toml"
            // MARK: - Code
            case "js":
                return "application/javascript"
            case "py":
                return "text/x-python"
            case "swift":
                return "text/x-swift"
            case "css":
                return "text/css"
            case "ipynb":
                return "application/x-ipynb+json"
            // MARK: - Archives and Compressed
            case "zip":
                return "application/zip"
            case "gz", "gzip":
                return "application/gzip"
            case "tar":
                return "application/x-tar"
            case "bz2":
                return "application/x-bzip2"
            case "7z":
                return "application/x-7z-compressed"
            // MARK: - PDF and Documents
            case "pdf":
                return "application/pdf"
            // MARK: - Images
            case "png":
                return "image/png"
            case "jpg", "jpeg":
                return "image/jpeg"
            case "gif":
                return "image/gif"
            case "webp":
                return "image/webp"
            case "bmp":
                return "image/bmp"
            case "tiff", "tif":
                return "image/tiff"
            // MARK: - Audio
            case "m4a":
                return "audio/mp4"
            case "mp3":
                return "audio/mpeg"
            case "wav":
                return "audio/wav"
            case "flac":
                return "audio/flac"
            case "ogg":
                return "audio/ogg"
            // MARK: - Video
            case "mp4":
                return "video/mp4"
            case "webm":
                return "video/webm"
            // MARK: - ML/Model/Raw Data
            case "bin", "safetensors", "gguf", "ggml":
                return "application/octet-stream"
            case "pt", "pth":
                return "application/octet-stream"
            case "onnx":
                return "application/octet-stream"
            case "ckpt":
                return "application/octet-stream"
            case "npz":
                return "application/octet-stream"
            // MARK: - Default
            default:
                return "application/octet-stream"
            }
        #endif
    }
}

/// Checks if a string is a valid Git commit hash (40 hex characters).
private func isCommitHash(_ string: String) -> Bool {
    guard string.count == 40 else { return false }
    return string.allSatisfy { $0.isHexDigit }
}

// MARK: - Cached Repo Info

/// Saves the API response for a commit to the metadata directory for future fast path lookups.
private func saveCachedRepoInfo(
    _ info: RepoInfoForDownload,
    cache: HubCache,
    repo: Repo.ID,
    kind: Repo.Kind,
    commit: String
) {
    let metadataDir = cache.metadataDirectory(repo: repo, kind: kind)
    try? FileManager.default.createDirectory(at: metadataDir, withIntermediateDirectories: true)
    let path = metadataDir.appendingPathComponent("\(commit).json")
    try? JSONEncoder().encode(info).write(to: path)
}

/// Loads cached repo info from the metadata directory, if available.
private func loadCachedRepoInfo(
    cache: HubCache,
    repo: Repo.ID,
    kind: Repo.Kind,
    commit: String
) -> RepoInfoForDownload? {
    let path = cache.metadataDirectory(repo: repo, kind: kind)
        .appendingPathComponent("\(commit).json")
    guard let data = try? Data(contentsOf: path) else { return nil }
    return try? JSONDecoder().decode(RepoInfoForDownload.self, from: data)
}

/// Checks whether all files matching the given globs are present in the snapshot directory,
/// using cached repo info to know the complete file list for the commit.
///
/// Returns the snapshot path if all matching files are present, `nil` otherwise.
private func verifiedSnapshotPath(
    cache: HubCache,
    repo: Repo.ID,
    kind: Repo.Kind,
    commit: String,
    matching globs: [String]
) -> URL? {
    let snapshotDir = cache.snapshotsDirectory(repo: repo, kind: kind)
        .appendingPathComponent(commit)

    guard let cachedInfo = loadCachedRepoInfo(
        cache: cache, repo: repo, kind: kind, commit: commit
    ), let siblings = cachedInfo.siblings else { return nil }

    let entries = siblings.filter { entry in
        guard !globs.isEmpty else { return true }
        return globs.contains { glob in
            fnmatch(glob, entry.path, 0) == 0
        }
    }

    let allPresent = entries.allSatisfy { entry in
        FileManager.default.fileExists(
            atPath: snapshotDir.appendingPathComponent(entry.path).path
        )
    }

    return allPresent ? snapshotDir : nil
}

/// Repository info with commit hash and file list.
struct RepoInfoForDownload: Codable {
    let commitHash: String
    let siblings: [Git.TreeEntry]?
}

extension HubClient {
    /// Gets repository info including commit hash and file list with sizes in a single API call.
    /// Uses `filesMetadata=true` to include file sizes for size-weighted progress reporting.
    func getRepoInfo(for repo: Repo.ID, kind: Repo.Kind, revision: String) async throws -> RepoInfoForDownload {
        switch kind {
        case .model:
            let model = try await getModel(repo, revision: revision, filesMetadata: true)
            guard let sha = model.sha else {
                throw HubCacheError.unexpectedAPIResponse("Could not resolve revision '\(revision)' to commit hash")
            }
            let siblings = model.siblings?.map {
                Git.TreeEntry(path: $0.relativeFilename, type: .file, oid: nil, size: $0.size, lastCommit: nil)
            }
            return RepoInfoForDownload(commitHash: sha, siblings: siblings)
        case .dataset:
            let dataset = try await getDataset(repo, revision: revision, filesMetadata: true)
            guard let sha = dataset.sha else {
                throw HubCacheError.unexpectedAPIResponse("Could not resolve revision '\(revision)' to commit hash")
            }
            let siblings = dataset.siblings?.map {
                Git.TreeEntry(path: $0.relativeFilename, type: .file, oid: nil, size: $0.size, lastCommit: nil)
            }
            return RepoInfoForDownload(commitHash: sha, siblings: siblings)
        case .space:
            let space = try await getSpace(repo, revision: revision, filesMetadata: true)
            guard let sha = space.sha else {
                throw HubCacheError.unexpectedAPIResponse("Could not resolve revision '\(revision)' to commit hash")
            }
            let siblings = space.siblings?.map {
                Git.TreeEntry(path: $0.relativeFilename, type: .file, oid: nil, size: $0.size, lastCommit: nil)
            }
            return RepoInfoForDownload(commitHash: sha, siblings: siblings)
        }
    }
}

// MARK: - File Metadata

/// Metadata for a file fetched without following redirects.
/// Used to get headers from the HuggingFace response before redirect to CDN.
struct FileMetadata {
    /// The commit hash from the X-Repo-Commit header.
    let commitHash: String?
    /// The ETag for cache storage (X-Linked-Etag for xet files, ETag otherwise).
    /// This matches huggingface_hub's behavior for Python cache compatibility.
    let etag: String?
}

extension HubClient {
    /// Fetches file metadata without following redirects to CDN.
    /// This allows us to get X-Repo-Commit and X-Linked-Etag from the HuggingFace response
    /// before the redirect to CDN (which doesn't have these headers).
    /// Follows same-host redirects (e.g., renamed repos) but blocks cross-host redirects (e.g., CDN).
    func fetchFileMetadata(url: URL) async throws -> FileMetadata {
        let request = try await httpClient.createRequest(.head, url: url)

        let (_, response) = try await metadataSession.data(for: request)

        guard let httpResponse = response as? HTTPURLResponse else {
            return FileMetadata(commitHash: nil, etag: nil)
        }

        // Use X-Linked-Etag if available (xet storage), otherwise fall back to ETag.
        // This matches huggingface_hub's behavior for Python cache compatibility.
        let linkedEtag = httpResponse.value(forHTTPHeaderField: "X-Linked-Etag")
        let etag = linkedEtag ?? httpResponse.value(forHTTPHeaderField: "ETag")
        let commitHash = httpResponse.value(forHTTPHeaderField: "X-Repo-Commit")

        return FileMetadata(
            commitHash: commitHash,
            etag: etag.map { HubCache.normalizeEtag($0) }
        )
    }
}

/// URLSession delegate that follows relative redirects (same host) but blocks absolute redirects (different host).
/// This matches huggingface_hub's `_httpx_follow_relative_redirects` behavior.
/// - Relative redirects (e.g., renamed repos on same host) are followed automatically.
/// - Absolute redirects (e.g., to CDN) are blocked so we can capture X-Repo-Commit and X-Linked-Etag headers.
/// Safe to mark @unchecked Sendable: stateless singleton with no mutable state.
final class SameHostRedirectDelegate: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
    static let shared = SameHostRedirectDelegate()
    private override init() { super.init() }

    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        willPerformHTTPRedirection response: HTTPURLResponse,
        newRequest request: URLRequest,
        completionHandler: @escaping (URLRequest?) -> Void
    ) {
        // Follow relative redirects (same host) but block absolute redirects (different host)
        guard let originalHost = task.originalRequest?.url?.host,
            let newHost = request.url?.host
        else {
            completionHandler(nil)
            return
        }

        if originalHost == newHost {
            // Same host redirect (e.g., renamed repo) - follow it
            completionHandler(request)
        } else {
            // Different host redirect (e.g., CDN) - block to capture headers
            completionHandler(nil)
        }
    }
}
