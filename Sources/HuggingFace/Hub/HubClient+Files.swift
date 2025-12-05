import Foundation
import UniformTypeIdentifiers

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

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
        let urlPath = "/api/\(kind.pluralized)/\(repo)/upload/\(branch)"
        var request = try await httpClient.createRequest(.post, urlPath)

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

public extension HubClient {
    /// Download file data using URLSession.dataTask
    /// - Parameters:
    ///   - repoPath: Path to file in repository
    ///   - repo: Repository identifier
    ///   - kind: Kind of repository
    ///   - revision: Git revision (branch, tag, or commit)
    ///   - useRaw: Use raw endpoint instead of resolve
    ///   - cachePolicy: Cache policy for the request
    /// - Returns: File data
    func downloadContentsOfFile(
        at repoPath: String,
        from repo: Repo.ID,
        kind: Repo.Kind = .model,
        revision: String = "main",
        useRaw: Bool = false,
        cachePolicy: URLRequest.CachePolicy = .useProtocolCachePolicy
    ) async throws -> Data {
        // Check cache first
        if let cache = cache,
            let cachedPath = cache.cachedFilePath(
                repo: repo,
                kind: kind,
                revision: revision,
                filename: repoPath
            )
        {
            return try Data(contentsOf: cachedPath)
        }

        let endpoint = useRaw ? "raw" : "resolve"
        let urlPath = "/\(repo)/\(endpoint)/\(revision)/\(repoPath)"
        var request = try await httpClient.createRequest(.get, urlPath)
        request.cachePolicy = cachePolicy

        let (data, response) = try await session.data(for: request)
        _ = try httpClient.validateResponse(response, data: data)

        // Store in cache if we have etag and commit info
        if let cache = cache,
            let httpResponse = response as? HTTPURLResponse,
            let etag = httpResponse.value(forHTTPHeaderField: "ETag"),
            let commitHash = httpResponse.value(forHTTPHeaderField: "X-Repo-Commit")
        {
            try? cache.storeData(
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

    /// Download file to a destination URL with automatic resume support.
    ///
    /// This method supports resumable downloads. If a download is interrupted,
    /// subsequent calls will automatically resume from where they left off.
    /// Incomplete downloads are stored in the cache directory and matched by etag.
    ///
    /// When `inBackground` is true, the download uses a background URL session that
    /// can continue even when the app is suspended. This is useful for large files
    /// on iOS. Note that background downloads have less fine-grained resume control.
    ///
    /// - Parameters:
    ///   - repoPath: Path to file in repository
    ///   - repo: Repository identifier
    ///   - destination: Destination URL for downloaded file
    ///   - kind: Kind of repository
    ///   - revision: Git revision
    ///   - useRaw: Use raw endpoint
    ///   - inBackground: Whether to use a background URL session for the download
    ///   - forceDownload: Skip cache and always download from server
    ///   - progress: Optional Progress object to track download progress and throughput
    /// - Returns: Final destination URL
    func downloadFile(
        at repoPath: String,
        from repo: Repo.ID,
        to destination: URL,
        kind: Repo.Kind = .model,
        revision: String = "main",
        useRaw: Bool = false,
        inBackground: Bool = false,
        forceDownload: Bool = false,
        progress: Progress? = nil
    ) async throws -> URL {
        // Check cache first (unless forceDownload is true)
        if !forceDownload,
            let cache = cache,
            let cachedPath = cache.cachedFilePath(
                repo: repo,
                kind: kind,
                revision: revision,
                filename: repoPath
            )
        {
            // Create parent directory if needed
            try FileManager.default.createDirectory(
                at: destination.deletingLastPathComponent(),
                withIntermediateDirectories: true
            )
            // Copy from cache to destination (resolve symlinks first)
            let resolvedPath = cachedPath.resolvingSymlinksInPath()
            try? FileManager.default.removeItem(at: destination)
            try FileManager.default.copyItem(at: resolvedPath, to: destination)
            progress?.completedUnitCount = progress?.totalUnitCount ?? 100
            return destination
        }

        // Perform download with retry logic
        var lastError: Error?
        let maxRetries = downloadConfiguration.maxRetries
        let retryDelay = downloadConfiguration.retryDelay

        for attempt in 0 ... maxRetries {
            do {
                return try await performDownloadWithResume(
                    repoPath: repoPath,
                    repo: repo,
                    destination: destination,
                    kind: kind,
                    revision: revision,
                    useRaw: useRaw,
                    inBackground: inBackground,
                    progress: progress
                )
            } catch {
                lastError = error

                // Don't retry on cancellation or if this was the last attempt
                if error is CancellationError || attempt >= maxRetries {
                    throw error
                }

                // Wait before retrying
                try await Task.sleep(for: .seconds(retryDelay))
            }
        }

        throw lastError ?? HTTPClientError.requestError("Download failed after \(maxRetries) retries")
    }

    /// Performs the actual download with resume support (called by retry logic).
    private func performDownloadWithResume(
        repoPath: String,
        repo: Repo.ID,
        destination: URL,
        kind: Repo.Kind,
        revision: String,
        useRaw: Bool,
        inBackground: Bool,
        progress: Progress?
    ) async throws -> URL {
        // Get file metadata to determine etag and size
        let fileInfo = try await getFile(at: repoPath, in: repo, kind: kind, revision: revision)
        guard fileInfo.exists else {
            throw HTTPClientError.requestError("File not found: \(repoPath)")
        }

        let etag = fileInfo.etag ?? UUID().uuidString
        let expectedSize = fileInfo.size ?? 0

        // Check for incomplete download and get resume offset
        var resumeOffset: Int64 = 0
        var incompletePath: URL?

        if let cache = cache {
            let incompleteSize = cache.incompleteFileSize(repo: repo, kind: kind, filename: repoPath, etag: etag)
            if incompleteSize > 0 && incompleteSize < expectedSize {
                resumeOffset = Int64(incompleteSize)
                incompletePath = cache.incompleteFilePath(repo: repo, kind: kind, filename: repoPath, etag: etag)
            } else {
                // Prepare fresh incomplete file
                incompletePath = try cache.prepareIncompleteFile(repo: repo, kind: kind, filename: repoPath, etag: etag)
            }
        }

        // Build request with Range header if resuming
        let endpoint = useRaw ? "raw" : "resolve"
        let urlPath = "/\(repo)/\(endpoint)/\(revision)/\(repoPath)"
        var request = try await httpClient.createRequest(.get, urlPath)

        if resumeOffset > 0 {
            request.setValue("bytes=\(resumeOffset)-", forHTTPHeaderField: "Range")
        }

        // Set up progress tracking
        if let progress = progress {
            progress.totalUnitCount = expectedSize
            progress.completedUnitCount = resumeOffset
        }

        // Perform download (chunked for foreground, download task for background)
        let downloadedURL: URL
        let response: URLResponse

        if inBackground {
            // Background downloads use URLSession download tasks
            let backgroundSession = createBackgroundSession(identifier: "hf-download-\(UUID().uuidString)")
            (downloadedURL, response) = try await backgroundSession.download(
                for: request,
                delegate: progress.map { DownloadProgressDelegate(progress: $0) }
            )
        } else {
            // Foreground downloads use chunked streaming for better resume support
            (downloadedURL, response) = try await performChunkedDownload(
                request: request,
                incompletePath: incompletePath,
                resumeOffset: resumeOffset,
                expectedSize: expectedSize,
                progress: progress
            )
        }

        _ = try httpClient.validateResponse(response, data: nil)

        // Get commit hash from response for caching
        let httpResponse = response as? HTTPURLResponse
        let commitHash = httpResponse?.value(forHTTPHeaderField: "X-Repo-Commit")

        // Create parent directory if needed
        try FileManager.default.createDirectory(
            at: destination.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )

        // Copy to final destination first (before storing in cache removes the source)
        try? FileManager.default.removeItem(at: destination)
        try FileManager.default.copyItem(at: downloadedURL, to: destination)

        // Store in cache (uses copyItem internally, so source is preserved)
        if let cache = cache, let commitHash = commitHash {
            try? cache.storeFile(
                at: downloadedURL,
                repo: repo,
                kind: kind,
                revision: commitHash,
                filename: repoPath,
                etag: etag,
                ref: revision != commitHash ? revision : nil
            )
            // Clean up incomplete file now that it's been copied to both destination and cache
            cache.removeIncompleteFile(repo: repo, kind: kind, filename: repoPath, etag: etag)
        } else {
            // No cache, just remove the downloaded file (we already copied to destination)
            try? FileManager.default.removeItem(at: downloadedURL)
        }

        return destination
    }

    /// Performs a chunked download with resume support.
    private func performChunkedDownload(
        request: URLRequest,
        incompletePath: URL?,
        resumeOffset: Int64,
        expectedSize: Int64,
        progress: Progress?
    ) async throws -> (URL, URLResponse) {
        let (asyncBytes, response) = try await session.bytes(for: request)

        guard let httpResponse = response as? HTTPURLResponse else {
            throw HTTPClientError.requestError("Invalid response")
        }

        // Check for successful status (200 for full download, 206 for partial)
        guard httpResponse.statusCode == 200 || httpResponse.statusCode == 206 else {
            throw HTTPClientError.requestError("HTTP error: \(httpResponse.statusCode)")
        }

        // Determine output file
        let outputPath: URL
        if let incompletePath = incompletePath {
            outputPath = incompletePath
        } else {
            outputPath = FileManager.default.temporaryDirectory
                .appendingPathComponent(UUID().uuidString)
        }

        // Open file for writing (append if resuming)
        let fileHandle: FileHandle
        if resumeOffset > 0, FileManager.default.fileExists(atPath: outputPath.path) {
            fileHandle = try FileHandle(forWritingTo: outputPath)
            try fileHandle.seekToEnd()
        } else {
            FileManager.default.createFile(atPath: outputPath.path, contents: nil)
            fileHandle = try FileHandle(forWritingTo: outputPath)
        }

        defer {
            try? fileHandle.close()
        }

        // Track progress and throughput
        var totalBytesWritten = resumeOffset
        var lastProgressUpdate = Date()
        var lastBytesForSpeed = resumeOffset
        let chunkSize = 64 * 1024  // 64KB chunks

        var buffer = Data(capacity: chunkSize)

        for try await byte in asyncBytes {
            buffer.append(byte)

            if buffer.count >= chunkSize {
                try fileHandle.write(contentsOf: buffer)
                totalBytesWritten += Int64(buffer.count)
                buffer.removeAll(keepingCapacity: true)

                // Update progress
                if let progress = progress {
                    progress.completedUnitCount = totalBytesWritten

                    // Calculate throughput
                    let now = Date()
                    let elapsed = now.timeIntervalSince(lastProgressUpdate)
                    if elapsed >= 0.1 {
                        let deltaBytes = totalBytesWritten - lastBytesForSpeed
                        let speed = Double(deltaBytes) / elapsed
                        progress.setUserInfoObject(speed, forKey: .throughputKey)
                        lastProgressUpdate = now
                        lastBytesForSpeed = totalBytesWritten
                    }
                }
            }
        }

        // Write remaining buffer
        if !buffer.isEmpty {
            try fileHandle.write(contentsOf: buffer)
            totalBytesWritten += Int64(buffer.count)
        }

        // Final progress update
        if let progress = progress {
            progress.completedUnitCount = totalBytesWritten
        }

        // Verify size if known
        if expectedSize > 0 && totalBytesWritten != expectedSize {
            throw HTTPClientError.requestError(
                "Downloaded size mismatch: expected \(expectedSize), got \(totalBytesWritten)"
            )
        }

        return (outputPath, response)
    }

    /// Creates a background URL session for downloads that can continue when the app is suspended.
    private func createBackgroundSession(identifier: String) -> URLSession {
        let config = URLSessionConfiguration.background(withIdentifier: identifier)
        config.isDiscretionary = false
        config.sessionSendsLaunchEvents = true
        return URLSession(configuration: config)
    }

    /// Download file to a destination URL (convenience method without progress tracking)
    /// - Parameters:
    ///   - repoPath: Path to file in repository
    ///   - repo: Repository identifier
    ///   - destination: Destination URL for downloaded file
    ///   - kind: Kind of repository
    ///   - revision: Git revision
    ///   - useRaw: Use raw endpoint
    /// - Returns: Final destination URL
    func downloadContentsOfFile(
        at repoPath: String,
        from repo: Repo.ID,
        to destination: URL,
        kind: Repo.Kind = .model,
        revision: String = "main",
        useRaw: Bool = false
    ) async throws -> URL {
        return try await downloadFile(
            at: repoPath,
            from: repo,
            to: destination,
            kind: kind,
            revision: revision,
            useRaw: useRaw,
            progress: nil
        )
    }
}

// MARK: - Progress Delegate

private final class DownloadProgressDelegate: NSObject, URLSessionDownloadDelegate, @unchecked Sendable {
    private let progress: Progress
    private var lastUpdateTime: Date
    private var lastBytesWritten: Int64 = 0
    private let minimumUpdateInterval: TimeInterval = 0.1  // Update speed every 100ms

    init(progress: Progress) {
        self.progress = progress
        self.lastUpdateTime = Date()
    }

    func urlSession(
        _: URLSession,
        downloadTask _: URLSessionDownloadTask,
        didWriteData _: Int64,
        totalBytesWritten: Int64,
        totalBytesExpectedToWrite: Int64
    ) {
        progress.totalUnitCount = totalBytesExpectedToWrite
        progress.completedUnitCount = totalBytesWritten

        // Calculate and report throughput
        let now = Date()
        let elapsed = now.timeIntervalSince(lastUpdateTime)
        if elapsed >= minimumUpdateInterval {
            let deltaBytes = totalBytesWritten - lastBytesWritten
            let speed = Double(deltaBytes) / elapsed
            progress.setUserInfoObject(speed, forKey: .throughputKey)
            lastUpdateTime = now
            lastBytesWritten = totalBytesWritten
        }
    }

    func urlSession(
        _: URLSession,
        downloadTask _: URLSessionDownloadTask,
        didFinishDownloadingTo _: URL
    ) {
        // The actual file handling is done in the async/await layer
    }
}

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
        let urlPath = "/api/\(kind.pluralized)/\(repo)/commit/\(branch)"
        let operations = repoPaths.map { path in
            Value.object(["op": .string("delete"), "path": .string(path)])
        }
        let params: [String: Value] = [
            "title": .string(message),
            "operations": .array(operations),
        ]

        let _: Bool = try await httpClient.fetch(.post, urlPath, params: params)
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
        let urlPath = "/api/\(kind.pluralized)/\(repo)/tree/\(revision)"
        let params: [String: Value]? = recursive ? ["recursive": .bool(true)] : nil

        return try await httpClient.fetch(.get, urlPath, params: params)
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
        let urlPath = "/\(repo)/resolve/\(revision)/\(repoPath)"
        var request = try await httpClient.createRequest(.head, urlPath)
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

// MARK: - Snapshot Download

public extension HubClient {
    /// Download a repository snapshot to a local directory.
    ///
    /// This method downloads all files from a repository to the specified destination.
    /// Files are automatically cached in the Python-compatible cache directory,
    /// allowing cache reuse between Swift and Python Hugging Face clients.
    ///
    /// - Parameters:
    ///   - repo: Repository identifier
    ///   - kind: Kind of repository
    ///   - destination: Local destination directory
    ///   - revision: Git revision (branch, tag, or commit)
    ///   - matching: Glob patterns to filter files (empty array downloads all files)
    ///   - progressHandler: Optional closure called with progress updates
    /// - Returns: URL to the local snapshot directory
    func downloadSnapshot(
        of repo: Repo.ID,
        kind: Repo.Kind = .model,
        to destination: URL,
        revision: String = "main",
        matching globs: [String] = [],
        progressHandler: ((Progress) -> Void)? = nil
    ) async throws -> URL {
        let filenames = try await listFiles(in: repo, kind: kind, revision: revision, recursive: true)
            .map(\.path)
            .filter { filename in
                guard !globs.isEmpty else { return true }
                return globs.contains { glob in
                    fnmatch(glob, filename, 0) == 0
                }
            }

        let progress = Progress(totalUnitCount: Int64(filenames.count))
        progressHandler?(progress)

        for filename in filenames {
            let fileProgress = Progress(totalUnitCount: 100, parent: progress, pendingUnitCount: 1)
            let fileDestination = destination.appendingPathComponent(filename)

            // downloadFile handles cache lookup and storage automatically
            _ = try await downloadFile(
                at: filename,
                from: repo,
                to: fileDestination,
                kind: kind,
                revision: revision,
                progress: fileProgress
            )

            if Task.isCancelled {
                return destination
            }

            fileProgress.completedUnitCount = 100
        }

        progressHandler?(progress)
        return destination
    }
}

// MARK: -

private struct UploadResponse: Codable {
    let path: String
    let commit: String?
}

// MARK: -

private extension URL {
    var mimeType: String? {
        guard let uti = UTType(filenameExtension: pathExtension) else {
            return nil
        }
        return uti.preferredMIMEType
    }
}
