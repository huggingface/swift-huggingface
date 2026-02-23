import Crypto
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
}

// MARK: - Upload Operations

/// Response from the upload endpoint.
private struct UploadResponse: Codable {
    /// The path to the uploaded file.
    let path: String

    /// The commit hash of the uploaded file.
    let commit: String?
}

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

/// Metadata associated with a Hub file response.
private struct FileMetadata: Sendable {
    /// The repository commit hash from the `X-Repo-Commit` header.
    let commitHash: String?

    /// The raw ETag value from `X-Linked-Etag` or `ETag`, when available.
    let etag: String?

    /// The normalized ETag used for cache storage.
    let normalizedEtag: String?

    /// Creates file metadata from response header values.
    ///
    /// - Parameters:
    ///   - commitHash: The repository commit hash, if provided.
    ///   - etag: The normalized ETag value, if provided.
    init(commitHash: String?, etag: String?) {
        self.commitHash = commitHash
        self.etag = etag
        normalizedEtag = etag.map(Self.normalizeEtag)
    }

    /// Creates file metadata from an HTTP response.
    ///
    /// - Parameter response: The response to parse.
    /// - Returns: `nil` when the response is not an HTTP response,
    ///            or when it does not contain either metadata header.
    init?(response: URLResponse) {
        guard let httpResponse = response as? HTTPURLResponse else {
            return nil
        }

        let linkedEtag = httpResponse.value(forHTTPHeaderField: "X-Linked-Etag")
        let etag = linkedEtag ?? httpResponse.value(forHTTPHeaderField: "ETag")
        let commitHash = httpResponse.value(forHTTPHeaderField: "X-Repo-Commit")
        guard commitHash != nil || etag != nil else {
            return nil
        }

        self.init(commitHash: commitHash, etag: etag)
    }

    private static func normalizeEtag(_ etag: String) -> String {
        var normalized = etag

        // Remove weak validator prefix
        if normalized.hasPrefix("W/") {
            normalized = String(normalized.dropFirst(2))
        }

        // Remove surrounding quotes
        if normalized.hasPrefix("\""), normalized.hasSuffix("\"") {
            normalized = String(normalized.dropFirst().dropLast())
        }

        return normalized
    }
}

/// Metadata about a cached snapshot of a repository.
private struct CachedSnapshotMetadata: Codable, Sendable {
    /// The entries in the snapshot.
    let entries: [Git.TreeEntry]
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
        let url = httpClient.host
            .appending(path: repo.namespace)
            .appending(path: repo.name)
            .appending(path: endpoint.rawValue)
            .appending(component: revision)
            .appending(path: repoPath)

        // HEAD preflight to capture Hub metadata before potential cross-host CDN redirect.
        // Only needed when caching is enabled, and failures should not block the download.
        let preflightMetadata: FileMetadata? =
            if cache != nil {
                try? await fetchFileMetadata(url: url)
            } else {
                nil
            }

        // GET request for the actual file bytes
        var request = try await httpClient.createRequest(.get, url: url)
        request.cachePolicy = cachePolicy
        let (data, response) = try await session.data(for: request)
        _ = try httpClient.validateResponse(response, data: data)

        // Store in cache if we have etag and commit info,
        // using the fallback metadata from the GET response object (no extra request)
        let responseMetadata = FileMetadata(response: response)
        if let cache = cache,
            let etag = preflightMetadata?.normalizedEtag ?? responseMetadata?.normalizedEtag,
            let commitHash = preflightMetadata?.commitHash ?? responseMetadata?.commitHash
        {
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

    /// Download file to a destination URL using URLSession.downloadTask
    /// - Parameters:
    ///   - repoPath: Path to file in repository
    ///   - repo: Repository identifier
    ///   - destination: Optional destination URL for downloaded file
    ///   - kind: Kind of repository
    ///   - revision: Git revision
    ///   - endpoint: Select resolve or raw endpoint
    ///   - cachePolicy: Cache policy for the request
    ///   - progress: Optional Progress object to track download progress
    /// - Returns: Cached file path, or destination path when provided.
    func downloadFile(
        at repoPath: String,
        from repo: Repo.ID,
        to destination: URL? = nil,
        kind: Repo.Kind = .model,
        revision: String = "main",
        endpoint: FileDownloadEndpoint = .resolve,
        cachePolicy: URLRequest.CachePolicy = .useProtocolCachePolicy,
        progress: Progress? = nil,
        transport: FileDownloadTransport = .automatic
    ) async throws -> URL {
        // Check cache first
        if let cache = cache,
            let cachedPath = cache.cachedFilePath(
                repo: repo,
                kind: kind,
                revision: revision,
                filename: repoPath
            )
        {
            if let progress {
                progress.completedUnitCount = progress.totalUnitCount
            }
            return try copyFileToLocalDirectoryIfNeeded(cachedPath, repoPath: repoPath, destination: destination)
        }
        if endpoint == .resolve, transport.shouldAttemptXet, let xetDestination = destination {
            do {
                if let downloaded = try await downloadFileWithXet(
                    repoPath: repoPath,
                    repo: repo,
                    kind: kind,
                    revision: revision,
                    destination: xetDestination,
                    progress: progress,
                    transport: transport
                ) {
                    return downloaded
                }
            } catch {
                if transport == .xet {
                    throw error
                }
            }
        }

        // Fallback to existing LFS download method
        let url = httpClient.host
            .appending(path: repo.namespace)
            .appending(path: repo.name)
            .appending(path: endpoint.rawValue)
            .appending(component: revision)
            .appending(path: repoPath)

        // HEAD preflight: capture Hub metadata before potential cross-host CDN redirect.
        // Only needed when caching is enabled, and failures should not block the download.
        let preflightMetadata: FileMetadata? =
            if cache != nil {
                try? await fetchFileMetadata(url: url)
            } else {
                nil
            }

        // GET request for the actual file bytes
        var request = try await httpClient.createRequest(.get, url: url)
        request.cachePolicy = cachePolicy
        #if canImport(FoundationNetworking)
            let (tempURL, response) = try await session.asyncDownload(for: request, progress: progress)
        #else
            let (tempURL, response) = try await session.download(
                for: request,
                delegate: progress.map { DownloadProgressDelegate(progress: $0) }
            )
        #endif
        _ = try httpClient.validateResponse(response, data: nil)

        // Store in cache before moving to destination
        // This fallback parses metadata from the existing GET response object (no extra request).
        let responseMetadata = FileMetadata(response: response)
        if let cache = cache,
            let etag = preflightMetadata?.normalizedEtag ?? responseMetadata?.normalizedEtag,
            let commitHash = preflightMetadata?.commitHash ?? responseMetadata?.commitHash
        {
            try? await cache.storeFile(
                at: tempURL,
                repo: repo,
                kind: kind,
                revision: commitHash,
                filename: repoPath,
                etag: etag,
                ref: revision != commitHash ? revision : nil
            )
            if let cachedPath = cache.cachedFilePath(
                repo: repo,
                kind: kind,
                revision: commitHash,
                filename: repoPath
            ) {
                return try copyFileToLocalDirectoryIfNeeded(
                    cachedPath,
                    repoPath: repoPath,
                    destination: destination
                )
            }
        }

        guard let destination else {
            throw HTTPClientError.unexpectedError(
                "Unable to determine cached file path for '\(repoPath)'; provide a destination"
            )
        }
        // Create parent directory if needed
        try FileManager.default.createDirectory(
            at: destination.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )

        // Move from temporary location to final destination
        try? FileManager.default.removeItem(at: destination)
        try FileManager.default.moveItem(at: tempURL, to: destination)

        return destination
    }

    /// Download file to a destination URL using a tree entry (uses file size for transport selection).
    /// - Parameters:
    ///   - entry: File entry from the repository tree
    ///   - repo: Repository identifier
    ///   - destination: Optional destination URL for downloaded file
    ///   - kind: Kind of repository
    ///   - revision: Git revision
    ///   - endpoint: Select resolve or raw endpoint
    ///   - cachePolicy: Cache policy for the request
    ///   - progress: Optional Progress object to track download progress
    ///   - transport: Download transport selection
    /// - Returns: Cached file path, or destination path when provided.
    func downloadFile(
        _ entry: Git.TreeEntry,
        from repo: Repo.ID,
        to destination: URL? = nil,
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
                to: destination,
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
            to: destination,
            kind: kind,
            revision: revision,
            endpoint: endpoint,
            cachePolicy: cachePolicy,
            progress: progress,
            transport: transport
        )
    }

    #if !canImport(FoundationNetworking)
        /// Download file with resume capability
        ///
        /// - Note: This method is only available on Apple platforms.
        ///   On Linux, resume functionality is not supported.
        ///
        /// - Parameters:
        ///   - resumeData: Resume data from a previous download attempt
        ///   - destination: Destination URL for downloaded file
        ///   - progress: Optional Progress object to track download progress
        /// - Returns: Final destination URL
        func resumeDownloadFile(
            resumeData: Data,
            to destination: URL,
            progress: Progress? = nil
        ) async throws -> URL {
            let (tempURL, response) = try await session.download(
                resumeFrom: resumeData,
                delegate: progress.map { DownloadProgressDelegate(progress: $0) }
            )
            _ = try httpClient.validateResponse(response, data: nil)

            // Move from temporary location to final destination
            try? FileManager.default.removeItem(at: destination)
            try FileManager.default.moveItem(at: tempURL, to: destination)

            return destination
        }
    #endif

    /// Download file to a destination URL (convenience method without progress tracking)
    /// - Parameters:
    ///   - repoPath: Path to file in repository
    ///   - repo: Repository identifier
    ///   - destination: Destination URL for downloaded file
    ///   - kind: Kind of repository
    ///   - revision: Git revision
    ///   - endpoint: Select resolve or raw endpoint
    ///   - cachePolicy: Cache policy for the request
    /// - Returns: Final destination URL
    func downloadContentsOfFile(
        at repoPath: String,
        from repo: Repo.ID,
        to destination: URL? = nil,
        kind: Repo.Kind = .model,
        revision: String = "main",
        endpoint: FileDownloadEndpoint = .resolve,
        cachePolicy: URLRequest.CachePolicy = .useProtocolCachePolicy,
        transport: FileDownloadTransport = .automatic
    ) async throws -> URL {
        return try await downloadFile(
            at: repoPath,
            from: repo,
            to: destination,
            kind: kind,
            revision: revision,
            endpoint: endpoint,
            cachePolicy: cachePolicy,
            progress: nil,
            transport: transport
        )
    }
}

// MARK: - Progress Delegate

#if !canImport(FoundationNetworking)
    private final class DownloadProgressDelegate: NSObject, URLSessionDownloadDelegate, @unchecked Sendable {
        private let progress: Progress

        init(progress: Progress) {
            self.progress = progress
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
        }

        func urlSession(
            _: URLSession,
            downloadTask _: URLSessionDownloadTask,
            didFinishDownloadingTo _: URL
        ) {
            // The actual file handling is done in the async/await layer
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

            let metadata = FileMetadata(response: response)
            let etag = metadata?.etag
            let revision = metadata?.commitHash

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
    /// Download a repository snapshot.
    ///
    /// This method downloads all files from a repository to the cache by default.
    /// Files are automatically cached in the Python-compatible cache directory,
    /// allowing cache reuse between Swift and Python Hugging Face clients.
    ///
    /// - Parameters:
    ///   - repo: Repository identifier
    ///   - kind: Kind of repository
    ///   - destination: Optional local destination directory
    ///   - revision: Git revision (branch, tag, or commit)
    ///   - matching: Glob patterns to filter files (empty array downloads all files)
    ///   - progressHandler: Optional closure called with progress updates.
    ///     Updates are delivered on the main actor.
    /// - Returns: URL to the cache snapshot directory by default,
    ///            or destination when provided.
    func downloadSnapshot(
        of repo: Repo.ID,
        kind: Repo.Kind = .model,
        to destination: URL? = nil,
        revision: String = "main",
        matching globs: [String] = [],
        progressHandler: (@Sendable (Progress) -> Void)? = nil
    ) async throws -> URL {
        if let fastPath = cachedSnapshotPath(
            repo: repo,
            kind: kind,
            revision: revision,
            matching: globs
        ) {
            let progress = Progress(totalUnitCount: 1)
            progress.completedUnitCount = 1
            if let progressHandler {
                await MainActor.run {
                    progressHandler(progress)
                }
            }
            return try copySnapshotToLocalDirectoryIfNeeded(from: fastPath, destination: destination)
        }

        let allEntries = try await listFiles(in: repo, kind: kind, revision: revision, recursive: true)
        let entries =
            allEntries
            .filter { entry in
                guard !globs.isEmpty else { return true }
                return globs.contains { glob in
                    fnmatch(glob, entry.path, 0) == 0
                }
            }

        let progress = Progress(totalUnitCount: Int64(entries.count))
        if let progressHandler {
            await MainActor.run {
                progressHandler(progress)
            }
        }

        if let cache, isCommitHash(revision) {
            try? saveCachedSnapshotMetadata(
                CachedSnapshotMetadata(entries: allEntries),
                cache: cache,
                repo: repo,
                kind: kind,
                commitHash: revision
            )
        }

        for entry in entries {
            try validateSnapshotEntryPath(entry.path)
            let fileProgress = Progress(totalUnitCount: 100, parent: progress, pendingUnitCount: 1)
            let fileDestination = destination?.appendingPathComponent(entry.path)
            #if canImport(FoundationNetworking)
                let samplingTask: Task<Void, Never>? =
                    if let progressHandler {
                        Task(priority: Task.currentPriority) {
                            while !Task.isCancelled {
                                await MainActor.run {
                                    progressHandler(progress)
                                }
                                try? await Task.sleep(for: .milliseconds(100))
                            }
                        }
                    } else {
                        nil
                    }
            #else
                let samplingTask: Task<Void, Never>? = nil
            #endif

            // downloadFile handles cache lookup and storage automatically
            do {
                _ = try await withTaskCancellationHandler {
                    try await downloadFile(
                        entry,
                        from: repo,
                        to: fileDestination,
                        kind: kind,
                        revision: revision,
                        progress: fileProgress
                    )
                } onCancel: {
                    samplingTask?.cancel()
                }
                try Task.checkCancellation()
            } catch {
                if let samplingTask {
                    samplingTask.cancel()
                    _ = await samplingTask.result
                }
                throw error
            }

            fileProgress.completedUnitCount = fileProgress.totalUnitCount
            if let samplingTask {
                samplingTask.cancel()
                _ = await samplingTask.result
            }
            if let progressHandler {
                await MainActor.run {
                    progressHandler(progress)
                }
            }
        }

        if let progressHandler {
            await MainActor.run {
                progressHandler(progress)
            }
        }

        let resolvedCommitHash =
            isCommitHash(revision)
            ? revision
            : cache?.resolveRevision(repo: repo, kind: kind, ref: revision) ?? revision
        let snapshotPath =
            (cache?.snapshotsDirectory(repo: repo, kind: kind) ?? destination ?? URL(fileURLWithPath: "."))
            .appendingPathComponent(resolvedCommitHash)

        if let destination {
            return destination
        }
        return try copySnapshotToLocalDirectoryIfNeeded(from: snapshotPath, destination: destination)
    }
}

// MARK: - Xet Operations

/// Metadata associated with a Xet-enabled Hub file response.
private struct XetFileMetadata: Sendable {
    /// The content-addressed Xet object ID from `X-Xet-Hash`.
    let fileID: String

    /// The linked file size in bytes, when available.
    let fileSizeBytes: Int?

    /// Creates Xet metadata from an HTTP response.
    ///
    /// - Parameter response: The response to parse.
    /// - Returns: `nil` when the response does not include a valid Xet file ID.
    init?(response: URLResponse) {
        guard let httpResponse = response as? HTTPURLResponse else {
            return nil
        }

        let rawFileID = httpResponse.value(forHTTPHeaderField: "X-Xet-Hash")?
            .trimmingCharacters(in: .whitespacesAndNewlines)
        guard let fileID = rawFileID, !fileID.isEmpty else {
            return nil
        }
        guard fileID.count == 64, fileID.allSatisfy(\.isHexDigit) else {
            return nil
        }

        let rawSize =
            httpResponse.value(forHTTPHeaderField: "X-Linked-Size")
            ?? ((200 ... 299).contains(httpResponse.statusCode)
                ? httpResponse.value(forHTTPHeaderField: "Content-Length")
                : nil)

        self.fileID = fileID
        fileSizeBytes = rawSize.flatMap(Int.init)
    }
}

private extension HubClient {
    /// Generates the path to a cached snapshot.
    func cachedSnapshotPath(
        repo: Repo.ID,
        kind: Repo.Kind,
        revision: String,
        matching globs: [String]
    ) -> URL? {
        guard let cache,
            isCommitHash(revision),
            let metadata = loadCachedSnapshotMetadata(
                cache: cache,
                repo: repo,
                kind: kind,
                commitHash: revision
            )
        else {
            return nil
        }

        let snapshotPath = cache.snapshotsDirectory(repo: repo, kind: kind).appendingPathComponent(revision)
        let requiredEntries = metadata.entries.filter { entry in
            guard !globs.isEmpty else { return true }
            return globs.contains { glob in
                fnmatch(glob, entry.path, 0) == 0
            }
        }

        let isComplete = requiredEntries.allSatisfy { entry in
            FileManager.default.fileExists(
                atPath: snapshotPath.appendingPathComponent(entry.path).path
            )
        }
        return isComplete ? snapshotPath : nil
    }

    /// Generates the URL for cached snapshot metadata.
    func cachedSnapshotMetadataURL(
        cache: HubCache,
        repo: Repo.ID,
        kind: Repo.Kind,
        commitHash: String
    ) -> URL {
        cache.metadataDirectory(repo: repo, kind: kind)
            .appendingPathComponent("\(commitHash).json")
    }

    /// Saves cached snapshot metadata to a file.
    func saveCachedSnapshotMetadata(
        _ metadata: CachedSnapshotMetadata,
        cache: HubCache,
        repo: Repo.ID,
        kind: Repo.Kind,
        commitHash: String
    ) throws {
        let url = cachedSnapshotMetadataURL(cache: cache, repo: repo, kind: kind, commitHash: commitHash)
        let data = try JSONEncoder().encode(metadata)
        try FileManager.default.createDirectory(
            at: url.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try data.write(to: url, options: .atomic)
    }

    /// Loads cached snapshot metadata from a file.
    func loadCachedSnapshotMetadata(
        cache: HubCache,
        repo: Repo.ID,
        kind: Repo.Kind,
        commitHash: String
    ) -> CachedSnapshotMetadata? {
        let url = cachedSnapshotMetadataURL(cache: cache, repo: repo, kind: kind, commitHash: commitHash)
        guard let data = try? Data(contentsOf: url) else {
            return nil
        }
        return try? JSONDecoder().decode(CachedSnapshotMetadata.self, from: data)
    }

    /// Copies a file to a local directory if needed.
    func copyFileToLocalDirectoryIfNeeded(
        _ source: URL,
        repoPath: String,
        destination: URL?
    ) throws -> URL {
        guard let destination else {
            return source
        }
        let target = destination.appendingPathComponent(repoPath)
        try FileManager.default.createDirectory(
            at: target.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try? FileManager.default.removeItem(at: target)
        let resolvedSource = source.resolvingSymlinksInPath()
        try FileManager.default.copyItem(at: resolvedSource, to: target)
        return target
    }

    /// Copies a snapshot to a local directory if needed.
    func copySnapshotToLocalDirectoryIfNeeded(
        from snapshotPath: URL,
        destination: URL?
    ) throws -> URL {
        guard let destination else {
            return snapshotPath
        }
        let fileManager = FileManager.default
        try fileManager.createDirectory(at: destination, withIntermediateDirectories: true)
        guard
            let enumerator = fileManager.enumerator(
                at: snapshotPath,
                includingPropertiesForKeys: [.isRegularFileKey, .isSymbolicLinkKey],
                options: [.skipsHiddenFiles]
            )
        else {
            return destination
        }
        while let fileURL = enumerator.nextObject() as? URL {
            let values = try fileURL.resourceValues(forKeys: [.isRegularFileKey, .isSymbolicLinkKey])
            guard values.isRegularFile == true || values.isSymbolicLink == true else {
                continue
            }
            let relativePath = fileURL.path.replacingOccurrences(of: snapshotPath.path + "/", with: "")
            let target = destination.appendingPathComponent(relativePath)
            try fileManager.createDirectory(
                at: target.deletingLastPathComponent(),
                withIntermediateDirectories: true
            )
            try? fileManager.removeItem(at: target)
            let resolvedSource = fileURL.resolvingSymlinksInPath()
            try fileManager.copyItem(at: resolvedSource, to: target)
        }
        return destination
    }

    /// Downloads file data using Xet's content-addressable storage system.
    func downloadDataWithXet(
        repoPath: String,
        repo: Repo.ID,
        kind: Repo.Kind,
        revision: String,
        transport: FileDownloadTransport
    ) async throws -> Data? {
        guard
            let fileID = try await fetchXetFileID(
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
            try await downloader.data(for: fileID)
        }
    }

    /// Downloads a file using Xet's content-addressable storage system.
    @discardableResult
    func downloadFileWithXet(
        repoPath: String,
        repo: Repo.ID,
        kind: Repo.Kind,
        revision: String,
        destination: URL,
        progress: Progress?,
        transport: FileDownloadTransport
    ) async throws -> URL? {
        guard
            let fileID = try await fetchXetFileID(
                repoPath: repoPath,
                repo: repo,
                revision: revision,
                transport: transport
            )
        else {
            return nil
        }

        _ = try await Xet.withDownloader(
            refreshURL: xetRefreshURL(for: repo, kind: kind, revision: revision),
            hubToken: try? await httpClient.tokenProvider.getToken()
        ) { downloader in
            try await downloader.download(fileID, to: destination)
        }

        progress?.totalUnitCount = 100
        progress?.completedUnitCount = 100

        return destination
    }

    /// Fetch the Xet file ID for a given repository, path, and revision.
    /// - Parameters:
    ///   - repoPath: Path to file
    ///   - repo: Repository identifier
    ///   - revision: Git revision
    ///   - transport: Transport to use
    /// - Returns: Xet file ID
    /// - Throws: Error if the file ID cannot be fetched
    func fetchXetFileID(
        repoPath: String,
        repo: Repo.ID,
        revision: String,
        transport: FileDownloadTransport
    ) async throws -> String? {
        let urlPath = "/\(repo)/resolve/\(revision)/\(repoPath)"
        var request = try await httpClient.createRequest(.head, urlPath)
        request.cachePolicy = .reloadIgnoringLocalCacheData

        #if canImport(FoundationNetworking)
            let (_, response) = try await metadataSession.data(for: request)
        #else
            let (_, response) = try await session.data(for: request, delegate: SameHostRedirectDelegate.shared)
        #endif
        guard let metadata = XetFileMetadata(response: response) else {
            return nil
        }

        if !transport.shouldUseXet(
            fileSizeBytes: metadata.fileSizeBytes,
            minimumFileSizeBytes: xetMinimumFileSizeBytes
        ) {
            return nil
        }

        return metadata.fileID
    }

    /// Generate the Xet refresh URL for a given repository, kind, and revision.
    func xetRefreshURL(for repo: Repo.ID, kind: Repo.Kind, revision: String) -> URL {
        let url = httpClient.host.appendingPathComponent(
            "api/\(kind.pluralized)/\(repo)/xet-read-token/\(revision)"
        )
        return url
    }

    /// Fetch metadata without following cross-host redirects.
    ///
    /// This captures `X-Linked-Etag` and `X-Repo-Commit` before a potential CDN redirect.
    func fetchFileMetadata(url: URL) async throws -> FileMetadata? {
        let request = try await httpClient.createRequest(.head, url: url)
        #if canImport(FoundationNetworking)
            let (_, response) = try await metadataSession.data(for: request)
        #else
            let (_, response) = try await session.data(for: request, delegate: SameHostRedirectDelegate.shared)
        #endif
        return FileMetadata(response: response)
    }

    /// Validates a snapshot entry path before writing to local disk.
    func validateSnapshotEntryPath(_ path: String) throws {
        guard !path.trimmingCharacters(in: .whitespaces).isEmpty else {
            throw HubCacheError.invalidPathComponent(path)
        }
        if path.contains("\0") || path.contains("\\") || path.hasPrefix("/") {
            throw HubCacheError.invalidPathComponent(path)
        }
        let components = path.split(separator: "/", omittingEmptySubsequences: false)
        guard components.allSatisfy({ !$0.isEmpty && $0 != ".." }) else {
            throw HubCacheError.invalidPathComponent(path)
        }
    }
}

// MARK: - Same-Host Redirect Delegate

/// Follows same-host redirects while blocking cross-host redirects.
final class SameHostRedirectDelegate: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
    static let shared = SameHostRedirectDelegate()

    private override init() {
        super.init()
    }

    func urlSession(
        _: URLSession,
        task: URLSessionTask,
        willPerformHTTPRedirection _: HTTPURLResponse,
        newRequest request: URLRequest,
        completionHandler: @escaping (URLRequest?) -> Void
    ) {
        guard let originalHost = task.originalRequest?.url?.host,
            let newHost = request.url?.host
        else {
            completionHandler(nil)
            return
        }
        completionHandler(originalHost == newHost ? request : nil)
    }
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

// MARK: -

private func isCommitHash(_ revision: String) -> Bool {
    revision.count == 40 && revision.allSatisfy(\.isHexDigit)
}
