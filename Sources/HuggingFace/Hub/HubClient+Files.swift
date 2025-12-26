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

        if let cache = cache, let etag = etag, let commitHash = commitHash {
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

    /// Download file to a destination URL with automatic resume support.
    ///
    /// If a previous download was interrupted, this method automatically resumes
    /// from where it left off using HTTP Range headers.
    ///
    /// - Parameters:
    ///   - repoPath: Path to file in repository
    ///   - repo: Repository identifier
    ///   - destination: Destination URL for downloaded file
    ///   - kind: Kind of repository
    ///   - revision: Git revision
    ///   - endpoint: Select resolve or raw endpoint
    ///   - cachePolicy: Cache policy for the request
    ///   - progress: Optional Progress object to track download progress
    /// - Returns: Final destination URL
    func downloadFile(
        at repoPath: String,
        from repo: Repo.ID,
        to destination: URL,
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
            // Create parent directory if needed
            try FileManager.default.createDirectory(
                at: destination.deletingLastPathComponent(),
                withIntermediateDirectories: true
            )
            // Copy from cache to destination (resolve symlinks first)
            let resolvedPath = cachedPath.resolvingSymlinksInPath()
            try? FileManager.default.removeItem(at: destination)
            try FileManager.default.copyItem(at: resolvedPath, to: destination)
            if let progress {
                progress.completedUnitCount = progress.totalUnitCount
            }
            return destination
        }
        if endpoint == .resolve, transport.shouldAttemptXet {
            do {
                if let downloaded = try await downloadFileWithXet(
                    repoPath: repoPath,
                    repo: repo,
                    kind: kind,
                    revision: revision,
                    destination: destination,
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
        return try await downloadWithCacheCoordination(
            request: request,
            metadata: metadata,
            repo: repo,
            kind: kind,
            revision: revision,
            repoPath: repoPath,
            destination: destination,
            progress: progress
        )
    }

    /// Downloads a file with cache coordination (blob check, locking) shared across platforms.
    ///
    /// This method handles:
    /// 1. Check if blob already exists → skip download
    /// 2. Acquire file lock to prevent parallel downloads
    /// 3. Platform-specific download (with resume on Apple, without on Linux)
    /// 4. Store in cache and copy to destination
    private func downloadWithCacheCoordination(
        request: URLRequest,
        metadata: FileMetadata?,
        repo: Repo.ID,
        kind: Repo.Kind,
        revision: String,
        repoPath: String,
        destination: URL,
        progress: Progress?
    ) async throws -> URL {
        let fileManager = FileManager.default

        // If no cache or etag, download directly to destination
        // Note: metadata.etag is already normalized by fetchFileMetadata
        guard let normalizedEtag = metadata?.etag, let cache = cache else {
            return try await downloadToDestinationWithoutCache(
                request: request,
                destination: destination,
                progress: progress
            )
        }
        let blobsDir = cache.blobsDirectory(repo: repo, kind: kind)
        let blobPath = blobsDir.appendingPathComponent(normalizedEtag)
        let commitHash = metadata?.commitHash ?? revision

        // Check if blob already exists (skip download)
        if fileManager.fileExists(atPath: blobPath.path) {
            return try copyBlobToDestination(
                cache: cache,
                blobPath: blobPath,
                repo: repo,
                kind: kind,
                revision: revision,
                commitHash: commitHash,
                repoPath: repoPath,
                etag: normalizedEtag,
                destination: destination
            )
        }

        // Acquire lock to prevent parallel downloads of the same blob
        let lock = FileLock(path: blobPath)
        return try await lock.withLock {
            // Double-check blob doesn't exist after acquiring lock
            if fileManager.fileExists(atPath: blobPath.path) {
                return try copyBlobToDestination(
                    cache: cache,
                    blobPath: blobPath,
                    repo: repo,
                    kind: kind,
                    revision: revision,
                    commitHash: commitHash,
                    repoPath: repoPath,
                    etag: normalizedEtag,
                    destination: destination
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

            // Create symlink and copy to destination
            return try copyBlobToDestination(
                cache: cache,
                blobPath: blobPath,
                repo: repo,
                kind: kind,
                revision: revision,
                commitHash: commitHash,
                repoPath: repoPath,
                etag: normalizedEtag,
                destination: destination
            )
        }
    }

    /// Copies a cached blob to the destination, creating symlinks as needed.
    private func copyBlobToDestination(
        cache: HubCache,
        blobPath: URL,
        repo: Repo.ID,
        kind: Repo.Kind,
        revision: String,
        commitHash: String,
        repoPath: String,
        etag: String,
        destination: URL
    ) throws -> URL {
        let fileManager = FileManager.default

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

        // Copy from cache to destination
        try fileManager.createDirectory(
            at: destination.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try? fileManager.removeItem(at: destination)
        try fileManager.copyItem(at: blobPath, to: destination)

        return destination
    }

    /// Downloads directly to destination when cache is unavailable.
    private func downloadToDestinationWithoutCache(
        request: URLRequest,
        destination: URL,
        progress: Progress?
    ) async throws -> URL {
        let fileManager = FileManager.default

        #if canImport(FoundationNetworking)
            let (tempURL, response) = try await session.asyncDownload(for: request, progress: progress)
        #else
            let (tempURL, response) = try await session.download(
                for: request,
                delegate: progress.map { DownloadProgressDelegate(progress: $0) }
            )
        #endif

        _ = try httpClient.validateResponse(response, data: nil)

        try fileManager.createDirectory(
            at: destination.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try? fileManager.removeItem(at: destination)
        try fileManager.moveItem(at: tempURL, to: destination)

        return destination
    }

    /// Download file to a destination URL using a tree entry (uses file size for transport selection).
    /// - Parameters:
    ///   - entry: File entry from the repository tree
    ///   - repo: Repository identifier
    ///   - destination: Destination URL for downloaded file
    ///   - kind: Kind of repository
    ///   - revision: Git revision
    ///   - endpoint: Select resolve or raw endpoint
    ///   - cachePolicy: Cache policy for the request
    ///   - progress: Optional Progress object to track download progress
    ///   - transport: Download transport selection
    /// - Returns: Final destination URL
    func downloadFile(
        _ entry: Git.TreeEntry,
        from repo: Repo.ID,
        to destination: URL,
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
        private func downloadToCacheApple(
            request: URLRequest,
            blobPath: URL,
            etag: String,
            blobsDir: URL,
            progress: Progress?
        ) async throws {
            let fileManager = FileManager.default
            let incompletePath = blobsDir.appendingPathComponent("\(etag).incomplete")

            // Check for incomplete file to resume
            var resumeSize: Int64 = 0
            if fileManager.fileExists(atPath: incompletePath.path),
                let attrs = try? fileManager.attributesOfItem(atPath: incompletePath.path),
                let size = attrs[.size] as? Int64
            {
                resumeSize = size
            } else {
                fileManager.createFile(atPath: incompletePath.path, contents: nil)
            }

            // Add Range header if resuming
            var resumeRequest = request
            if resumeSize > 0 {
                resumeRequest.setValue("bytes=\(resumeSize)-", forHTTPHeaderField: "Range")
            }

            // Start streaming download
            let (asyncBytes, response) = try await session.bytes(for: resumeRequest)

            guard let httpResponse = response as? HTTPURLResponse else {
                throw HTTPClientError.unexpectedError("Invalid HTTP response")
            }

            let statusCode = httpResponse.statusCode
            guard (200 ..< 300).contains(statusCode) else {
                if statusCode == 416 {
                    // Range not satisfiable - delete incomplete and retry
                    try? fileManager.removeItem(at: incompletePath)
                    return try await downloadToCacheApple(
                        request: request,
                        blobPath: blobPath,
                        etag: etag,
                        blobsDir: blobsDir,
                        progress: progress
                    )
                }
                throw HTTPClientError.responseError(
                    response: httpResponse,
                    detail: "Download failed with status \(statusCode)"
                )
            }

            // Determine expected total size
            let contentLength = Int64(httpResponse.value(forHTTPHeaderField: "Content-Length") ?? "") ?? 0
            let expectedTotalSize: Int64
            if statusCode == 206 {
                expectedTotalSize = resumeSize + contentLength
            } else {
                if resumeSize > 0 {
                    try? fileManager.removeItem(at: incompletePath)
                    fileManager.createFile(atPath: incompletePath.path, contents: nil)
                    resumeSize = 0
                }
                expectedTotalSize = contentLength
            }

            // Set up progress tracking
            if let progress, progress.totalUnitCount == 0 {
                progress.totalUnitCount = expectedTotalSize
            }
            progress?.completedUnitCount = resumeSize
            let startTime = CFAbsoluteTimeGetCurrent()

            // Open file for appending
            let fileHandle = try FileHandle(forWritingTo: incompletePath)
            defer { try? fileHandle.close() }

            if resumeSize > 0 {
                try fileHandle.seekToEnd()
            }

            // Stream bytes to file
            var totalBytesWritten = resumeSize
            var buffer = Data()
            let bufferSize = 65536

            for try await byte in asyncBytes {
                try Task.checkCancellation()
                buffer.append(byte)

                if buffer.count >= bufferSize {
                    try fileHandle.write(contentsOf: buffer)
                    totalBytesWritten += Int64(buffer.count)
                    buffer.removeAll(keepingCapacity: true)

                    progress?.completedUnitCount = totalBytesWritten
                    if expectedTotalSize > 0 {
                        let elapsed = CFAbsoluteTimeGetCurrent() - startTime
                        if elapsed > 0 {
                            progress?.setUserInfoObject(
                                Double(totalBytesWritten - resumeSize) / elapsed,
                                forKey: .throughputKey
                            )
                        }
                    }
                }
            }

            if !buffer.isEmpty {
                try fileHandle.write(contentsOf: buffer)
                totalBytesWritten += Int64(buffer.count)
            }

            try fileHandle.close()

            // Verify file size
            if expectedTotalSize > 0 && totalBytesWritten != expectedTotalSize {
                throw HubCacheError.integrityError(
                    expected: "\(expectedTotalSize) bytes",
                    actual: "\(totalBytesWritten) bytes"
                )
            }

            // Move incomplete to final blob path
            try? fileManager.removeItem(at: blobPath)
            try fileManager.moveItem(at: incompletePath, to: blobPath)
        }
    #endif

    #if !canImport(FoundationNetworking)
        /// Download file with resume capability using URLSession resume data.
        ///
        /// - Note: This method is only available on Apple platforms.
        ///   On Linux, resume functionality is not supported.
        ///
        /// - Warning: This method does not store the result in the cache.
        ///
        /// - Parameters:
        ///   - resumeData: Resume data from a previous download attempt
        ///   - destination: Destination URL for downloaded file
        ///   - progress: Optional Progress object to track download progress
        /// - Returns: Final destination URL
        @available(
            *,
            deprecated,
            message:
                "Use downloadFile() instead, which provides automatic resume support via HTTP Range headers and stores results in the cache."
        )
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
        to destination: URL,
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
    /// Delegate for tracking download progress with throughput calculation.
    /// Safe to mark @unchecked Sendable: mutable state is only accessed from URLSession's
    /// delegate queue, which serializes all callbacks.
    private final class DownloadProgressDelegate: NSObject, URLSessionDownloadDelegate, @unchecked Sendable {
        private let progress: Progress
        private let startTime: CFAbsoluteTime
        private var lastUpdateTime: CFAbsoluteTime
        private var lastBytesWritten: Int64 = 0

        init(progress: Progress) {
            self.progress = progress
            self.startTime = CFAbsoluteTimeGetCurrent()
            self.lastUpdateTime = startTime
        }

        func urlSession(
            _: URLSession,
            downloadTask _: URLSessionDownloadTask,
            didWriteData bytesWritten: Int64,
            totalBytesWritten: Int64,
            totalBytesExpectedToWrite: Int64
        ) {
            progress.totalUnitCount = totalBytesExpectedToWrite
            progress.completedUnitCount = totalBytesWritten

            // Calculate throughput (bytes per second)
            let currentTime = CFAbsoluteTimeGetCurrent()
            let elapsedSinceStart = currentTime - startTime

            if elapsedSinceStart > 0 {
                let bytesPerSecond = Double(totalBytesWritten) / elapsedSinceStart
                progress.setUserInfoObject(bytesPerSecond, forKey: .throughputKey)
            }

            lastUpdateTime = currentTime
            lastBytesWritten = totalBytesWritten
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
    /// Files are downloaded in parallel (up to `maxConcurrent` simultaneous downloads)
    /// and progress is weighted by file size for accurate reporting.
    ///
    /// In offline mode (explicit or auto-detected), this method returns cached files
    /// without making network requests. An error is thrown if required files are not cached.
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
        to destination: URL,
        revision: String = "main",
        matching globs: [String] = [],
        maxConcurrent: Int = 8,
        progressHandler: ((Progress) -> Void)? = nil
    ) async throws -> URL {
        // Handle offline mode (returns all cached files, ignoring globs - matches huggingface_hub)
        if await shouldUseOfflineMode() {
            return try await downloadSnapshotOffline(
                of: repo,
                kind: kind,
                to: destination,
                revision: revision
            )
        }

        // Optimization: Skip API calls only when revision is already a commit hash
        // (immutable). For branch names like "main", we must check the remote to
        // get the current commit - the branch may have been updated.
        // This matches huggingface_hub's behavior for safety.
        if isCommitHash(revision),
            let cache = cache,
            let snapshotPath = cache.snapshotPath(repo: repo, kind: kind, revision: revision)
        {
            let cachedFiles = try? enumerateCachedFiles(at: snapshotPath, matching: globs)
            if let cachedFiles = cachedFiles, !cachedFiles.isEmpty {
                // All files are cached - just copy to destination without API calls
                try copyCachedFiles(cachedFiles, from: snapshotPath, to: destination)

                // Report complete progress
                let totalProgress = Progress(totalUnitCount: 1)
                totalProgress.completedUnitCount = 1
                progressHandler?(totalProgress)

                return destination
            }
        }

        // Not fully cached - need API call to get file list with sizes
        // Uses blobs=true for size-weighted progress (single API call)
        let repoInfo = try await getRepoInfo(for: repo, kind: kind, revision: revision)
        let commitHash = repoInfo.commitHash

        guard let siblings = repoInfo.siblings else {
            throw HubCacheError.offlineModeError("Could not get file list for repository")
        }

        let entries = siblings.filter { entry in
            guard !globs.isEmpty else { return true }
            return globs.contains { glob in
                fnmatch(glob, entry.path, 0) == 0
            }
        }

        // Size-weighted progress: total is sum of file sizes (bytes)
        let totalBytes = entries.reduce(Int64(0)) { $0 + Int64($1.size ?? 1) }
        let totalProgress = Progress(totalUnitCount: totalBytes)
        let startTime = CFAbsoluteTimeGetCurrent()
        progressHandler?(totalProgress)

        // Track speed updates - updated periodically, handler called after file completions
        let speedUpdateTask = Task {
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 250_000_000)  // 250ms
                let elapsed = CFAbsoluteTimeGetCurrent() - startTime
                let bytesCompleted = totalProgress.completedUnitCount
                // Only report speed after we've actually downloaded something
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
                // Wait for a slot to open up
                while activeCount >= maxConcurrent {
                    try await group.next()
                    activeCount -= 1
                    // Notify handler after each file completes (includes speed from background task)
                    progressHandler?(totalProgress)
                }

                // Check for cancellation
                if Task.isCancelled {
                    break
                }

                // Size-weighted progress for this file (using bytes as units throughout)
                let fileSize = Int64(entry.size ?? 1)
                let fileProgress = Progress(totalUnitCount: fileSize, parent: totalProgress, pendingUnitCount: fileSize)
                let fileDestination = destination.appendingPathComponent(entry.path)

                group.addTask {
                    _ = try await self.downloadFile(
                        at: entry.path,
                        from: repo,
                        to: fileDestination,
                        kind: kind,
                        revision: commitHash,  // Use SHA to enable cache shortcut
                        progress: fileProgress
                    )
                    // Ensure completion (download may have updated totalUnitCount)
                    fileProgress.completedUnitCount = fileProgress.totalUnitCount
                }
                activeCount += 1
            }

            // Drain remaining tasks and notify handler for each
            for try await _ in group {
                progressHandler?(totalProgress)
            }
        }

        // Update ref mapping if we resolved a branch/tag to commit hash
        if revision != commitHash, let cache = cache {
            try? cache.updateRef(repo: repo, kind: kind, ref: revision, commit: commitHash)
        }

        // Compute final speed before last handler call
        let elapsed = CFAbsoluteTimeGetCurrent() - startTime
        if elapsed > 0 && totalProgress.completedUnitCount > 0 {
            let finalSpeed = Double(totalProgress.completedUnitCount) / elapsed
            totalProgress.setUserInfoObject(finalSpeed, forKey: .throughputKey)
        }

        progressHandler?(totalProgress)
        return destination
    }

    /// Download a repository snapshot with aggregate speed reporting.
    ///
    /// The speed (in bytes/sec) is the aggregate throughput across all concurrent downloads,
    /// calculated as total bytes downloaded divided by elapsed time.
    ///
    /// - Parameters:
    ///   - repo: Repository identifier
    ///   - kind: Kind of repository
    ///   - destination: Local destination directory
    ///   - revision: Git revision (branch, tag, or commit)
    ///   - matching: Glob patterns to filter files (empty array downloads all files)
    ///   - maxConcurrent: Maximum number of concurrent downloads (default: 8)
    ///   - progressHandler: Closure called with progress and speed (bytes/sec)
    /// - Returns: URL to the local snapshot directory
    @discardableResult
    func downloadSnapshot(
        of repo: Repo.ID,
        kind: Repo.Kind = .model,
        to destination: URL,
        revision: String = "main",
        matching globs: [String] = [],
        maxConcurrent: Int = 8,
        progressHandler: @escaping (Progress, Double?) -> Void
    ) async throws -> URL {
        try await downloadSnapshot(
            of: repo,
            kind: kind,
            to: destination,
            revision: revision,
            matching: globs,
            maxConcurrent: maxConcurrent
        ) { progress in
            let speed = progress.userInfo[.throughputKey] as? Double
            progressHandler(progress, speed)
        }
    }

    /// Handles snapshot download in offline mode using only cached files.
    ///
    /// Matches huggingface_hub behavior: returns all cached files, ignoring glob patterns.
    /// As huggingface_hub notes: "we can't check if all the files are actually there" —
    /// this is a best-effort approach that returns whatever is cached.
    private func downloadSnapshotOffline(
        of repo: Repo.ID,
        kind: Repo.Kind,
        to destination: URL,
        revision: String
    ) async throws -> URL {
        guard let cache = cache else {
            throw HubCacheError.offlineModeError("Cache is disabled but offline mode requires cached files")
        }

        // Get cached snapshot path for this repo/revision
        guard let snapshotPath = cache.snapshotPath(repo: repo, kind: kind, revision: revision) else {
            throw HubCacheError.offlineModeError("Repository '\(repo)' not available in cache")
        }

        // Verify snapshot directory exists
        guard FileManager.default.fileExists(atPath: snapshotPath.path) else {
            throw HubCacheError.offlineModeError("Repository '\(repo)' not available in cache")
        }

        // Get all cached files (ignore glob patterns in offline mode, matching huggingface_hub)
        let cachedFiles = try enumerateCachedFiles(at: snapshotPath, matching: [])

        if cachedFiles.isEmpty {
            throw HubCacheError.offlineModeError("No cached files available for '\(repo)'")
        }

        // Copy cached files to destination
        try copyCachedFiles(cachedFiles, from: snapshotPath, to: destination)

        return destination
    }

    /// Copies cached files from snapshot path to destination.
    private func copyCachedFiles(_ files: [String], from snapshotPath: URL, to destination: URL) throws {
        let fileManager = FileManager.default
        try fileManager.createDirectory(at: destination, withIntermediateDirectories: true)

        for relativePath in files {
            let sourceURL = snapshotPath.appendingPathComponent(relativePath)
            let destURL = destination.appendingPathComponent(relativePath)

            try fileManager.createDirectory(
                at: destURL.deletingLastPathComponent(),
                withIntermediateDirectories: true
            )

            // Copy file (resolve symlinks first since cache uses symlinks)
            let resolvedSource = sourceURL.resolvingSymlinksInPath()
            try? fileManager.removeItem(at: destURL)
            try fileManager.copyItem(at: resolvedSource, to: destURL)
        }
    }

    /// Enumerates files in a cached snapshot directory that match the given globs.
    ///
    /// This is used to check if all requested files are cached before making API calls.
    private func enumerateCachedFiles(at snapshotPath: URL, matching globs: [String]) throws -> [String] {
        let fileManager = FileManager.default

        guard
            let enumerator = fileManager.enumerator(
                at: snapshotPath,
                includingPropertiesForKeys: [.isRegularFileKey, .isSymbolicLinkKey],
                options: [.skipsHiddenFiles]
            )
        else {
            return []
        }

        var files: [String] = []
        while let fileURL = enumerator.nextObject() as? URL {
            let resourceValues = try fileURL.resourceValues(forKeys: [.isRegularFileKey, .isSymbolicLinkKey])
            // Include both regular files and symlinks (cache uses symlinks to blobs)
            if resourceValues.isRegularFile == true || resourceValues.isSymbolicLink == true {
                // Get relative path from snapshot root
                let relativePath = fileURL.path.replacingOccurrences(
                    of: snapshotPath.path + "/",
                    with: ""
                )
                files.append(relativePath)
            }
        }

        // Filter by globs if specified
        if globs.isEmpty {
            return files
        }
        return files.filter { path in
            globs.contains { glob in
                fnmatch(glob, path, 0) == 0
            }
        }
    }
}

/// Holds per-file progress observation state.
private struct FileProgressReporter {
    let observer: ProgressObservation
    let continuation: AsyncStream<Double>.Continuation
    let task: Task<Void, Never>
    let samplingTask: Task<Void, Never>?

    /// Creates a per-file progress reporter that coalesces frequent updates and
    /// delivers callbacks on the main actor.
    /// On platforms that lack KVO for `Progress` (e.g. Linux),
    /// progress is polled at `minimumInterval`.
    ///
    /// - Parameters:
    ///   - parentProgress: Parent progress aggregating file-level progress.
    ///   - fileProgress: Progress instance for the current file.
    ///   - progressHandler: Callback invoked on the main actor.
    ///   - minimumDelta: Minimum progress fraction delta required to report. Defaults to 0.01.
    ///   - minimumInterval: Minimum time interval between reports. Defaults to 100 milliseconds.
    init?(
        parentProgress: Progress,
        fileProgress: Progress,
        progressHandler: (@Sendable (Progress) -> Void)?,
        minimumDelta: Double = 0.01,
        minimumInterval: Duration = .milliseconds(100)
    ) {
        guard let progressHandler else { return nil }

        // Use a continuous clock to track time intervals
        let clock = ContinuousClock()

        // Stream progress updates from KVO into a single async consumer
        let (progressStream, continuation) = AsyncStream<Double>.makeStream()

        // Coalesce updates on a task that delivers on the main actor
        let task = Task {
            var lastReportedFraction = -1.0
            var lastReportedTime = clock.now

            for await current in progressStream {
                let now = clock.now
                if current >= 1.0 || lastReportedFraction < 0 {
                    lastReportedFraction = current
                    lastReportedTime = now
                    await MainActor.run {
                        progressHandler(parentProgress)
                    }
                    continue
                }

                // Enforce both delta and time-based throttling
                guard current - lastReportedFraction >= minimumDelta else { continue }
                guard now - lastReportedTime >= minimumInterval else { continue }

                lastReportedFraction = current
                lastReportedTime = now
                await MainActor.run {
                    progressHandler(parentProgress)
                }
            }
        }

        // KVO drives the stream; the task does the throttled delivery
        let observer: ProgressObservation
        var samplingTask: Task<Void, Never>?
        #if canImport(FoundationNetworking)
            observer = ProgressObservation()
            samplingTask = Task {
                while !Task.isCancelled {
                    continuation.yield(fileProgress.fractionCompleted)
                    try? await Task.sleep(for: minimumInterval)
                }
            }
        #else
            observer = fileProgress.observe(\.fractionCompleted, options: [.new]) { _, change in
                guard change.newValue != nil else { return }
                continuation.yield(fileProgress.fractionCompleted)
            }
        #endif

        self.observer = observer
        self.continuation = continuation
        self.task = task
        self.samplingTask = samplingTask
    }

    func finish() async {
        // Ensure observation and coalescing task are torn down cleanly
        observer.invalidate()
        continuation.finish()
        samplingTask?.cancel()
        _ = await task.result
        if let samplingTask {
            _ = await samplingTask.result
        }
    }
}

#if canImport(FoundationNetworking)
    private struct ProgressObservation {
        func invalidate() {}
    }
#else
    private typealias ProgressObservation = NSKeyValueObservation
#endif

// MARK: - Xet Operations

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

    func fetchXetFileID(
        repoPath: String,
        repo: Repo.ID,
        revision: String,
        transport: FileDownloadTransport
    ) async throws -> String? {
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

        return isValidHash(fileID, pattern: sha256Pattern) ? fileID : nil
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

/// Normalizes an ETag by removing the weak validator prefix and quotes.
private func normalizeEtag(_ etag: String) -> String {
    var result = etag
    // Strip weak validator prefix if present (e.g., W/"abc" -> "abc")
    if result.hasPrefix("W/") {
        result = String(result.dropFirst(2))
    }
    // Strip surrounding quotes
    return result.trimmingCharacters(in: CharacterSet(charactersIn: "\""))
}

/// Checks if a string is a valid Git commit hash (40 hex characters).
private func isCommitHash(_ string: String) -> Bool {
    guard string.count == 40 else { return false }
    return string.allSatisfy { $0.isHexDigit }
}

// MARK: - File Entry

/// A file entry with path and optional size, used for snapshot downloads.
struct FileEntry {
    let path: String
    let size: Int?
}

/// Repository info with commit hash and file list.
struct RepoInfoForDownload {
    let commitHash: String
    let siblings: [FileEntry]?
}

extension HubClient {
    /// Gets repository info including commit hash and file list with sizes in a single API call.
    /// Uses `filesMetadata=true` to include file sizes for size-weighted progress reporting.
    func getRepoInfo(for repo: Repo.ID, kind: Repo.Kind, revision: String) async throws -> RepoInfoForDownload {
        switch kind {
        case .model:
            let model = try await getModel(repo, revision: revision, filesMetadata: true)
            guard let sha = model.sha else {
                throw HubCacheError.offlineModeError("Could not resolve revision '\(revision)' to commit hash")
            }
            let siblings = model.siblings?.map { FileEntry(path: $0.relativeFilename, size: $0.size) }
            return RepoInfoForDownload(commitHash: sha, siblings: siblings)
        case .dataset:
            let dataset = try await getDataset(repo, revision: revision, filesMetadata: true)
            guard let sha = dataset.sha else {
                throw HubCacheError.offlineModeError("Could not resolve revision '\(revision)' to commit hash")
            }
            let siblings = dataset.siblings?.map { FileEntry(path: $0.relativeFilename, size: $0.size) }
            return RepoInfoForDownload(commitHash: sha, siblings: siblings)
        case .space:
            let space = try await getSpace(repo, revision: revision, filesMetadata: true)
            guard let sha = space.sha else {
                throw HubCacheError.offlineModeError("Could not resolve revision '\(revision)' to commit hash")
            }
            let siblings = space.siblings?.map { FileEntry(path: $0.relativeFilename, size: $0.size) }
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

        // Create a session that follows same-host redirects but blocks CDN redirects
        let config = URLSessionConfiguration.default
        let session = URLSession(configuration: config, delegate: SameHostRedirectDelegate.shared, delegateQueue: nil)
        defer { session.invalidateAndCancel() }

        let (_, response) = try await session.data(for: request)

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
            etag: etag.map { normalizeEtag($0) }
        )
    }
}

/// URLSession delegate that follows relative redirects (same host) but blocks absolute redirects (different host).
/// This matches huggingface_hub's `_httpx_follow_relative_redirects` behavior.
/// - Relative redirects (e.g., renamed repos on same host) are followed automatically.
/// - Absolute redirects (e.g., to CDN) are blocked so we can capture X-Repo-Commit and X-Linked-Etag headers.
/// Safe to mark @unchecked Sendable: stateless singleton with no mutable state.
private final class SameHostRedirectDelegate: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
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
