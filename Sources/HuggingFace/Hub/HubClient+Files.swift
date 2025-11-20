import CryptoKit
import Foundation
import UniformTypeIdentifiers

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

#if canImport(Xet)
    import Xet
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
        var request = try httpClient.createRequest(.post, urlPath)

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
        kind _: Repo.Kind = .model,
        revision: String = "main",
        useRaw: Bool = false,
        cachePolicy: URLRequest.CachePolicy = .useProtocolCachePolicy
    ) async throws -> Data {
        #if canImport(Xet)
            if isXetEnabled {
                do {
                    let tempDirectory = FileManager.default.temporaryDirectory
                        .appendingPathComponent(UUID().uuidString, isDirectory: true)
                    let tempFile = tempDirectory.appendingPathComponent(UUID().uuidString)
                    try FileManager.default.createDirectory(at: tempDirectory, withIntermediateDirectories: true)
                    defer { try? FileManager.default.removeItem(at: tempDirectory) }

                    if try await downloadFileWithXet(
                        repoPath: repoPath,
                        repo: repo,
                        revision: revision,
                        destination: tempFile,
                        progress: nil
                    ) != nil {
                        return try Data(contentsOf: tempFile)
                    } else {
                        print("⚠️  Xet returned nil for \(repoPath), falling back to LFS")
                    }
                } catch {
                    print("⚠️  Xet failed for \(repoPath): \(error), falling back to LFS")
                }
            }
        #endif

        // Fallback to existing LFS download method
        let endpoint = useRaw ? "raw" : "resolve"
        let urlPath = "/\(repo)/\(endpoint)/\(revision)/\(repoPath)"
        var request = try httpClient.createRequest(.get, urlPath)
        request.cachePolicy = cachePolicy

        let (data, response) = try await session.data(for: request)
        _ = try httpClient.validateResponse(response, data: data)

        return data
    }

    /// Download file to a destination URL using URLSession.downloadTask
    /// - Parameters:
    ///   - repoPath: Path to file in repository
    ///   - repo: Repository identifier
    ///   - destination: Destination URL for downloaded file
    ///   - kind: Kind of repository
    ///   - revision: Git revision
    ///   - useRaw: Use raw endpoint
    ///   - cachePolicy: Cache policy for the request
    ///   - progress: Optional Progress object to track download progress
    /// - Returns: Final destination URL
    func downloadFile(
        at repoPath: String,
        from repo: Repo.ID,
        to destination: URL,
        kind _: Repo.Kind = .model,
        revision: String = "main",
        useRaw: Bool = false,
        cachePolicy: URLRequest.CachePolicy = .useProtocolCachePolicy,
        progress: Progress? = nil
    ) async throws -> URL {
        #if canImport(Xet)
            if isXetEnabled {
                do {
                    if let downloaded = try await downloadFileWithXet(
                        repoPath: repoPath,
                        repo: repo,
                        revision: revision,
                        destination: destination,
                        progress: progress
                    ) {
                        return downloaded
                    } else {
                        print("⚠️  Xet returned nil for \(repoPath), falling back to LFS")
                    }
                } catch {
                    print("⚠️  Xet failed for \(repoPath): \(error), falling back to LFS")
                }
            }
        #endif

        // Fallback to existing LFS download method
        let endpoint = useRaw ? "raw" : "resolve"
        let urlPath = "/\(repo)/\(endpoint)/\(revision)/\(repoPath)"
        var request = try httpClient.createRequest(.get, urlPath)
        request.cachePolicy = cachePolicy

        let (tempURL, response) = try await session.download(
            for: request,
            delegate: progress.map { DownloadProgressDelegate(progress: $0) }
        )
        _ = try httpClient.validateResponse(response, data: nil)

        // Move from temporary location to final destination
        try? FileManager.default.removeItem(at: destination)
        try FileManager.default.moveItem(at: tempURL, to: destination)

        return destination
    }

    /// Download file with resume capability
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

    /// Download file to a destination URL (convenience method without progress tracking)
    /// - Parameters:
    ///   - repoPath: Path to file in repository
    ///   - repo: Repository identifier
    ///   - destination: Destination URL for downloaded file
    ///   - kind: Kind of repository
    ///   - revision: Git revision
    ///   - useRaw: Use raw endpoint
    ///   - cachePolicy: Cache policy for the request
    /// - Returns: Final destination URL
    func downloadContentsOfFile(
        at repoPath: String,
        from repo: Repo.ID,
        to destination: URL,
        kind: Repo.Kind = .model,
        revision: String = "main",
        useRaw: Bool = false,
        cachePolicy: URLRequest.CachePolicy = .useProtocolCachePolicy
    ) async throws -> URL {
        return try await downloadFile(
            at: repoPath,
            from: repo,
            to: destination,
            kind: kind,
            revision: revision,
            useRaw: useRaw,
            cachePolicy: cachePolicy,
            progress: nil
        )
    }
}

// MARK: - Progress Delegate

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
        var request = try httpClient.createRequest(.head, urlPath)
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
        let repoDestination = destination
        let repoMetadataDestination =
            repoDestination
            .appendingPathComponent(".cache")
            .appendingPathComponent("huggingface")
            .appendingPathComponent("download")

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

            let fileDestination = repoDestination.appendingPathComponent(filename)
            let metadataDestination = repoMetadataDestination.appendingPathComponent(filename + ".metadata")

            let localMetadata = readDownloadMetadata(at: metadataDestination)
            let remoteFile = try await getFile(at: filename, in: repo, kind: kind, revision: revision)

            let localCommitHash = localMetadata?.commitHash ?? ""
            let remoteCommitHash = remoteFile.revision ?? ""

            if isValidHash(remoteCommitHash, pattern: commitHashPattern),
                FileManager.default.fileExists(atPath: fileDestination.path),
                localMetadata != nil,
                localCommitHash == remoteCommitHash
            {
                fileProgress.completedUnitCount = 100
                continue
            }

            _ = try await downloadFile(
                at: filename,
                from: repo,
                to: fileDestination,
                kind: kind,
                revision: revision,
                progress: fileProgress
            )

            if let etag = remoteFile.etag, let revision = remoteFile.revision {
                try writeDownloadMetadata(
                    commitHash: revision,
                    etag: etag,
                    to: metadataDestination
                )
            }

            if Task.isCancelled {
                return repoDestination
            }

            fileProgress.completedUnitCount = 100
        }

        progressHandler?(progress)
        return repoDestination
    }
}

#if canImport(Xet)
    private extension HubClient {
        /// Downloads a file using Xet's content-addressable storage system.
        ///
        /// This method uses a cached XetClient instance and JWT tokens to maximize
        /// download performance through connection reuse and reduced API overhead.
        ///
        /// Performance optimizations:
        /// - Reuses a single XetClient across all downloads for HTTP/TLS connection pooling
        /// - Caches CAS JWT tokens per (repo, revision) to avoid redundant API calls
        /// - Leverages Xet's parallel chunk downloading (configurable via XET_MAX_PARALLEL_FILES env var)
        @discardableResult
        func downloadFileWithXet(
            repoPath: String,
            repo: Repo.ID,
            revision: String,
            destination: URL,
            progress: Progress?
        ) async throws -> URL? {
            guard isXetEnabled else {
                return nil
            }

            let xetClient = try getXetClient()

            guard
                let fileInfo = try xetClient.getFileInfo(
                    repo: repo.rawValue,
                    path: repoPath,
                    revision: revision
                )
            else {
                return nil
            }

            let jwt = try getCachedJwt(
                xetClient: xetClient,
                repo: repo.rawValue,
                revision: revision,
                isUpload: false
            )

            let destinationDirectory = destination.deletingLastPathComponent()
            try FileManager.default.createDirectory(
                at: destinationDirectory,
                withIntermediateDirectories: true
            )

            let downloads = try xetClient.downloadFiles(
                fileInfos: [fileInfo],
                destinationDir: destinationDirectory.path,
                jwtInfo: jwt
            )

            guard let downloadedPath = downloads.first else {
                return nil
            }

            let downloadedURL = URL(fileURLWithPath: downloadedPath)

            if downloadedURL.standardizedFileURL != destination.standardizedFileURL {
                if FileManager.default.fileExists(atPath: destination.path) {
                    try FileManager.default.removeItem(at: destination)
                }
                try FileManager.default.moveItem(at: downloadedURL, to: destination)
            }

            progress?.totalUnitCount = 100
            progress?.completedUnitCount = 100

            return destination
        }
    }
#endif

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

        while autoreleasepool(invoking: {
            guard let nextChunk = try? fileHandle.read(upToCount: chunkSize),
                !nextChunk.isEmpty
            else {
                return false
            }

            hasher.update(data: nextChunk)
            return true
        }) {}

        let digest = hasher.finalize()
        return digest.map { String(format: "%02x", $0) }.joined()
    }
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
