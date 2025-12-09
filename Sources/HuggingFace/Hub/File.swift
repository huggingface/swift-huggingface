import Foundation

/// Information about a file in a repository.
public struct File: Hashable, Codable, Sendable {
    /// A Boolean value indicating whether the file exists in the repository.
    public let exists: Bool

    /// The size of the file in bytes.
    public let size: Int64?

    /// The entity tag (ETag) for the file, used for caching and change detection.
    public let etag: String?

    /// The Git revision (commit SHA) at which this file information was retrieved.
    public let revision: String?

    /// A Boolean value indicating whether the file is stored using Git Large File Storage (LFS).
    public let isLFS: Bool

    init(
        exists: Bool,
        size: Int64? = nil,
        etag: String? = nil,
        revision: String? = nil,
        isLFS: Bool = false
    ) {
        self.exists = exists
        self.size = size
        self.etag = etag
        self.revision = revision
        self.isLFS = isLFS
    }
}

// MARK: - File Metadata

/// Metadata about a downloaded file stored locally.
public struct LocalDownloadFileMetadata: Hashable, Codable, Sendable {
    /// Commit hash of the file in the repository.
    public let commitHash: String

    /// ETag of the file in the repository. Used to check if the file has changed.
    /// For LFS files, this is the sha256 of the file. For regular files, it corresponds to the git hash.
    public let etag: String

    /// Path of the file in the repository.
    public let filename: String

    /// The timestamp of when the metadata was saved (i.e., when the metadata was accurate).
    public let timestamp: Date

    public init(commitHash: String, etag: String, filename: String, timestamp: Date) {
        self.commitHash = commitHash
        self.etag = etag
        self.filename = filename
        self.timestamp = timestamp
    }
}

// MARK: -

/// A collection of files to upload in a batch operation.
///
/// Use `FileBatch` to prepare multiple files for uploading to a repository in a single operation.
/// You can add files using subscript notation or dictionary literal syntax.
///
/// ```swift
/// var batch = FileBatch()
/// batch["config.json"] = .path("/path/to/config.json")
/// batch["model.safetensors"] = .url(
///     URL(fileURLWithPath: "/path/to/model.safetensors"),
///     mimeType: "application/octet-stream"
/// )
/// let _ = try await client.uploadFiles(batch, to: "username/my-repo", message: "Initial commit")
/// ```
/// - SeeAlso: `HubClient.uploadFiles(_:to:kind:branch:message:maxConcurrent:)`
public struct FileBatch: Hashable, Codable, Sendable {
    /// An entry representing a file to upload.
    public struct Entry: Hashable, Codable, Sendable {
        /// The file URL pointing to the local file to upload.
        public var url: URL

        /// The MIME type of the file.
        public var mimeType: String?

        private init(url: URL, mimeType: String? = nil) {
            self.url = url
            self.mimeType = mimeType
        }

        /// Creates a file entry from a file system path.
        /// - Parameters:
        ///   - path: The file system path to the local file.
        ///   - mimeType: The MIME type of the file. If not provided, the MIME type is inferred from the file extension.
        /// - Returns: A file entry for the specified path.
        public static func path(_ path: String, mimeType: String? = nil) -> Self {
            return Self(url: URL(fileURLWithPath: path), mimeType: mimeType)
        }

        /// Creates a file entry from a URL.
        /// - Parameters:
        ///   - url: The file URL. Must be a file URL (e.g., `file:///path/to/file`), not a remote URL.
        ///   - mimeType: Optional MIME type for the file.
        /// - Returns: A file entry, or `nil` if the URL is not a file URL.
        /// - Note: Only file URLs are accepted because this API requires local file access for upload.
        ///         Remote URLs (http, https, etc.) are not supported and will return `nil`.
        public static func url(_ url: URL, mimeType: String? = nil) -> Self? {
            guard url.isFileURL else {
                return nil
            }
            return Self(url: url, mimeType: mimeType)
        }
    }

    private var entries: [String: Entry]

    /// Creates an empty file batch.
    public init() {
        entries = [:]
    }

    /// Creates a file batch with the specified entries.
    /// - Parameter entries: A dictionary mapping repository paths to file entries.
    public init(_ entries: [String: Entry]) {
        self.entries = entries
    }

    /// Accesses the file entry for the specified repository path.
    /// - Parameter path: The path in the repository where the file will be uploaded.
    /// - Returns: The file entry for the specified path, or `nil` if no entry exists.
    public subscript(path: String) -> Entry? {
        get {
            return entries[path]
        }
        set {
            entries[path] = newValue
        }
    }
}

// MARK: - Collection

extension FileBatch: Swift.Collection {
    public typealias Index = Dictionary<String, Entry>.Index

    public var startIndex: Index { entries.startIndex }
    public var endIndex: Index { entries.endIndex }
    public func index(after i: Index) -> Index { entries.index(after: i) }
    public subscript(position: Index) -> (key: String, value: Entry) { entries[position] }

    public func makeIterator() -> Dictionary<String, Entry>.Iterator {
        return entries.makeIterator()
    }
}

// MARK: - ExpressibleByDictionaryLiteral

extension FileBatch: ExpressibleByDictionaryLiteral {
    public init(dictionaryLiteral elements: (String, Entry)...) {
        self.init(Dictionary(uniqueKeysWithValues: elements))
    }
}
