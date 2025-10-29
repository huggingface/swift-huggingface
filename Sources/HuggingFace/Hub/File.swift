import Foundation

/// Information about a file in a repository
public struct File: Hashable, Codable, Sendable {
    public let exists: Bool
    public let size: Int64?
    public let etag: String?
    public let revision: String?
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

public struct FileBatch: Hashable, Codable, Sendable {
    public struct Entry: Hashable, Codable, Sendable {
        public var url: URL
        public var mimeType: String?

        private init(url: URL, mimeType: String? = nil) {
            self.url = url
            self.mimeType = mimeType
        }

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

    public init() {
        self.entries = [:]
    }

    public init(_ entries: [String: Entry]) {
        self.entries = entries
    }

    public subscript(path: String) -> Entry? {
        get {
            return entries[path]
        }
        set {
            entries[path] = newValue
        }
    }
}

extension FileBatch: ExpressibleByDictionaryLiteral {
    public init(dictionaryLiteral elements: (String, Entry)...) {
        self.init(Dictionary(uniqueKeysWithValues: elements))
    }
}

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
