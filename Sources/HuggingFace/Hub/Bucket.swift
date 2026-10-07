#if HUGGINGFACE_ENABLE_XET

    import Foundation

    /// Information about a bucket on the Hub.
    public struct Bucket: Identifiable, Codable, Hashable, Sendable {
        public typealias ID = Repo.ID

        /// The bucket's identifier (`namespace/name`).
        public let id: Bucket.ID

        /// The bucket's visibility.
        public let visibility: Repo.Visibility?

        /// When the bucket was created.
        public let createdAt: Date?

        /// Total size of the bucket in bytes.
        public let size: Int64?

        /// Number of files in the bucket.
        public let totalFiles: Int?

        private enum CodingKeys: String, CodingKey {
            case id
            case visibility = "private"
            case createdAt
            case size
            case totalFiles
        }

        /// An entry in a bucket's tree listing — either a file or a folder.
        public enum TreeEntry: Sendable, Hashable, Decodable {
            case file(File)
            case folder(Folder)

            /// The path of this entry within the bucket.
            public var path: String {
                switch self {
                case .file(let f): return f.path
                case .folder(let f): return f.path
                }
            }

            private enum DiscriminatorKeys: String, CodingKey { case type }

            public init(from decoder: Decoder) throws {
                let kindContainer = try decoder.container(keyedBy: DiscriminatorKeys.self)
                let type = try kindContainer.decode(String.self, forKey: .type)
                let single = try decoder.singleValueContainer()
                switch type {
                case "file":
                    self = .file(try single.decode(File.self))
                case "directory":
                    self = .folder(try single.decode(Folder.self))
                default:
                    throw DecodingError.dataCorruptedError(
                        forKey: .type,
                        in: kindContainer,
                        debugDescription: "Unknown bucket tree entry type '\(type)' (expected 'file' or 'directory')"
                    )
                }
            }
        }

        /// A file in a bucket.
        public struct File: Codable, Hashable, Sendable {
            /// Discriminator: always `"file"`.
            public let type: String

            /// The file's path within the bucket.
            public let path: String

            /// File size in bytes.
            public let size: Int64

            /// Content-addressed Xet hash. Always present as buckets are Xet-only.
            public let xetHash: String

            /// Last-modified time, if known.
            public let mtime: Date?

            /// When the file was uploaded to the bucket, if known.
            public let uploadedAt: Date?

            private enum CodingKeys: String, CodingKey {
                case type
                case path
                case size
                case xetHash
                case mtime
                case uploadedAt
            }
        }

        /// A folder in a bucket.
        public struct Folder: Codable, Hashable, Sendable {
            /// Discriminator: always `"directory"`.
            public let type: String

            /// The folder's path within the bucket.
            public let path: String

            /// When the folder was last touched, if known.
            public let uploadedAt: Date?
        }

        /// A cloud region for bucket creation. Requires Team plan or above.
        ///
        /// Use ``us`` or ``eu``,
        /// or create a region from its identifier
        /// to use one this package doesn't list yet.
        public struct Region: RawRepresentable, Codable, Hashable, CaseIterable, Sendable {
            /// The region's identifier, such as `"us"`.
            public let rawValue: String

            /// Creates a region from its identifier.
            public init(rawValue: String) {
                self.rawValue = rawValue
            }

            /// The United States region.
            public static let us = Region(rawValue: "us")

            /// The European Union region.
            public static let eu = Region(rawValue: "eu")

            /// The regions this package lists.
            public static let allCases: [Region] = [.us, .eu]
        }
    }

#endif  // HUGGINGFACE_ENABLE_XET
