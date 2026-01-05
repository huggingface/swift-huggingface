import Foundation

/// Information about a dataset on the Hub.
public struct Dataset: Identifiable, Codable, Sendable {
    /// The dataset's identifier (e.g., "squad").
    public let id: Repo.ID

    /// The author of the dataset.
    public let author: String?

    /// The SHA hash of the dataset's latest commit.
    public let sha: String?

    /// The date the dataset was last modified.
    public let lastModified: Date?

    /// The visibility of the dataset.
    public let visibility: Repo.Visibility?

    /// Whether the dataset is gated.
    public let gated: GatedMode?

    /// Whether the dataset is disabled.
    public let isDisabled: Bool?

    /// The number of downloads.
    public let downloads: Int?

    /// The number of likes.
    public let likes: Int?

    /// The tags associated with the dataset.
    public let tags: [String]?

    /// The date the dataset was created.
    public let createdAt: Date?

    /// The card data (README metadata).
    public let cardData: [String: Value]?

    /// The sibling files information.
    public let siblings: [SiblingInfo]?

    /// Information about a sibling file in the repository.
    public struct SiblingInfo: Codable, Sendable {
        /// The relative path of the file.
        public let relativeFilename: String
        /// The size of the file in bytes (available when blobs=true).
        public let size: Int?

        private enum CodingKeys: String, CodingKey {
            case relativeFilename = "rfilename"
            case size
        }
    }

    private enum CodingKeys: String, CodingKey {
        case id
        case author
        case sha
        case lastModified
        case visibility = "private"
        case gated
        case isDisabled = "disabled"
        case downloads
        case likes
        case tags
        case createdAt
        case cardData
        case siblings
    }
}

// MARK: -

/// Information about a Parquet file.
public struct ParquetFileInfo: Codable, Sendable {
    /// The dataset identifier.
    public let dataset: String

    /// The configuration/subset name.
    public let config: String

    /// The split name.
    public let split: String

    /// The download URL for the Parquet file.
    public let url: String

    /// The filename.
    public let filename: String

    /// The file size in bytes.
    public let size: Int?
}
