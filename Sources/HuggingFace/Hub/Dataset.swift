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

    /// Dataset description.
    public let description: String?

    /// Dataset citation.
    public let citation: String?

    /// Papers With Code identifier.
    public let paperswithcodeID: String?

    /// The all-time download count.
    public let downloadsAllTime: Int?

    /// The trending score.
    public let trendingScore: Int?

    /// The used storage in bytes.
    public let usedStorage: Int?

    /// The resource group metadata.
    public let resourceGroup: [String: Value]?

    /// The sibling files information.
    public let siblings: [SiblingInfo]?

    /// Information about a sibling file in the repository.
    public struct SiblingInfo: Codable, Sendable {
        /// The relative path of the file.
        public let relativeFilename: String

        /// The file size in bytes, when available.
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
        case description
        case citation
        case paperswithcodeID = "paperswithcode_id"
        case downloadsAllTime
        case trendingScore
        case usedStorage
        case resourceGroup
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

    private enum CodingKeys: String, CodingKey {
        case dataset
        case config
        case split
        case url
        case filename
        case size
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if let url = try? container.decode(String.self) {
            guard let urlObject = URL(string: url) else {
                throw DecodingError.dataCorruptedError(
                    in: container,
                    debugDescription: "Invalid URL string for ParquetFileInfo: \(url)"
                )
            }
            let components = urlObject.pathComponents
            guard let datasetsIndex = components.firstIndex(of: "datasets") else {
                throw DecodingError.dataCorruptedError(
                    in: container,
                    debugDescription:
                        "URL path for ParquetFileInfo does not contain 'datasets' segment: \(urlObject.path)"
                )
            }

            // Expected layout:
            // /api/datasets/{namespace}/{dataset}/parquet/{config}/{split}/{file}
            let datasetIndex = datasetsIndex + 2
            let parquetIndex = datasetsIndex + 3
            let configIndex = datasetsIndex + 4
            let splitIndex = datasetsIndex + 5
            let fileIndex = datasetsIndex + 6
            guard
                components.indices.contains(datasetIndex),
                components.indices.contains(parquetIndex),
                components.indices.contains(configIndex),
                components.indices.contains(splitIndex),
                components.indices.contains(fileIndex),
                components[parquetIndex] == "parquet"
            else {
                throw DecodingError.dataCorruptedError(
                    in: container,
                    debugDescription:
                        "URL path for ParquetFileInfo does not match expected layout '/api/datasets/{namespace}/{dataset}/parquet/{config}/{split}/{file}': \(urlObject.path)"
                )
            }

            self.url = url
            self.filename = components[fileIndex]
            self.size = nil
            self.dataset = components[datasetIndex]
            self.config = components[configIndex]
            self.split = components[splitIndex]
            return
        }

        let keyedContainer = try decoder.container(keyedBy: CodingKeys.self)
        self.dataset = try keyedContainer.decode(String.self, forKey: .dataset)
        self.config = try keyedContainer.decode(String.self, forKey: .config)
        self.split = try keyedContainer.decode(String.self, forKey: .split)
        self.url = try keyedContainer.decode(String.self, forKey: .url)
        self.filename = try keyedContainer.decode(String.self, forKey: .filename)
        self.size = try keyedContainer.decodeIfPresent(Int.self, forKey: .size)
    }
}
