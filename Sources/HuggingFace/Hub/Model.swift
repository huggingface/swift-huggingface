import Foundation

/// Information about a model on the Hub.
public struct Model: Identifiable, Codable, Sendable {
    /// The model's identifier (e.g., "facebook/bart-large-cnn").
    public let id: Repo.ID

    /// The author of the model.
    public let author: String?

    /// The SHA hash of the model's latest commit.
    public let sha: String?

    /// The date the model was last modified.
    public let lastModified: Date?

    /// The visibility of the model.
    public let visibility: Repo.Visibility?

    /// Whether the model is gated.
    public let gated: GatedMode?

    /// Whether the model is disabled.
    public let isDisabled: Bool?

    /// The number of downloads.
    public let downloads: Int?

    /// The number of likes.
    public let likes: Int?

    /// The library name (e.g., "transformers", "diffusers").
    public let library: String?

    /// The tags associated with the model.
    public let tags: [String]?

    /// The pipeline tag (e.g., "text-classification").
    public let pipelineTag: String?

    /// The date the model was created.
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
        case library = "library_name"
        case tags
        case pipelineTag = "pipeline_tag"
        case createdAt
        case cardData
        case siblings
    }
}

// MARK: -

/// The gated mode for a repository.
public enum GatedMode: Hashable, Sendable {
    /// The repository is not gated.
    case notGated

    /// The repository is gated with automatic approval.
    case auto

    /// The repository is gated with manual approval.
    case manual
}

// MARK: - Codable

extension GatedMode: Codable {
    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()

        if let boolValue = try? container.decode(Bool.self) {
            self = boolValue ? .auto : .notGated
        } else if let stringValue = try? container.decode(String.self) {
            switch stringValue {
            case "auto":
                self = .auto
            case "manual":
                self = .manual
            default:
                throw DecodingError.dataCorruptedError(
                    in: container,
                    debugDescription: "Invalid GatedMode value: \(stringValue)"
                )
            }
        } else {
            throw DecodingError.dataCorruptedError(
                in: container,
                debugDescription: "GatedMode must be Bool or String"
            )
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .notGated:
            try container.encode(false)
        case .auto:
            try container.encode("auto")
        case .manual:
            try container.encode("manual")
        }
    }
}
