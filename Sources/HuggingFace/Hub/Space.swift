import Foundation

/// Information about a Space on the Hub.
public struct Space: Identifiable, Codable, Sendable {
    /// The space's identifier (e.g., "user/space-name").
    public let id: Repo.ID

    /// The author of the space.
    public let author: String?

    /// The SHA hash of the space's latest commit.
    public let sha: String?

    /// The date the space was last modified.
    public let lastModified: Date?

    /// The visibility of the space.
    public let visibility: Repo.Visibility?

    /// Whether the space is gated.
    public let gated: GatedMode?

    /// Whether the space is disabled.
    public let isDisabled: Bool?

    /// The number of likes.
    public let likes: Int?

    /// The SDK used by the space (e.g., "gradio", "streamlit").
    public let sdk: String?

    /// The tags associated with the space.
    public let tags: [String]?

    /// The date the space was created.
    public let createdAt: Date?

    /// The runtime information.
    public let runtime: Runtime?

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

    /// Space log entry from streaming logs.
    public struct LogEntry: Codable, Sendable {
        /// The log message.
        public let message: String

        /// The timestamp of the log entry.
        public let timestamp: Date?

        /// The log level.
        public let level: String?

        /// Additional metadata.
        public let metadata: [String: String]?
    }

    /// Space metrics from streaming metrics.
    public struct Metrics: Codable, Sendable {
        /// The timestamp of the metrics.
        public let timestamp: Date

        /// CPU usage percentage.
        public let cpuUsage: Double?

        /// Memory usage in bytes.
        public let memoryUsage: Int?

        /// GPU usage percentage.
        public let gpuUsage: Double?

        /// GPU memory usage in bytes.
        public let gpuMemoryUsage: Int?

        /// Network I/O bytes.
        public let networkIO: Int?

        /// Disk I/O bytes.
        public let diskIO: Int?

        /// Additional custom metrics.
        public let customMetrics: [String: Double]?

        private enum CodingKeys: String, CodingKey {
            case timestamp
            case cpuUsage
            case memoryUsage
            case gpuUsage
            case gpuMemoryUsage
            case networkIO
            case diskIO
            case customMetrics
        }
    }

    /// Space event from streaming events.
    public struct Event: Codable, Sendable {
        /// The event type.
        public let type: String

        /// The timestamp of the event.
        public let timestamp: Date

        /// The event message.
        public let message: String?

        /// Additional event data.
        public let data: [String: String]?

        /// The session UUID if applicable.
        public let sessionUUID: String?

        private enum CodingKeys: String, CodingKey {
            case type
            case timestamp
            case message
            case data
            case sessionUUID = "sessionUuid"
        }
    }

    /// Runtime information for a Space.
    public struct Runtime: Codable, Sendable {
        /// The current stage of the space (e.g., "RUNNING", "STOPPED").
        public let stage: String

        /// The hardware tier (e.g., "cpu-basic", "t4-small").
        public let hardware: String?

        /// The requested hardware tier.
        public let requestedHardware: String?

        /// The storage tier.
        public let storageTier: String?

        /// Additional resources information.
        public let resources: [String: Value]?

        private enum CodingKeys: String, CodingKey {
            case stage
            case hardware
            case requestedHardware
            case storageTier
            case resources
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
        case likes
        case sdk
        case tags
        case createdAt
        case runtime
        case cardData
        case siblings
    }
}
