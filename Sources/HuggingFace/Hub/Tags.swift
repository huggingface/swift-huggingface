import Foundation

/// Tag information grouped by type for Hub repositories.
public struct Tags: Sendable {
    private let storage: [String: [Entry]]

    /// Creates a tags collection from a dictionary.
    public init(_ dictionary: [String: [Entry]]) {
        self.storage = dictionary
    }

    /// Information about a single tag.
    public struct Entry: Identifiable, Sendable {
        /// The tag identifier.
        public let id: String

        /// The tag label.
        public let label: String

        /// The number of repositories with this tag.
        public let count: Int?

    }
}

// MARK: - Codable

extension Tags: Codable {
    public init(from decoder: Decoder) throws {
        // The API returns tags directly as a dictionary without a wrapper
        let container = try decoder.singleValueContainer()
        self.storage = try container.decode([String: [Entry]].self)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(storage)
    }
}

extension Tags.Entry: Codable {
    private enum CodingKeys: String, CodingKey {
        case id
        case label
        case modelCount
        case datasetCount
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.id = try container.decode(String.self, forKey: .id)
        self.label = try container.decode(String.self, forKey: .label)
        if let modelCount = try container.decodeIfPresent(Int.self, forKey: .modelCount) {
            self.count = modelCount
        } else if let datasetCount = try container.decodeIfPresent(Int.self, forKey: .datasetCount) {
            self.count = datasetCount
        } else {
            self.count = nil
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(id, forKey: .id)
        try container.encode(label, forKey: .label)
        try container.encode(count, forKey: .modelCount)
        try container.encode(count, forKey: .datasetCount)
    }
}

// MARK: - Collection

extension Tags: Swift.Collection {
    public typealias Index = Dictionary<String, [Entry]>.Index

    public var startIndex: Index { storage.startIndex }
    public var endIndex: Index { storage.endIndex }
    public func index(after i: Index) -> Index { storage.index(after: i) }
    public subscript(position: Index) -> (key: String, value: [Entry]) { storage[position] }

    /// Access the tag list for a given type key.
    public subscript(_ key: String) -> [Entry]? { storage[key] }
}

// MARK: - ExpressibleByDictionaryLiteral

extension Tags: ExpressibleByDictionaryLiteral {
    public init(dictionaryLiteral elements: (String, [Entry])...) {
        self.init(Dictionary(uniqueKeysWithValues: elements))
    }
}
