import Foundation

/// Tag information grouped by type for Hub repositories.
public struct Tags: Sendable {
    private let storage: [String: [Entry]]

    /// Creates a tags collection from a dictionary.
    public init(_ dictionary: [String: [Entry]]) {
        self.storage = dictionary
    }

    /// Information about a single tag.
    public struct Entry: Identifiable, Sendable, Codable {
        /// The tag identifier.
        public let id: String

        /// The tag label.
        public let label: String

        /// The tag type (e.g., "library", "license", "pipeline_tag").
        public let type: String
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
