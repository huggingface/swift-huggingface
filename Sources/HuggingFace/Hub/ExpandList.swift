import Foundation

/// A list of fields to request with the Hub `expand` query parameter.
public struct ExpandList: Hashable, Sendable {
    private var storage: Set<String>

    /// The field names included in the list.
    public var fields: [String] {
        storage.sorted()
    }

    /// Creates an empty expand list.
    public init() {
        self.storage = []
    }

    /// Creates an expand list from a comma-separated string.
    public init(_ csv: String) {
        self.storage = Set(csv.split(separator: ",").compactMap(Self.normalize))
    }

    /// Creates an expand list from a field array.
    public init(_ fields: [String]) {
        self.storage = Set(fields.compactMap(Self.normalize))
    }

    private static func normalize<S: StringProtocol>(_ field: S) -> String? {
        let trimmed = field.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }
}

// MARK: - CustomStringConvertible

extension ExpandList: CustomStringConvertible {
    public var description: String {
        fields.joined(separator: ",")
    }
}

// MARK

// MARK: - SetAlgebra

extension ExpandList: SetAlgebra {
    public func contains(_ member: String) -> Bool {
        guard let normalized = Self.normalize(member) else {
            return false
        }
        return storage.contains(normalized)
    }

    @discardableResult
    public mutating func insert(_ newMember: String) -> (inserted: Bool, memberAfterInsert: String) {
        guard let normalized = Self.normalize(newMember) else {
            return (false, newMember)
        }
        let (inserted, memberAfterInsert) = storage.insert(normalized)
        return (inserted, memberAfterInsert)
    }

    @discardableResult
    public mutating func update(with newMember: String) -> String? {
        guard let normalized = Self.normalize(newMember) else {
            return nil
        }
        return storage.update(with: normalized)
    }

    @discardableResult
    public mutating func remove(_ member: String) -> String? {
        guard let normalized = Self.normalize(member) else {
            return nil
        }
        return storage.remove(normalized)
    }

    public func union(_ other: ExpandList) -> ExpandList {
        ExpandList(Array(storage.union(other.storage)))
    }

    public func intersection(_ other: ExpandList) -> ExpandList {
        ExpandList(Array(storage.intersection(other.storage)))
    }

    public func symmetricDifference(_ other: ExpandList) -> ExpandList {
        ExpandList(Array(storage.symmetricDifference(other.storage)))
    }

    public mutating func formUnion(_ other: ExpandList) {
        storage.formUnion(other.storage)
    }

    public mutating func formIntersection(_ other: ExpandList) {
        storage.formIntersection(other.storage)
    }

    public mutating func formSymmetricDifference(_ other: ExpandList) {
        storage.formSymmetricDifference(other.storage)
    }
}

// MARK: - ExpressibleByArrayLiteral

extension ExpandList: ExpressibleByArrayLiteral {
    public init(arrayLiteral elements: String...) {
        self.init(elements)
    }
}

// MARK: - ExpressibleByStringLiteral

extension ExpandList: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) {
        self.init(value)
    }
}
