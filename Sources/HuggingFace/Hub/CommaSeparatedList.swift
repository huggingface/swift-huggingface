import Foundation

/// A set-backed list encoded as comma-separated query parameter values.
public struct CommaSeparatedList<Value: Hashable & Sendable>: Hashable, Sendable {
    private var storage: Set<Value>

    /// The values included in the list.
    public var values: [Value] {
        Array(storage)
    }

    /// Creates an empty list.
    public init() {
        self.storage = []
    }

    /// Creates a list from an array of values.
    public init(_ values: [Value]) {
        self.storage = Set(values)
    }
}

// MARK: - SetAlgebra

extension CommaSeparatedList: SetAlgebra {
    public func contains(_ member: Value) -> Bool {
        storage.contains(member)
    }

    @discardableResult
    public mutating func insert(_ newMember: Value) -> (inserted: Bool, memberAfterInsert: Value) {
        storage.insert(newMember)
    }

    @discardableResult
    public mutating func update(with newMember: Value) -> Value? {
        storage.update(with: newMember)
    }

    @discardableResult
    public mutating func remove(_ member: Value) -> Value? {
        storage.remove(member)
    }

    public func union(_ other: CommaSeparatedList<Value>) -> CommaSeparatedList<Value> {
        CommaSeparatedList<Value>(Array(storage.union(other.storage)))
    }

    public func intersection(_ other: CommaSeparatedList<Value>) -> CommaSeparatedList<Value> {
        CommaSeparatedList<Value>(Array(storage.intersection(other.storage)))
    }

    public func symmetricDifference(_ other: CommaSeparatedList<Value>) -> CommaSeparatedList<Value> {
        CommaSeparatedList<Value>(Array(storage.symmetricDifference(other.storage)))
    }

    public mutating func formUnion(_ other: CommaSeparatedList<Value>) {
        storage.formUnion(other.storage)
    }

    public mutating func formIntersection(_ other: CommaSeparatedList<Value>) {
        storage.formIntersection(other.storage)
    }

    public mutating func formSymmetricDifference(_ other: CommaSeparatedList<Value>) {
        storage.formSymmetricDifference(other.storage)
    }

    private static func normalize<S: StringProtocol>(_ value: S) -> String? {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }
}

// MARK: - ExpressibleByArrayLiteral

extension CommaSeparatedList: ExpressibleByArrayLiteral {
    public init(arrayLiteral elements: Value...) {
        self.init(elements)
    }
}

// MARK: - String Values

extension CommaSeparatedList where Value == String {
    /// Creates a list from a comma-separated string.
    public init(_ csv: String) {
        self.init(csv.split(separator: ",").compactMap(Self.normalize))
    }

    /// Sorted string values in the list.
    public var fields: [String] {
        storage.sorted()
    }

    /// Comma-separated representation.
    public var csvValue: String {
        fields.joined(separator: ",")
    }
}

extension CommaSeparatedList: RawRepresentable where Value == String {
    public init(rawValue: String) {
        self.init(rawValue)
    }

    public var rawValue: String {
        csvValue
    }
}

extension CommaSeparatedList: ExpressibleByStringLiteral where Value == String {
    public init(stringLiteral value: String) {
        self.init(value)
    }
}

extension CommaSeparatedList: ExpressibleByExtendedGraphemeClusterLiteral where Value == String {
    public init(extendedGraphemeClusterLiteral value: String) {
        self.init(value)
    }
}

extension CommaSeparatedList: ExpressibleByUnicodeScalarLiteral where Value == String {
    public init(unicodeScalarLiteral value: String) {
        self.init(value)
    }
}

// MARK: - RawRepresentable String-backed Values

extension CommaSeparatedList where Value: RawRepresentable, Value.RawValue == String {
    /// Creates a list from a comma-separated string.
    public init(csv: String) {
        self.init(
            csv.split(separator: ",")
                .compactMap(Self.normalize)
                .compactMap(Value.init(rawValue:))
        )
    }

    /// Comma-separated representation.
    public var csvValue: String {
        storage.map(\.rawValue).sorted().joined(separator: ",")
    }
}

extension CommaSeparatedList where Value: CaseIterable & RawRepresentable, Value.RawValue == String {
    /// A list containing all known enum values.
    public static var all: Self {
        Self(Array(Value.allCases))
    }
}
