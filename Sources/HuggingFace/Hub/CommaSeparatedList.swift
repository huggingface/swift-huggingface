import Foundation

/// A string-backed value that can be one of the known values or an unknown custom value.
public enum Extensible<Value: Hashable & Sendable>: Hashable, Sendable {
    /// A known value.
    case known(Value)

    /// An unknown custom value.
    case custom(String)
}

extension Extensible: RawRepresentable where Value: RawRepresentable, Value.RawValue == String {
    public init?(rawValue: String) {
        if let value = Value(rawValue: rawValue) {
            self = .known(value)
        } else {
            self = .custom(rawValue)
        }
    }

    public var rawValue: String {
        switch self {
        case let .known(value):
            value.rawValue
        case let .custom(value):
            value
        }
    }
}

extension Extensible: CaseIterable where Value: CaseIterable & RawRepresentable, Value.RawValue == String {
    public static var allCases: [Self] {
        Value.allCases.map(Self.known)
    }
}

/// A comma-separated list of extensible values.
public typealias ExtensibleCommaSeparatedList<Value> = CommaSeparatedList<Extensible<Value>>
where Value: RawRepresentable & Hashable & Sendable, Value.RawValue == String

// MARK: -

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

    private static func parse<S: StringProtocol>(_ rawValue: S) -> [String] {
        rawValue
            .split(separator: ",")
            .compactMap { token in
                let trimmed = token.trimmingCharacters(in: .whitespacesAndNewlines)
                return trimmed.isEmpty ? nil : trimmed
            }
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
    public init(_ rawValue: String) {
        self.init(Self.parse(rawValue))
    }

    /// Sorted string values in the list.
    public var fields: [String] {
        storage.sorted()
    }
}

extension CommaSeparatedList: RawRepresentable where Value == String {
    public init(rawValue: String) {
        self.init(Self.parse(rawValue))
    }

    public var rawValue: String {
        fields.joined(separator: ",")
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
    public init(rawValue: String) {
        self.init(
            Self.parse(rawValue)
                .compactMap(Value.init(rawValue:))
        )
    }

    /// Raw comma-separated representation.
    public var rawValue: String {
        storage.map(\.rawValue).sorted().joined(separator: ",")
    }
}

// MARK: - RawRepresentable CaseIterable Values

extension CommaSeparatedList where Value: CaseIterable & RawRepresentable, Value.RawValue == String {
    /// A list containing all known enum values.
    public static var all: Self {
        Self(Array(Value.allCases))
    }
}
