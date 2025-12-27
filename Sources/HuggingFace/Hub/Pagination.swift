import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

// MARK: - Paginated Sequence

/// An async sequence that automatically paginates through API results.
///
/// ```swift
/// for try await model in client.listAllModels() {
///     print(model.name)
/// }
/// ```
public struct PaginatedSequence<T: Decodable & Sendable>: AsyncSequence, Sendable {
    public typealias Element = T

    private let firstPageFetcher: @Sendable () async throws -> PaginatedResponse<T>
    private let nextPageFetcher: @Sendable (URL) async throws -> PaginatedResponse<T>
    private let limit: Int?

    init(
        limit: Int? = nil,
        firstPage: @escaping @Sendable () async throws -> PaginatedResponse<T>,
        nextPage: @escaping @Sendable (URL) async throws -> PaginatedResponse<T>
    ) {
        self.limit = limit
        firstPageFetcher = firstPage
        nextPageFetcher = nextPage
    }

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(
            limit: limit,
            firstPageFetcher: firstPageFetcher,
            nextPageFetcher: nextPageFetcher
        )
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        private let firstPageFetcher: @Sendable () async throws -> PaginatedResponse<T>
        private let nextPageFetcher: @Sendable (URL) async throws -> PaginatedResponse<T>
        private let limit: Int?
        private var currentItems: [T] = []
        private var currentIndex: Int = 0
        private var nextURL: URL?
        private var fetchedFirstPage = false
        private var totalYielded: Int = 0

        init(
            limit: Int?,
            firstPageFetcher: @escaping @Sendable () async throws -> PaginatedResponse<T>,
            nextPageFetcher: @escaping @Sendable (URL) async throws -> PaginatedResponse<T>
        ) {
            self.limit = limit
            self.firstPageFetcher = firstPageFetcher
            self.nextPageFetcher = nextPageFetcher
        }

        public mutating func next() async throws -> T? {
            // Check if we've hit the limit
            if let limit, totalYielded >= limit {
                return nil
            }

            // Return next item from current page if available
            if currentIndex < currentItems.count {
                let item = currentItems[currentIndex]
                currentIndex += 1
                totalYielded += 1
                return item
            }

            // Fetch next page
            let page: PaginatedResponse<T>
            if !fetchedFirstPage {
                fetchedFirstPage = true
                page = try await firstPageFetcher()
            } else if let url = nextURL {
                page = try await nextPageFetcher(url)
            } else {
                return nil
            }

            currentItems = page.items
            currentIndex = 0
            nextURL = page.nextURL

            // Return first item from new page
            guard currentIndex < currentItems.count else {
                return nil
            }

            let item = currentItems[currentIndex]
            currentIndex += 1
            totalYielded += 1
            return item
        }
    }
}

// MARK: - Sort Direction

/// Sort direction for list queries.
public enum SortDirection: Int, Hashable, Sendable {
    /// Ascending order.
    case ascending = 1

    /// Descending order.
    case descending = -1
}

/// A response that includes pagination information from Link headers.
public struct PaginatedResponse<T: Decodable & Sendable>: Sendable {
    /// The items in the current page.
    public let items: [T]

    /// The URL for the next page, if available.
    public let nextURL: URL?

    /// Creates a paginated response.
    ///
    /// - Parameters:
    ///   - items: The items in the current page.
    ///   - nextURL: The URL for the next page, if available.
    public init(items: [T], nextURL: URL?) {
        self.items = items
        self.nextURL = nextURL
    }
}

// MARK: - Link Header Parsing

/// Parses the Link header from an HTTP response to extract the next page URL.
///
/// The Link header format follows RFC 8288: `<url>; rel="next"`
///
/// - Parameter response: The HTTP response to parse.
/// - Returns: The URL for the next page, or `nil` if not found.
func parseNextPageURL(from response: HTTPURLResponse) -> URL? {
    guard let linkHeader = response.value(forHTTPHeaderField: "Link") else {
        return nil
    }
    return parseNextPageURL(from: linkHeader)
}

/// Parses a Link header string to extract the next page URL.
///
/// - Parameter linkHeader: The Link header value.
/// - Returns: The URL for the next page, or `nil` if not found.
func parseNextPageURL(from linkHeader: String) -> URL? {
    // Parse Link header format: <https://example.com/page2>; rel="next"
    let links = linkHeader.components(separatedBy: ",")
    for link in links {
        let components = link.components(separatedBy: ";")
        guard components.count >= 2 else { continue }

        let urlPart = components[0].trimmingCharacters(in: .whitespaces)
        let relPart = components[1].trimmingCharacters(in: .whitespaces)

        // Check if this is the "next" link
        if relPart.contains("rel=\"next\"") || relPart.contains("rel='next'") {
            // Extract URL from angle brackets
            let urlString = urlPart.trimmingCharacters(in: CharacterSet(charactersIn: "<>"))

            // Check for empty URL string to ensure consistent behavior across platforms
            guard !urlString.isEmpty, let url = URL(string: urlString) else {
                continue
            }

            return url
        }
    }

    return nil
}
