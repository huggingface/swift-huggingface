import Foundation
#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

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

    /// The request URL that produced this page, if available.
    ///
    /// This is used internally to resolve relative pagination links and to preserve
    /// query parameters when the server's `next` URL omits them.
    public let requestURL: URL?

    /// Creates a paginated response.
    ///
    /// - Parameters:
    ///   - items: The items in the current page.
    ///   - nextURL: The URL for the next page, if available.
    ///   - requestURL: The request URL that produced this page, if available.
    public init(items: [T], nextURL: URL?, requestURL: URL? = nil) {
        self.items = items
        self.nextURL = nextURL
        self.requestURL = requestURL
    }
}

/// An async sequence of paginated responses.
///
/// `Pages` yields one ``PaginatedResponse`` at a time and fetches subsequent pages lazily.
/// The next page is requested only when iteration advances past the current page.
/// If iteration stops early, no additional page requests are performed.
///
/// Use this type with `for try await` to process page-by-page results while retaining
/// explicit control over when to stop pagination.
public struct Pages<T: Decodable & Sendable>: AsyncSequence, Sendable {
    public typealias Element = PaginatedResponse<T>

    private let firstPage: PaginatedResponse<T>
    private let fetchNext: @Sendable (PaginatedResponse<T>) async throws -> PaginatedResponse<T>?

    /// Creates a lazy page sequence from an initial page and next-page fetcher.
    ///
    /// - Parameters:
    ///   - firstPage: The first page yielded by the sequence.
    ///   - fetchNext: A closure that fetches the page after the provided page.
    ///                Return `nil` when no additional pages are available.
    public init(
        firstPage: PaginatedResponse<T>,
        fetchNext: @Sendable @escaping (PaginatedResponse<T>) async throws -> PaginatedResponse<T>?
    ) {
        self.firstPage = firstPage
        self.fetchNext = fetchNext
    }

    public func makeAsyncIterator() -> Iterator {
        Iterator(current: firstPage, fetchNext: fetchNext)
    }

    public struct Iterator: AsyncIteratorProtocol {
        private var current: PaginatedResponse<T>?
        private let fetchNext: @Sendable (PaginatedResponse<T>) async throws -> PaginatedResponse<T>?
        private var hasYieldedFirstPage = false

        fileprivate init(
            current: PaginatedResponse<T>?,
            fetchNext: @Sendable @escaping (PaginatedResponse<T>) async throws -> PaginatedResponse<T>?
        ) {
            self.current = current
            self.fetchNext = fetchNext
        }

        public mutating func next() async throws -> PaginatedResponse<T>? {
            if !hasYieldedFirstPage {
                hasYieldedFirstPage = true
                return current
            }

            guard let current else {
                return nil
            }

            self.current = try await fetchNext(current)
            return self.current
        }
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

/// Resolves a server-provided pagination URL against the current request context.
///
/// Some endpoints can return `next` URLs that omit original query filters.
/// This helper resolves relative URLs and preserves missing query parameters from
/// the current request URL.
///
/// - Parameters:
///   - nextURL: The `next` URL parsed from the Link header.
///   - requestURL: The request URL for the current page.
/// - Returns: A resolved next-page URL with missing query parameters preserved.
func resolveNextPageURL(_ nextURL: URL, requestURL: URL?) -> URL {
    var resolvedNextURL = nextURL

    if let requestURL,
        let nextComponents = URLComponents(url: nextURL, resolvingAgainstBaseURL: true),
        nextComponents.host == nil
    {
        resolvedNextURL = URL(string: nextURL.relativeString, relativeTo: requestURL)?.absoluteURL ?? nextURL
    }

    guard
        let requestURL,
        var nextComponents = URLComponents(url: resolvedNextURL, resolvingAgainstBaseURL: true),
        let requestComponents = URLComponents(url: requestURL, resolvingAgainstBaseURL: true)
    else {
        return resolvedNextURL
    }

    var nextQueryItems = nextComponents.queryItems ?? []
    let existingNames = Set(nextQueryItems.map(\.name))

    for requestItem in requestComponents.queryItems ?? [] where !existingNames.contains(requestItem.name) {
        nextQueryItems.append(requestItem)
    }

    nextComponents.queryItems = nextQueryItems.isEmpty ? nil : nextQueryItems
    return nextComponents.url ?? resolvedNextURL
}
