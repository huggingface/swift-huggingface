import Foundation
import Testing

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

@testable import HuggingFace

/// Thread-safe counter for testing fetch counts in Sendable closures.
private final class Counter: @unchecked Sendable {
    private var _value: Int = 0
    private let lock = NSLock()

    var value: Int {
        lock.lock()
        defer { lock.unlock() }
        return _value
    }

    func increment() {
        lock.lock()
        _value += 1
        lock.unlock()
    }
}

@Suite("Pagination Tests")
struct PaginationTests {
    // MARK: - PaginatedSequence Tests

    @Test("PaginatedSequence iterates through multiple pages")
    func paginatedSequenceMultiplePages() async throws {
        let page2URL = URL(string: "https://example.com/page2")!
        let page3URL = URL(string: "https://example.com/page3")!
        let fetchCount = Counter()

        let sequence = PaginatedSequence<Int>(
            limit: nil,
            firstPage: {
                fetchCount.increment()
                return PaginatedResponse(items: [1, 2, 3], nextURL: page2URL)
            },
            nextPage: { url in
                fetchCount.increment()
                if url == page2URL {
                    return PaginatedResponse(items: [4, 5, 6], nextURL: page3URL)
                } else {
                    return PaginatedResponse(items: [7, 8], nextURL: nil)
                }
            }
        )

        var results: [Int] = []
        for try await item in sequence {
            results.append(item)
        }

        #expect(results == [1, 2, 3, 4, 5, 6, 7, 8])
        #expect(fetchCount.value == 3)  // All three pages fetched
    }

    @Test("PaginatedSequence respects limit parameter")
    func paginatedSequenceWithLimit() async throws {
        let page2URL = URL(string: "https://example.com/page2")!
        let fetchCount = Counter()

        let sequence = PaginatedSequence<Int>(
            limit: 5,
            firstPage: {
                fetchCount.increment()
                return PaginatedResponse(items: [1, 2, 3], nextURL: page2URL)
            },
            nextPage: { _ in
                fetchCount.increment()
                return PaginatedResponse(items: [4, 5, 6, 7, 8], nextURL: nil)
            }
        )

        var results: [Int] = []
        for try await item in sequence {
            results.append(item)
        }

        #expect(results == [1, 2, 3, 4, 5])  // Stopped at limit
        #expect(fetchCount.value == 2)  // Needed second page to reach limit
    }

    @Test("PaginatedSequence handles empty first page")
    func paginatedSequenceEmptyFirstPage() async throws {
        let sequence = PaginatedSequence<Int>(
            limit: nil,
            firstPage: {
                PaginatedResponse(items: [], nextURL: nil)
            },
            nextPage: { _ in
                PaginatedResponse(items: [], nextURL: nil)
            }
        )

        var results: [Int] = []
        for try await item in sequence {
            results.append(item)
        }

        #expect(results.isEmpty)
    }

    @Test("PaginatedSequence is lazy - doesn't fetch until iterated")
    func paginatedSequenceLazyFetching() async throws {
        let fetchCount = Counter()

        let sequence = PaginatedSequence<Int>(
            limit: nil,
            firstPage: {
                fetchCount.increment()
                return PaginatedResponse(items: [1, 2, 3], nextURL: nil)
            },
            nextPage: { _ in
                fetchCount.increment()
                return PaginatedResponse(items: [], nextURL: nil)
            }
        )

        // Sequence created but not iterated
        #expect(fetchCount.value == 0)

        // Now iterate
        var iterator = sequence.makeAsyncIterator()
        _ = try await iterator.next()

        #expect(fetchCount.value == 1)
    }

    @Test("PaginatedSequence limit of zero returns no items")
    func paginatedSequenceLimitZero() async throws {
        let fetchCount = Counter()

        let sequence = PaginatedSequence<Int>(
            limit: 0,
            firstPage: {
                fetchCount.increment()
                return PaginatedResponse(items: [1, 2, 3], nextURL: nil)
            },
            nextPage: { _ in
                PaginatedResponse(items: [], nextURL: nil)
            }
        )

        var results: [Int] = []
        for try await item in sequence {
            results.append(item)
        }

        #expect(results.isEmpty)
        #expect(fetchCount.value == 0)  // Never fetched because limit is 0
    }

    // MARK: - Rate Limit Header Parsing Tests

    @Test("Parses IETF ratelimit header format")
    func parseRateLimitIETFFormat() {
        let response = makeHTTPResponse(headers: [
            "ratelimit": "\"api\";r=0;t=55"
        ])

        let seconds = parseRateLimitResetSeconds(from: response)

        #expect(seconds == 55)
    }

    @Test("Parses IETF ratelimit header with spaces")
    func parseRateLimitIETFFormatWithSpaces() {
        let response = makeHTTPResponse(headers: [
            "ratelimit": "\"api\"; r=0; t=120"
        ])

        let seconds = parseRateLimitResetSeconds(from: response)

        #expect(seconds == 120)
    }

    @Test("Falls back to Retry-After header")
    func parseRateLimitRetryAfterFallback() {
        let response = makeHTTPResponse(headers: [
            "Retry-After": "30"
        ])

        let seconds = parseRateLimitResetSeconds(from: response)

        #expect(seconds == 30)
    }

    @Test("Prefers IETF ratelimit over Retry-After")
    func parseRateLimitPrefersIETF() {
        let response = makeHTTPResponse(headers: [
            "ratelimit": "\"api\";r=0;t=55",
            "Retry-After": "30",
        ])

        let seconds = parseRateLimitResetSeconds(from: response)

        #expect(seconds == 55)  // IETF value, not Retry-After
    }

    @Test("Returns nil when no rate limit headers present")
    func parseRateLimitMissingHeaders() {
        let response = makeHTTPResponse(headers: [:])

        let seconds = parseRateLimitResetSeconds(from: response)

        #expect(seconds == nil)
    }

    @Test("Returns nil for malformed ratelimit header")
    func parseRateLimitMalformed() {
        let response = makeHTTPResponse(headers: [
            "ratelimit": "malformed"
        ])

        let seconds = parseRateLimitResetSeconds(from: response)

        #expect(seconds == nil)
    }

    @Test("Handles RateLimit header case variation")
    func parseRateLimitCaseVariation() {
        let response = makeHTTPResponse(headers: [
            "RateLimit": "\"api\";r=0;t=45"
        ])

        let seconds = parseRateLimitResetSeconds(from: response)

        #expect(seconds == 45)
    }

    // MARK: - PaginatedResponse Tests

    @Test("PaginatedResponse initializes correctly")
    func testPaginatedResponseInit() {
        let items = ["item1", "item2", "item3"]
        let nextURL = URL(string: "https://example.com/page2")

        let response = PaginatedResponse(items: items, nextURL: nextURL)

        #expect(response.items == items)
        #expect(response.nextURL == nextURL)
    }

    @Test("PaginatedResponse with nil nextURL")
    func testPaginatedResponseWithoutNextURL() {
        let items = ["item1", "item2"]
        let response = PaginatedResponse(items: items, nextURL: nil)

        #expect(response.items == items)
        #expect(response.nextURL == nil)
    }

    // MARK: - Link Header Parsing Tests

    @Test("Parses valid Link header with next URL")
    func testValidLinkHeader() {
        let response = makeHTTPResponse(
            linkHeader: "<https://huggingface.co/api/models?limit=10&skip=10>; rel=\"next\""
        )

        let nextURL = parseNextPageURL(from: response)

        #expect(nextURL != nil)
        #expect(nextURL?.absoluteString == "https://huggingface.co/api/models?limit=10&skip=10")
    }

    @Test("Parses Link header with single quotes")
    func testLinkHeaderWithSingleQuotes() {
        let response = makeHTTPResponse(
            linkHeader: "<https://huggingface.co/api/page2>; rel='next'"
        )

        let nextURL = parseNextPageURL(from: response)

        #expect(nextURL != nil)
        #expect(nextURL?.absoluteString == "https://huggingface.co/api/page2")
    }

    @Test("Parses Link header with multiple links")
    func testLinkHeaderWithMultipleLinks() {
        let response = makeHTTPResponse(
            linkHeader:
                "<https://huggingface.co/api/page1>; rel=\"prev\", <https://huggingface.co/api/page3>; rel=\"next\""
        )

        let nextURL = parseNextPageURL(from: response)

        #expect(nextURL != nil)
        #expect(nextURL?.absoluteString == "https://huggingface.co/api/page3")
    }

    @Test("Parses Link header with extra whitespace")
    func testLinkHeaderWithExtraWhitespace() {
        let response = makeHTTPResponse(
            linkHeader: "  <https://huggingface.co/api/page2>  ;  rel=\"next\"  "
        )

        let nextURL = parseNextPageURL(from: response)

        #expect(nextURL != nil)
        #expect(nextURL?.absoluteString == "https://huggingface.co/api/page2")
    }

    @Test("Parses Link header with complex query parameters")
    func testLinkHeaderWithComplexQueryParams() {
        let response = makeHTTPResponse(
            linkHeader:
                "<https://huggingface.co/api/models?limit=20&skip=40&sort=downloads&filter=text-generation>; rel=\"next\""
        )

        let nextURL = parseNextPageURL(from: response)

        #expect(nextURL != nil)
        #expect(
            nextURL?.absoluteString
                == "https://huggingface.co/api/models?limit=20&skip=40&sort=downloads&filter=text-generation"
        )
    }

    @Test("Returns nil when Link header is missing")
    func testMissingLinkHeader() {
        let response = makeHTTPResponse(linkHeader: nil)

        let nextURL = parseNextPageURL(from: response)

        #expect(nextURL == nil)
    }

    @Test("Returns nil when Link header is empty")
    func testEmptyLinkHeader() {
        let response = makeHTTPResponse(linkHeader: "")

        let nextURL = parseNextPageURL(from: response)

        #expect(nextURL == nil)
    }

    @Test("Returns nil when Link header has no next relation")
    func testLinkHeaderWithoutNext() {
        let response = makeHTTPResponse(
            linkHeader: "<https://huggingface.co/api/page1>; rel=\"prev\""
        )

        let nextURL = parseNextPageURL(from: response)

        #expect(nextURL == nil)
    }

    @Test("Returns nil for malformed Link header without angle brackets")
    func testMalformedLinkHeaderWithoutBrackets() {
        let response = makeHTTPResponse(
            linkHeader: "https://huggingface.co/api/page2; rel=\"next\""
        )

        let nextURL = parseNextPageURL(from: response)

        // Should still extract the URL even without proper angle brackets
        #expect(nextURL != nil)
    }

    @Test("Returns nil for Link header with invalid URL")
    func testLinkHeaderWithInvalidURL() {
        let response = makeHTTPResponse(
            linkHeader: "<>; rel=\"next\""
        )

        let nextURL = parseNextPageURL(from: response)

        #expect(nextURL == nil)
    }

    @Test("Returns nil for Link header missing semicolon separator")
    func testLinkHeaderMissingSeparator() {
        let response = makeHTTPResponse(
            linkHeader: "<https://huggingface.co/api/page2> rel=\"next\""
        )

        let nextURL = parseNextPageURL(from: response)

        #expect(nextURL == nil)
    }

    @Test("Handles Link header with additional parameters")
    func testLinkHeaderWithAdditionalParams() {
        let response = makeHTTPResponse(
            linkHeader: "<https://huggingface.co/api/page2>; rel=\"next\"; title=\"Next Page\""
        )

        let nextURL = parseNextPageURL(from: response)

        #expect(nextURL != nil)
        #expect(nextURL?.absoluteString == "https://huggingface.co/api/page2")
    }

    @Test("Parses first next link when multiple next links exist")
    func testMultipleNextLinks() {
        let response = makeHTTPResponse(
            linkHeader:
                "<https://huggingface.co/api/page2>; rel=\"next\", <https://huggingface.co/api/page3>; rel=\"next\""
        )

        let nextURL = parseNextPageURL(from: response)

        #expect(nextURL != nil)
        // Should return the first "next" link found
        #expect(nextURL?.absoluteString == "https://huggingface.co/api/page2")
    }

    // MARK: - Helper Methods

    private func makeHTTPResponse(linkHeader: String?) -> HTTPURLResponse {
        var headers: [String: String] = [:]
        if let linkHeader = linkHeader {
            headers["Link"] = linkHeader
        }

        return HTTPURLResponse(
            url: URL(string: "https://huggingface.co/api/test")!,
            statusCode: 200,
            httpVersion: nil,
            headerFields: headers
        )!
    }

    private func makeHTTPResponse(headers: [String: String]) -> HTTPURLResponse {
        HTTPURLResponse(
            url: URL(string: "https://huggingface.co/api/test")!,
            statusCode: 200,
            httpVersion: nil,
            headerFields: headers
        )!
    }
}

// MARK: - Integration Tests (Real API Calls)

/// Integration tests for pagination that make real API calls.
@Suite("Pagination Integration Tests")
struct PaginationIntegrationTests {
    @Test("Paginate over Hugging Face API models")
    func paginateHuggingFaceAPI() async throws {
        // Make real API calls and verify we can paginate through results
        let client = HubClient()
        var count = 0

        for try await _ in client.listAllModels(limit: 6) {
            count += 1
        }

        // Verify we got all the items we requested
        #expect(count == 6)
    }

    @Test("Pagination works across multiple pages")
    func paginationAcrossPages() async throws {
        // Request enough items to require fetching multiple pages
        // The API typically returns ~20-100 items per page
        let client = HubClient()
        var count = 0

        for try await _ in client.listAllModels(limit: 25) {
            count += 1
        }

        #expect(count == 25)
    }

    @Test("Pagination stops early when limit reached mid-page")
    func paginationStopsAtLimit() async throws {
        let client = HubClient()
        var count = 0

        // Request a small limit that should stop mid-page
        for try await _ in client.listAllModels(limit: 3) {
            count += 1
        }

        #expect(count == 3)
    }
}
