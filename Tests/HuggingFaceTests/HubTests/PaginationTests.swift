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

#if swift(>=6.1)
    // MARK: - Retry Logic Tests

    /// Tests for HTTP retry logic in pagination with exponential backoff.
    @Suite("Retry Logic Tests", .serialized)
    struct RetryLogicTests {
        /// Helper to create a URL session with mock protocol handlers
        func createMockClient() -> HubClient {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [MockURLProtocol.self]
            let session = URLSession(configuration: configuration)
            return HubClient(
                session: session,
                host: URL(string: "https://huggingface.co")!,
                userAgent: "TestClient/1.0"
            )
        }

        // MARK: - First Page Behavior

        @Test(
            "First page does not retry on server errors",
            arguments: [429, 500, 502, 503, 504]
        )
        func firstPageNoRetry(statusCode: Int) async throws {
            nonisolated(unsafe) var requestCount = 0

            await MockURLProtocol.setHandler { request in
                requestCount += 1
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: statusCode,
                    httpVersion: "HTTP/1.1",
                    headerFields: [:]
                )!
                return (response, Data("{\"error\": \"Error\"}".utf8))
            }

            let client = createMockClient()

            await #expect(throws: HTTPClientError.self) {
                for try await _ in client.listAllModels() {}
            }

            // First page uses .none retry config - only 1 request, no retries
            #expect(requestCount == 1)
        }

        // MARK: - Subsequent Page Retry Behavior

        @Test(
            "Subsequent page retries on server errors and succeeds",
            arguments: [429, 500, 502, 503, 504]
        )
        func retryOnServerError(statusCode: Int) async throws {
            nonisolated(unsafe) var requestCount = 0

            await MockURLProtocol.setHandler { request in
                requestCount += 1

                if requestCount == 1 {
                    // First page succeeds with next link
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 200,
                        httpVersion: "HTTP/1.1",
                        headerFields: [
                            "Content-Type": "application/json",
                            "Link": "<https://huggingface.co/api/models?cursor=page2>; rel=\"next\"",
                        ]
                    )!
                    return (response, Data("[{\"id\": \"model/1\"}]".utf8))
                } else if requestCount == 2 {
                    // Second page fails with the test status code
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: statusCode,
                        httpVersion: "HTTP/1.1",
                        headerFields: ["Retry-After": "0"]
                    )!
                    return (response, Data())
                } else {
                    // Retry succeeds
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 200,
                        httpVersion: "HTTP/1.1",
                        headerFields: ["Content-Type": "application/json"]
                    )!
                    return (response, Data("[{\"id\": \"model/2\"}]".utf8))
                }
            }

            let client = createMockClient()
            var models: [Model] = []
            for try await model in client.listAllModels() {
                models.append(model)
            }

            #expect(models.count == 2)
            #expect(requestCount == 3)  // first page + error + retry success
        }

        @Test(
            "Does not retry on client errors",
            arguments: [400, 401, 403, 404, 422]
        )
        func noRetryOnClientError(statusCode: Int) async throws {
            nonisolated(unsafe) var requestCount = 0

            await MockURLProtocol.setHandler { request in
                requestCount += 1

                if requestCount == 1 {
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 200,
                        httpVersion: "HTTP/1.1",
                        headerFields: [
                            "Content-Type": "application/json",
                            "Link": "<https://huggingface.co/api/models?cursor=page2>; rel=\"next\"",
                        ]
                    )!
                    return (response, Data("[{\"id\": \"model/1\"}]".utf8))
                }

                // Client errors should not be retried
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: statusCode,
                    httpVersion: "HTTP/1.1",
                    headerFields: [:]
                )!
                return (response, Data("{\"error\": \"Client Error\"}".utf8))
            }

            let client = createMockClient()

            await #expect(throws: HTTPClientError.self) {
                for try await _ in client.listAllModels() {}
            }

            // No retry on client errors - just 2 requests total
            #expect(requestCount == 2)
        }

        // MARK: - Retry Exhaustion

        @Test("Exhausts all retries and throws error", .mockURLSession)
        func exhaustsRetriesAndThrows() async throws {
            nonisolated(unsafe) var requestCount = 0

            await MockURLProtocol.setHandler { request in
                requestCount += 1

                if requestCount == 1 {
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 200,
                        httpVersion: "HTTP/1.1",
                        headerFields: [
                            "Content-Type": "application/json",
                            "Link": "<https://huggingface.co/api/models?cursor=page2>; rel=\"next\"",
                        ]
                    )!
                    return (response, Data("[{\"id\": \"model/1\"}]".utf8))
                }

                // All subsequent requests fail
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 503,
                    httpVersion: "HTTP/1.1",
                    headerFields: [:]
                )!
                return (response, Data("{\"error\": \"Service Unavailable\"}".utf8))
            }

            let client = createMockClient()

            await #expect(throws: HTTPClientError.self) {
                for try await _ in client.listAllModels() {}
            }

            // RetryConfiguration.default has maxRetries=5
            // Loop runs for attempts 0...5 = 6 attempts for second page
            // Total: 1 (first page) + 6 (second page attempts) = 7
            #expect(requestCount == 7)
        }

        // MARK: - Rate Limit Header Behavior

        @Test("Uses RateLimit header wait time on 429", .mockURLSession)
        func usesRateLimitHeaderForWaitTime() async throws {
            nonisolated(unsafe) var requestCount = 0
            nonisolated(unsafe) var timestamps: [Date] = []

            await MockURLProtocol.setHandler { request in
                timestamps.append(Date())
                requestCount += 1

                if requestCount == 1 {
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 200,
                        httpVersion: "HTTP/1.1",
                        headerFields: [
                            "Content-Type": "application/json",
                            "Link": "<https://huggingface.co/api/models?cursor=page2>; rel=\"next\"",
                        ]
                    )!
                    return (response, Data("[{\"id\": \"model/1\"}]".utf8))
                } else if requestCount == 2 {
                    // 429 with RateLimit header saying wait 1 second
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 429,
                        httpVersion: "HTTP/1.1",
                        headerFields: ["RateLimit": "\"api\";r=0;t=1"]
                    )!
                    return (response, Data())
                } else {
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 200,
                        httpVersion: "HTTP/1.1",
                        headerFields: ["Content-Type": "application/json"]
                    )!
                    return (response, Data("[{\"id\": \"model/2\"}]".utf8))
                }
            }

            let client = createMockClient()
            var models: [Model] = []
            for try await model in client.listAllModels() {
                models.append(model)
            }

            #expect(models.count == 2)
            #expect(requestCount == 3)

            // Verify wait time: header says t=1, code adds +1 for safety = 2 seconds
            if timestamps.count >= 3 {
                let delay = timestamps[2].timeIntervalSince(timestamps[1])
                #expect(delay >= 1.5)
            }
        }

        // MARK: - Network Error Retry

        @Test("Retries on network errors", .mockURLSession)
        func retriesOnNetworkError() async throws {
            nonisolated(unsafe) var requestCount = 0

            await MockURLProtocol.setHandler { request in
                requestCount += 1

                if requestCount == 1 {
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 200,
                        httpVersion: "HTTP/1.1",
                        headerFields: [
                            "Content-Type": "application/json",
                            "Link": "<https://huggingface.co/api/models?cursor=page2>; rel=\"next\"",
                        ]
                    )!
                    return (response, Data("[{\"id\": \"model/1\"}]".utf8))
                } else if requestCount == 2 {
                    // Simulate network error
                    throw URLError(.notConnectedToInternet)
                } else {
                    // Retry succeeds
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 200,
                        httpVersion: "HTTP/1.1",
                        headerFields: ["Content-Type": "application/json"]
                    )!
                    return (response, Data("[{\"id\": \"model/2\"}]".utf8))
                }
            }

            let client = createMockClient()
            var models: [Model] = []
            for try await model in client.listAllModels() {
                models.append(model)
            }

            #expect(models.count == 2)
            #expect(requestCount == 3)  // first page + network error + retry success
        }

        // MARK: - Task Cancellation

        @Test("Respects task cancellation during retry", .mockURLSession)
        func respectsTaskCancellation() async throws {
            nonisolated(unsafe) var requestCount = 0

            await MockURLProtocol.setHandler { request in
                requestCount += 1

                if requestCount == 1 {
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 200,
                        httpVersion: "HTTP/1.1",
                        headerFields: [
                            "Content-Type": "application/json",
                            "Link": "<https://huggingface.co/api/models?cursor=page2>; rel=\"next\"",
                        ]
                    )!
                    return (response, Data("[{\"id\": \"model/1\"}]".utf8))
                }

                // Keep returning 503 to trigger retry loop
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 503,
                    httpVersion: "HTTP/1.1",
                    headerFields: [:]
                )!
                return (response, Data())
            }

            let client = createMockClient()

            let task = Task {
                var models: [Model] = []
                for try await model in client.listAllModels() {
                    models.append(model)
                }
                return models
            }

            // Give time for first page to complete and retry loop to start
            try await Task.sleep(for: .milliseconds(100))

            // Cancel the task
            task.cancel()

            // Task should throw CancellationError
            await #expect(throws: (any Error).self) {
                _ = try await task.value
            }

            // Should have made fewer requests than exhausting all retries (7)
            #expect(requestCount < 7)
        }
    }
#endif  // swift(>=6.1)

// MARK: - Integration Tests (Real API Calls)

/// Integration tests for pagination that make real API calls.
///
/// Skip these tests in CI by setting the `SKIP_INTEGRATION_TESTS` environment variable.
@Suite(
    "Pagination Integration Tests",
    .enabled(if: ProcessInfo.processInfo.environment["SKIP_INTEGRATION_TESTS"] == nil)
)
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
