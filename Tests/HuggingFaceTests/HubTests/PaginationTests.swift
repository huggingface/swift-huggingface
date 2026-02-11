import Foundation
import Testing

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

@testable import HuggingFace

@Suite("Pagination Tests")
struct PaginationTests {
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

    // MARK: - nextPage(after:) Tests

    #if swift(>=6.1)
        private struct Item: Decodable, Sendable {
            let name: String
        }

        /// Helper to create a HubClient backed by MockURLProtocol.
        private func createMockClient() -> HubClient {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [MockURLProtocol.self]
            let session = URLSession(configuration: configuration)
            return HubClient(
                session: session,
                host: URL(string: "https://huggingface.co")!,
                userAgent: "TestClient/1.0"
            )
        }

        @Test("nextPage returns nil when there is no next URL", .mockURLSession)
        func testNextPageReturnsNilWhenNoNextURL() async throws {
            let page = PaginatedResponse<Item>(items: [], nextURL: nil)
            let client = createMockClient()

            let next = try await client.nextPage(after: page)

            #expect(next == nil)
        }

        @Test("nextPage fetches the next page when a next URL exists", .mockURLSession)
        func testNextPageFetchesNextPage() async throws {
            let nextURL = URL(string: "https://huggingface.co/api/items?page=2")!
            let page = PaginatedResponse<Item>(
                items: [],
                nextURL: nextURL
            )

            let mockResponse = """
                [{"name": "c"}]
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url == nextURL)
                #expect(request.httpMethod == "GET")

                let response = HTTPURLResponse(
                    url: nextURL,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!
                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
            let next = try await client.nextPage(after: page)

            #expect(next != nil)
            #expect(next?.items.count == 1)
            #expect(next?.items[0].name == "c")
            #expect(next?.nextURL == nil)
        }

        @Test("nextPage propagates the next Link header from the response", .mockURLSession)
        func testNextPagePropagatesLinkHeader() async throws {
            let nextURL = URL(string: "https://huggingface.co/api/items?page=2")!
            let page = PaginatedResponse<Item>(
                items: [],
                nextURL: nextURL
            )

            let thirdPageURL = "https://huggingface.co/api/items?page=3"

            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: nextURL,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: [
                        "Content-Type": "application/json",
                        "Link": "<\(thirdPageURL)>; rel=\"next\"",
                    ]
                )!
                return (response, Data("[{\"name\": \"b\"}]".utf8))
            }

            let client = createMockClient()
            let next = try await client.nextPage(after: page)

            #expect(next != nil)
            #expect(next?.nextURL?.absoluteString == thirdPageURL)
        }

        @Test("nextPage preserves missing query params from request URL", .mockURLSession)
        func testNextPagePreservesMissingQueryParams() async throws {
            let page = PaginatedResponse<Item>(
                items: [],
                nextURL: URL(string: "/api/items?skip=2"),
                requestURL: URL(string: "https://huggingface.co/api/items?search=bert&limit=2")
            )

            await MockURLProtocol.setHandler { request in
                let components = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)
                let query = Dictionary(
                    uniqueKeysWithValues: (components?.queryItems ?? []).map { ($0.name, $0.value ?? "") }
                )

                #expect(request.url?.host == "huggingface.co")
                #expect(query["skip"] == "2")
                #expect(query["search"] == "bert")
                #expect(query["limit"] == "2")

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!
                return (response, Data("[{\"name\":\"next\"}]".utf8))
            }

            let client = createMockClient()
            let next = try await client.nextPage(after: page)

            #expect(next?.items.first?.name == "next")
        }

        private final class RequestCounter: @unchecked Sendable {
            private let lock = NSLock()
            private var requests: [URLRequest] = []

            @discardableResult
            func append(_ request: URLRequest) -> Int {
                lock.lock()
                defer { lock.unlock() }
                requests.append(request)
                return requests.count
            }

            var count: Int {
                lock.lock()
                defer { lock.unlock() }
                return requests.count
            }
        }

        @Test("listAllModels fetches first page eagerly and subsequent pages lazily", .mockURLSession)
        func testListAllModelsEagerThenLazyFetch() async throws {
            let requestCounter = RequestCounter()
            let firstPageURL = URL(string: "https://huggingface.co/api/models?limit=2&search=bert")!
            let secondPageURL = URL(string: "https://huggingface.co/api/models?skip=2")!

            await MockURLProtocol.setHandler { request in
                let callIndex = requestCounter.append(request)
                let responseURL = request.url ?? firstPageURL

                let responseHeaders: [String: String]
                let responseBody: String
                if callIndex == 1 {
                    let components = URLComponents(url: responseURL, resolvingAgainstBaseURL: false)
                    let query = Dictionary(
                        uniqueKeysWithValues: (components?.queryItems ?? []).map { ($0.name, $0.value ?? "") }
                    )
                    #expect(query["search"] == "bert")
                    #expect(query["limit"] == "2")

                    responseHeaders = [
                        "Content-Type": "application/json",
                        "Link": "<\(secondPageURL.absoluteString)>; rel=\"next\"",
                    ]
                    responseBody = "[{\"id\":\"org/model-a\"}]"
                } else {
                    let components = URLComponents(url: responseURL, resolvingAgainstBaseURL: false)
                    let query = Dictionary(
                        uniqueKeysWithValues: (components?.queryItems ?? []).map { ($0.name, $0.value ?? "") }
                    )
                    #expect(query["skip"] == "2")
                    #expect(query["search"] == "bert")
                    #expect(query["limit"] == "2")

                    responseHeaders = [
                        "Content-Type": "application/json"
                    ]
                    responseBody = "[{\"id\":\"org/model-b\"}]"
                }

                let response = HTTPURLResponse(
                    url: responseURL,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: responseHeaders
                )!
                return (response, Data(responseBody.utf8))
            }

            let client = createMockClient()
            let pages = try await client.listAllModels(search: "bert", perPage: 2)
            #expect(requestCounter.count == 1)

            var iterator = pages.makeAsyncIterator()
            let firstPage = try await iterator.next()
            #expect(firstPage?.items.map(\.id.description) == ["org/model-a"])
            #expect(requestCounter.count == 1)

            let secondPage = try await iterator.next()
            #expect(secondPage?.items.map(\.id.description) == ["org/model-b"])
            #expect(requestCounter.count == 2)

            let donePage = try await iterator.next()
            #expect(donePage == nil)
            #expect(requestCounter.count == 2)
        }

        @Test("listAllModels does not fetch additional pages after early break", .mockURLSession)
        func testListAllModelsEarlyBreakStopsFetching() async throws {
            let requestCounter = RequestCounter()
            let secondPageURL = URL(string: "https://huggingface.co/api/models?skip=1")!

            await MockURLProtocol.setHandler { request in
                _ = requestCounter.append(request)
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: [
                        "Content-Type": "application/json",
                        "Link": "<\(secondPageURL.absoluteString)>; rel=\"next\"",
                    ]
                )!
                return (response, Data("[{\"id\":\"org/model-a\"}]".utf8))
            }

            let client = createMockClient()
            let pages = try await client.listAllModels(perPage: 1)
            #expect(requestCounter.count == 1)

            var pageCount = 0
            for try await _ in pages {
                pageCount += 1
                break
            }

            #expect(pageCount == 1)
            #expect(requestCounter.count == 1)
        }

        private actor PageFetcher {
            private var pendingPages: [PaginatedResponse<Item>]
            private var totalFetches = 0

            init(pendingPages: [PaginatedResponse<Item>]) {
                self.pendingPages = pendingPages
            }

            func next(after page: PaginatedResponse<Item>) -> PaginatedResponse<Item>? {
                guard page.nextURL != nil else {
                    return nil
                }
                totalFetches += 1
                guard !pendingPages.isEmpty else {
                    return nil
                }
                return pendingPages.removeFirst()
            }

            func fetchCount() -> Int {
                totalFetches
            }
        }

        @Test("Pages iterates across multiple pages lazily")
        func testPagesIteratesAcrossMultiplePages() async throws {
            let first = PaginatedResponse(
                items: [Item(name: "a"), Item(name: "b")],
                nextURL: URL(string: "https://huggingface.co/api/items?page=2")
            )
            let second = PaginatedResponse(
                items: [Item(name: "c")],
                nextURL: URL(string: "https://huggingface.co/api/items?page=3")
            )
            let third = PaginatedResponse(
                items: [Item(name: "d"), Item(name: "e")],
                nextURL: nil
            )

            let fetcher = PageFetcher(pendingPages: [second, third])
            let pages = Pages(firstPage: first) { page in
                await fetcher.next(after: page)
            }

            var namesByPage: [[String]] = []
            for try await page in pages {
                namesByPage.append(page.items.map(\.name))
            }

            #expect(namesByPage == [["a", "b"], ["c"], ["d", "e"]])
            #expect(await fetcher.fetchCount() == 2)
        }

        @Test("Pages stops fetching when iteration ends early")
        func testPagesEarlyBreakAvoidsAdditionalFetches() async throws {
            let first = PaginatedResponse(
                items: [Item(name: "a")],
                nextURL: URL(string: "https://huggingface.co/api/items?page=2")
            )
            let second = PaginatedResponse(
                items: [Item(name: "b")],
                nextURL: nil
            )
            let fetcher = PageFetcher(pendingPages: [second])
            let pages = Pages(firstPage: first) { page in
                await fetcher.next(after: page)
            }

            var yieldedPages = 0
            for try await _ in pages {
                yieldedPages += 1
                break
            }

            #expect(yieldedPages == 1)
            #expect(await fetcher.fetchCount() == 0)
        }

        @Test("Pages yields one page when there is no next URL")
        func testPagesSinglePage() async throws {
            let first = PaginatedResponse(
                items: [Item(name: "only")],
                nextURL: nil
            )
            let fetcher = PageFetcher(pendingPages: [])
            let pages = Pages(firstPage: first) { page in
                await fetcher.next(after: page)
            }

            var namesByPage: [[String]] = []
            for try await page in pages {
                namesByPage.append(page.items.map(\.name))
            }

            #expect(namesByPage == [["only"]])
            #expect(await fetcher.fetchCount() == 0)
        }

        @Test("Pages yields an empty first page without fetching more")
        func testPagesEmptyFirstPage() async throws {
            let first = PaginatedResponse<Item>(
                items: [],
                nextURL: nil
            )
            let fetcher = PageFetcher(pendingPages: [])
            let pages = Pages(firstPage: first) { page in
                await fetcher.next(after: page)
            }

            var pageCount = 0
            var firstPageItemCount: Int?
            for try await page in pages {
                pageCount += 1
                if firstPageItemCount == nil {
                    firstPageItemCount = page.items.count
                }
            }

            #expect(pageCount == 1)
            #expect(firstPageItemCount == 0)
            #expect(await fetcher.fetchCount() == 0)
        }
    #endif  // swift(>=6.1)

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
}
