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
