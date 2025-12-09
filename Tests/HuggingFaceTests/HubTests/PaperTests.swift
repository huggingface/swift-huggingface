import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Testing

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("Paper Tests", .serialized)
    struct PaperTests {
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

        @Test("List papers with no parameters", .mockURLSession)
        func testListPapers() async throws {
            let url = URL(string: "https://huggingface.co/api/papers")!

            let mockResponse = """
                [
                    {
                        "id": "2103.00020",
                        "title": "Learning Transferable Visual Models From Natural Language Supervision",
                        "summary": "State-of-the-art computer vision systems...",
                        "publishedAt": "2021-03-01T00:00:00.000Z"
                    },
                    {
                        "id": "2005.14165",
                        "title": "Language Models are Few-Shot Learners",
                        "summary": "Recent work has demonstrated substantial gains...",
                        "publishedAt": "2020-05-28T00:00:00.000Z"
                    }
                ]
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/papers")
                #expect(request.httpMethod == "GET")

                let response = HTTPURLResponse(
                    url: url,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
            let result = try await client.listPapers()

            #expect(result.items.count == 2)
            #expect(result.items[0].id == "2103.00020")
            #expect(
                result.items[0].title
                    == "Learning Transferable Visual Models From Natural Language Supervision"
            )
            #expect(result.items[1].id == "2005.14165")
        }

        @Test("List papers with search parameter", .mockURLSession)
        func testListPapersWithSearch() async throws {
            let mockResponse = """
                [
                    {
                        "id": "2103.00020",
                        "title": "Learning Transferable Visual Models From Natural Language Supervision",
                        "summary": "State-of-the-art computer vision systems...",
                        "publishedAt": "2021-03-01T00:00:00.000Z"
                    }
                ]
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/papers")
                #expect(request.url?.query?.contains("search=CLIP") == true)

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
            let result = try await client.listPapers(search: "CLIP")

            #expect(result.items.count == 1)
            #expect(result.items[0].id == "2103.00020")
        }

        @Test("List papers with sort parameter", .mockURLSession)
        func testListPapersWithSort() async throws {
            let mockResponse = """
                [
                    {
                        "id": "2005.14165",
                        "title": "Language Models are Few-Shot Learners",
                        "publishedAt": "2020-05-28T00:00:00.000Z"
                    }
                ]
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/papers")
                #expect(request.url?.query?.contains("sort=trending") == true)

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
            let result = try await client.listPapers(sort: "trending")

            #expect(result.items.count == 1)
        }

        @Test("List papers with limit parameter", .mockURLSession)
        func testListPapersWithLimit() async throws {
            let mockResponse = """
                [
                    {
                        "id": "2103.00020",
                        "title": "Learning Transferable Visual Models From Natural Language Supervision",
                        "publishedAt": "2021-03-01T00:00:00.000Z"
                    }
                ]
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/papers")
                #expect(request.url?.query?.contains("limit=1") == true)

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
            let result = try await client.listPapers(limit: 1)

            #expect(result.items.count == 1)
        }

        @Test("Get specific paper", .mockURLSession)
        func testGetPaper() async throws {
            let mockResponse = """
                {
                    "id": "2103.00020",
                    "title": "Learning Transferable Visual Models From Natural Language Supervision",
                    "summary": "State-of-the-art computer vision systems are trained to predict...",
                    "published": "2021-03-01T00:00:00.000Z",
                    "authors": ["Alec Radford", "Jong Wook Kim"],
                    "upvotes": 150
                }
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/papers/2103.00020")
                #expect(request.httpMethod == "GET")

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
            let paper = try await client.getPaper("2103.00020")

            #expect(paper.id == "2103.00020")
            #expect(
                paper.title
                    == "Learning Transferable Visual Models From Natural Language Supervision"
            )
            #expect(paper.upvotes == 150)
        }

        @Test("Handle 404 error for paper", .mockURLSession)
        func testGetPaperNotFound() async throws {
            let errorResponse = """
                {
                    "error": "Paper not found"
                }
                """

            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 404,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(errorResponse.utf8))
            }

            let client = createMockClient()

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.getPaper("9999.99999")
            }
        }
    }

#endif  // swift(>=6.1)
