import Foundation
import Testing

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("Collection Tests", .serialized)
    struct CollectionTests {
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

        @Test("List collections with no parameters", .mockURLSession)
        func testListCollections() async throws {
            let url = URL(string: "https://huggingface.co/api/collections")!

            let mockResponse = """
                [
                    {
                        "id": "123",
                        "slug": "user/my-collection",
                        "title": "My Collection",
                        "description": "A test collection",
                        "owner": "user",
                        "position": 1,
                        "private": false,
                        "theme": "default",
                        "upvotes": 10,
                        "items": []
                    },
                    {
                        "id": "456",
                        "slug": "org/another-collection",
                        "title": "Another Collection",
                        "owner": "org",
                        "position": 2,
                        "private": false,
                        "theme": "default",
                        "upvotes": 5,
                        "items": []
                    }
                ]
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/collections")
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
            let result = try await client.listCollections()

            #expect(result.items.count == 2)
            #expect(result.items[0].slug == "user/my-collection")
            #expect(result.items[0].title == "My Collection")
            #expect(result.items[1].slug == "org/another-collection")
        }

        @Test("List collections with owner filter", .mockURLSession)
        func testListCollectionsWithOwner() async throws {
            let mockResponse = """
                [
                    {
                        "id": "123",
                        "slug": "user/my-collection",
                        "title": "My Collection",
                        "owner": "user",
                        "position": 1,
                        "private": false,
                        "theme": "default",
                        "upvotes": 10,
                        "items": []
                    }
                ]
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/collections")
                #expect(request.url?.query?.contains("owner=user") == true)

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
            let result = try await client.listCollections(owner: "user")

            #expect(result.items.count == 1)
            #expect(result.items[0].owner == "user")
        }

        @Test("Get specific collection", .mockURLSession)
        func testGetCollection() async throws {
            let mockResponse = """
                {
                    "id": "123",
                    "slug": "user/my-collection",
                    "title": "My Collection",
                    "description": "A test collection",
                    "owner": "user",
                    "position": 1,
                    "private": false,
                    "theme": "default",
                    "upvotes": 10,
                    "items": [
                        {
                            "item_type": "model",
                            "item_id": "facebook/bart-large",
                            "position": 0,
                            "note": "A great model"
                        }
                    ]
                }
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/collections/user/my-collection")
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
            let collection = try await client.getCollection("user/my-collection")

            #expect(collection.slug == "user/my-collection")
            #expect(collection.title == "My Collection")
            #expect(collection.items?.count == 1)
            #expect(collection.items?[0].itemType == "model")
            #expect(collection.items?[0].itemID == "facebook/bart-large")
        }

        @Test("Handle 404 error for collection", .mockURLSession)
        func testGetCollectionNotFound() async throws {
            let errorResponse = """
                {
                    "error": "Collection not found"
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
                _ = try await client.getCollection("nonexistent/collection")
            }
        }
    }

#endif  // swift(>=6.1)
