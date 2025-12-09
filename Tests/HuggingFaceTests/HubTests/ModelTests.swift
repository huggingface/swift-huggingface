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
    @Suite("Model Tests", .serialized)
    struct ModelTests {
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

        @Test("List models with no parameters", .mockURLSession)
        func testListModels() async throws {
            let url = URL(string: "https://huggingface.co/api/models")!

            // Mock response with a list of models
            let mockResponse = """
                [
                    {
                        "id": "facebook/bart-large",
                        "author": "facebook",
                        "downloads": 1000000,
                        "likes": 500,
                        "pipeline_tag": "text-generation"
                    },
                    {
                        "id": "google/bert-base-uncased",
                        "author": "google",
                        "downloads": 2000000,
                        "likes": 1000,
                        "pipeline_tag": "fill-mask"
                    }
                ]
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/models")
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
            let result = try await client.listModels()

            #expect(result.items.count == 2)
            #expect(result.items[0].id == "facebook/bart-large")
            #expect(result.items[0].author == "facebook")
            #expect(result.items[1].id == "google/bert-base-uncased")
        }

        @Test("List models with search parameter", .mockURLSession)
        func testListModelsWithSearch() async throws {
            let mockResponse = """
                [
                    {
                        "id": "google/bert-base-uncased",
                        "author": "google",
                        "downloads": 2000000,
                        "likes": 1000,
                        "pipeline_tag": "fill-mask"
                    }
                ]
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/models")
                #expect(request.url?.query?.contains("search=bert") == true)

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
            let result = try await client.listModels(search: "bert")

            #expect(result.items.count == 1)
            #expect(result.items[0].id == "google/bert-base-uncased")
        }

        @Test("Get specific model", .mockURLSession)
        func testGetModel() async throws {
            let mockResponse = """
                {
                    "id": "facebook/bart-large",
                    "modelId": "facebook/bart-large",
                    "author": "facebook",
                    "downloads": 1000000,
                    "likes": 500,
                    "pipeline_tag": "text-generation",
                    "tags": ["pytorch", "transformers"]
                }
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/models/facebook/bart-large")
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
            let repoID: Repo.ID = "facebook/bart-large"
            let model = try await client.getModel(repoID)

            #expect(model.id == "facebook/bart-large")
            #expect(model.author == "facebook")
            #expect(model.downloads == 1000000)
        }

        @Test("Get model with revision", .mockURLSession)
        func testGetModelWithRevision() async throws {
            let mockResponse = """
                {
                    "id": "facebook/bart-large",
                    "modelId": "facebook/bart-large",
                    "author": "facebook",
                    "downloads": 1000000,
                    "likes": 500,
                    "pipeline_tag": "text-generation"
                }
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/models/facebook/bart-large/revision/v1.0")
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
            let repoID: Repo.ID = "facebook/bart-large"
            let model = try await client.getModel(repoID, revision: "v1.0")

            #expect(model.id == "facebook/bart-large")
        }

        @Test("Get model tags", .mockURLSession)
        func testGetModelTags() async throws {
            let mockResponse = """
                {
                    "tags": {
                        "pipeline_tag": [
                            {"id": "text-classification", "label": "Text Classification"},
                            {"id": "text-generation", "label": "Text Generation"}
                        ],
                        "library": [
                            {"id": "pytorch", "label": "PyTorch"},
                            {"id": "transformers", "label": "Transformers"}
                        ]
                    }
                }
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/models-tags-by-type")
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
            let tags = try await client.getModelTags()

            #expect(tags["pipeline_tag"]?.count == 2)
            #expect(tags["library"]?.count == 2)
        }

        @Test("Handle 404 error for model", .mockURLSession)
        func testGetModelNotFound() async throws {
            let errorResponse = """
                {
                    "error": "Model not found"
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
            let repoID: Repo.ID = "nonexistent/model"

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.getModel(repoID)
            }
        }

        @Test("Handle authorization requirement", .mockURLSession)
        func testGetModelRequiresAuth() async throws {
            let errorResponse = """
                {
                    "error": "Unauthorized"
                }
                """

            await MockURLProtocol.setHandler { request in
                // Verify no authorization header is present
                #expect(request.value(forHTTPHeaderField: "Authorization") == nil)

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 401,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(errorResponse.utf8))
            }

            let client = createMockClient()
            let repoID: Repo.ID = "private/model"

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.getModel(repoID)
            }
        }

        @Test("Client sends authorization header when token provided", .mockURLSession)
        func testClientWithBearerToken() async throws {
            let mockResponse = """
                {
                    "id": "private/model",
                    "modelId": "private/model",
                    "author": "private",
                    "private": true
                }
                """

            await MockURLProtocol.setHandler { request in
                // Verify authorization header is present
                #expect(request.value(forHTTPHeaderField: "Authorization") == "Bearer test_token")

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [MockURLProtocol.self]
            let session = URLSession(configuration: configuration)
            let client = HubClient(
                session: session,
                host: URL(string: "https://huggingface.co")!,
                bearerToken: "test_token"
            )

            let repoID: Repo.ID = "private/model"
            let model = try await client.getModel(repoID)

            #expect(model.id == "private/model")
        }
    }

#endif  // swift(>=6.1)
