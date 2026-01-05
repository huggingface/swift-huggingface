import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Testing

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
            var models: [Model] = []
            for try await model in client.listAllModels() {
                models.append(model)
            }

            #expect(models.count == 2)
            #expect(models[0].id == "facebook/bart-large")
            #expect(models[0].author == "facebook")
            #expect(models[1].id == "google/bert-base-uncased")
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
            var models: [Model] = []
            for try await model in client.listAllModels(search: "bert") {
                models.append(model)
            }

            #expect(models.count == 1)
            #expect(models[0].id == "google/bert-base-uncased")
        }

        @Test("List all models with automatic pagination", .mockURLSession)
        func testListAllModels() async throws {
            let mockResponsePage1 = """
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

            let mockResponsePage2 = """
                [
                    {
                        "id": "openai/gpt-2",
                        "author": "openai",
                        "downloads": 3000000,
                        "likes": 1500,
                        "pipeline_tag": "text-generation"
                    }
                ]
                """

            // Use nonisolated(unsafe) for test counter since MockURLProtocol handler is Sendable
            nonisolated(unsafe) var requestCount = 0
            await MockURLProtocol.setHandler { request in
                requestCount += 1

                if requestCount == 1 {
                    #expect(request.url?.path == "/api/models")
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 200,
                        httpVersion: "HTTP/1.1",
                        headerFields: [
                            "Content-Type": "application/json",
                            "Link": "<https://huggingface.co/api/models?cursor=abc123>; rel=\"next\"",
                        ]
                    )!
                    return (response, Data(mockResponsePage1.utf8))
                } else {
                    #expect(request.url?.absoluteString.contains("cursor=abc123") == true)
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 200,
                        httpVersion: "HTTP/1.1",
                        headerFields: ["Content-Type": "application/json"]
                    )!
                    return (response, Data(mockResponsePage2.utf8))
                }
            }

            let client = createMockClient()
            var models: [Model] = []
            for try await model in client.listAllModels() {
                models.append(model)
            }

            #expect(models.count == 3)
            #expect(models[0].id == "facebook/bart-large")
            #expect(models[1].id == "google/bert-base-uncased")
            #expect(models[2].id == "openai/gpt-2")
            #expect(requestCount == 2)  // Two pages fetched
        }

        @Test("List all models respects limit", .mockURLSession)
        func listAllModelsWithLimit() async throws {
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
                    },
                    {
                        "id": "openai/gpt-2",
                        "author": "openai",
                        "downloads": 3000000,
                        "likes": 1500,
                        "pipeline_tag": "text-generation"
                    }
                ]
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/models")
                #expect(request.url?.query?.contains("limit=2") == true)

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: [
                        "Content-Type": "application/json",
                        "Link": "<https://huggingface.co/api/models?cursor=abc123>; rel=\"next\"",
                    ]
                )!
                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
            var models: [Model] = []
            for try await model in client.listAllModels(limit: 2) {
                models.append(model)
            }

            #expect(models.count == 2)  // Stopped at limit even though page had 3
            #expect(models[0].id == "facebook/bart-large")
            #expect(models[1].id == "google/bert-base-uncased")
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
            // Mock response matches real API format (no "tags" wrapper)
            let mockResponse = """
                {
                    "pipeline_tag": [
                        {"id": "text-classification", "label": "Text Classification"},
                        {"id": "text-generation", "label": "Text Generation"}
                    ],
                    "library": [
                        {"id": "pytorch", "label": "PyTorch"},
                        {"id": "transformers", "label": "Transformers"}
                    ]
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

    // MARK: - Integration Tests (Real API Calls)

    /// Integration tests that make real API calls to the Hugging Face Hub.
    /// These tests verify that our implementation works correctly with the actual API,
    /// catching issues like response format changes or pagination header changes.
    ///
    /// Skip these tests in CI by setting the `SKIP_INTEGRATION_TESTS` environment variable.
    @Suite(
        "Model Integration Tests",
        .serialized,
        .enabled(if: ProcessInfo.processInfo.environment["SKIP_INTEGRATION_TESTS"] == nil)
    )
    struct ModelIntegrationTests {
        let client = HubClient()

        @Test("List models returns results from real API")
        func listModelsFromAPI() async throws {
            var count = 0
            for try await model in client.listAllModels(limit: 10) {
                count += 1
                // Verify basic model properties are populated
                #expect(!model.id.rawValue.isEmpty)
            }

            #expect(count > 0)
            #expect(count <= 10)
        }

        @Test("List models with search parameter")
        func listModelsWithSearch() async throws {
            var count = 0
            for try await model in client.listAllModels(search: "bert", limit: 10) {
                count += 1
                // Verify search results contain "bert" in the model ID
                #expect(model.id.rawValue.lowercased().contains("bert"))
            }

            #expect(count > 0)
        }

        @Test("List models with author filter")
        func listModelsWithAuthor() async throws {
            var count = 0
            for try await model in client.listAllModels(author: "google", limit: 10) {
                count += 1
                // Verify all models are from the specified author by checking namespace
                // (the author field may not be populated in list responses)
                #expect(model.id.namespace == "google")
            }

            #expect(count > 0)
        }

        @Test("Paginate over models from real API")
        func paginateModels() async throws {
            // Make real API calls and verify pagination works
            var count = 0
            for try await _ in client.listAllModels(limit: 5) {
                count += 1
            }

            #expect(count == 5)
        }

        @Test("Pagination fetches multiple pages")
        func paginationFetchesMultiplePages() async throws {
            // Request more items than a single page returns (default page size is typically 20-100)
            // to ensure pagination actually works across pages
            var count = 0
            for try await _ in client.listAllModels(limit: 50) {
                count += 1
            }

            #expect(count == 50)
        }

        @Test("Get model info for known model")
        func getModelInfo() async throws {
            // Use a well-known model that's unlikely to be deleted
            let repoID: Repo.ID = "google-bert/bert-base-uncased"
            let model = try await client.getModel(repoID)

            #expect(model.id == "google-bert/bert-base-uncased")
            #expect(model.author == "google-bert")
            #expect(model.downloads ?? 0 > 0)
        }

        @Test("Get model tags from real API")
        func getModelTags() async throws {
            let tags = try await client.getModelTags()

            // Verify we get some tag categories
            #expect(!tags.isEmpty)
            // Common tag categories that should exist
            #expect(tags["pipeline_tag"] != nil || tags["library"] != nil)
        }

        // MARK: - Sorting Tests

        @Test("List models sorted by downloads")
        func listModelsSortByDownloads() async throws {
            var count = 0
            for try await model in client.listAllModels(sort: "downloads", limit: 10) {
                count += 1
                // Sorted results should have downloads populated
                #expect(model.downloads != nil)
            }
            #expect(count == 10)
        }

        @Test("List models sorted by likes")
        func listModelsSortByLikes() async throws {
            var count = 0
            for try await model in client.listAllModels(sort: "likes", limit: 10) {
                count += 1
                // Sorted results should have likes populated
                #expect(model.likes != nil)
            }
            #expect(count == 10)
        }

        @Test("List models with filter parameter")
        func listModelsWithFilter() async throws {
            var count = 0
            for try await model in client.listAllModels(filter: "text-generation", limit: 10) {
                count += 1
                // Models with filter should have the pipeline tag
                #expect(model.pipelineTag == "text-generation" || model.tags?.contains("text-generation") == true)
            }
            #expect(count > 0)
        }

        @Test("List models with full parameter")
        func listModelsWithFull() async throws {
            var count = 0
            for try await model in client.listAllModels(limit: 5, full: true) {
                count += 1
                // Full response should include additional metadata
                #expect(model.sha != nil || model.lastModified != nil)
            }
            #expect(count > 0)
        }
    }

#endif  // swift(>=6.1)
