import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Replay
import Testing

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("Model Tests", .serialized)
    struct ModelTests {
        private func createClient() -> HubClient {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [PlaybackURLProtocol.self]
            let session = URLSession(configuration: configuration)

            return HubClient(
                session: session,
                host: URL(string: "https://huggingface.co")!,
                userAgent: "TestClient/1.0",
                cache: nil
            )
        }

        @Test(
            "List models with no parameters",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/models",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
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
                    }
                ]
            )
        )
        func testListModels() async throws {
            let client = createClient()
            let result = try await client.listModels()

            #expect(result.items.count == 2)
            #expect(result.items[0].id == "facebook/bart-large")
            #expect(result.items[0].author == "facebook")
            #expect(result.items[1].id == "google/bert-base-uncased")
        }

        @Test(
            "List models with search parameter",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/models?search=bert",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
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
                    }
                ]
            )
        )
        func testListModelsWithSearch() async throws {
            let client = createClient()
            let result = try await client.listModels(search: "bert")

            #expect(result.items.count == 1)
            #expect(result.items[0].id == "google/bert-base-uncased")
        }

        @Test(
            "Get specific model",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/models/facebook/bart-large",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
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
                    }
                ]
            )
        )
        func testGetModel() async throws {
            let client = createClient()
            let repoID: Repo.ID = "facebook/bart-large"
            let model = try await client.getModel(repoID)

            #expect(model.id == "facebook/bart-large")
            #expect(model.author == "facebook")
            #expect(model.downloads == 1000000)
        }

        @Test(
            "Get model with revision",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/models/facebook/bart-large/revision/v1.0",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "id": "facebook/bart-large",
                            "modelId": "facebook/bart-large",
                            "author": "facebook",
                            "downloads": 1000000,
                            "likes": 500,
                            "pipeline_tag": "text-generation"
                        }
                        """
                    }
                ]
            )
        )
        func testGetModelWithRevision() async throws {
            let client = createClient()
            let repoID: Repo.ID = "facebook/bart-large"
            let model = try await client.getModel(repoID, revision: "v1.0")

            #expect(model.id == "facebook/bart-large")
        }

        @Test(
            "Get model tags",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/models-tags-by-type",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
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
                    }
                ]
            )
        )
        func testGetModelTags() async throws {
            let client = createClient()
            let tags = try await client.getModelTags()

            #expect(tags["pipeline_tag"]?.count == 2)
            #expect(tags["library"]?.count == 2)
        }

        @Test(
            "Handle 404 error for model",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/models/nonexistent/model",
                        404,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "error": "Model not found"
                        }
                        """
                    }
                ]
            )
        )
        func testGetModelNotFound() async throws {
            let client = createClient()
            let repoID: Repo.ID = "nonexistent/model"

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.getModel(repoID)
            }
        }

        @Test(
            "Handle authorization requirement",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/models/private/model",
                        401,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "error": "Unauthorized"
                        }
                        """
                    }
                ]
            )
        )
        func testGetModelRequiresAuth() async throws {
            let client = createClient()
            let repoID: Repo.ID = "private/model"

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.getModel(repoID)
            }
        }

        @Test(
            "Client sends authorization header when token provided",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/models/private/model",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "id": "private/model",
                            "modelId": "private/model",
                            "author": "private",
                            "private": true
                        }
                        """
                    }
                ]
            )
        )
        func testClientWithBearerToken() async throws {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [PlaybackURLProtocol.self]
            let session = URLSession(configuration: configuration)
            let client = HubClient(
                session: session,
                host: URL(string: "https://huggingface.co")!,
                bearerToken: "test_token",
                cache: nil
            )

            let repoID: Repo.ID = "private/model"
            let model = try await client.getModel(repoID)

            #expect(model.id == "private/model")
        }
    }

#endif  // swift(>=6.1)
