import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Replay
import Testing

@testable import HuggingFace

#if swift(>=6.1)

    /// Tests for the Feature Extraction API
    @Suite("Feature Extraction Tests", .serialized)
    struct FeatureExtractionTests {
        private func createClient() -> InferenceClient {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [PlaybackURLProtocol.self]
            let session = URLSession(configuration: configuration)
            return InferenceClient(
                session: session,
                host: URL(string: "https://router.huggingface.co")!,
                userAgent: "TestClient/1.0"
            )
        }

        @Test(
            "Basic feature extraction with single input",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/embeddings",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "embeddings": [
                                [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
                            ],
                            "metadata": {
                                "model": "sentence-transformers/all-MiniLM-L6-v2",
                                "dimension": 384
                            }
                        }
                        """
                    }
                ]
            )
        )
        func testBasicFeatureExtraction() async throws {
            let client = createClient()
            let result = try await client.featureExtraction(
                model: "sentence-transformers/all-MiniLM-L6-v2",
                input: "Hello, world!"
            )

            #expect(result.embeddings.count == 1)
            #expect(result.embeddings[0].count == 10)
            #expect(result.embeddings[0][0] == 0.1)
            #expect(result.embeddings[0][9] == 1.0)
            #expect(result.metadata?["model"] == .string("sentence-transformers/all-MiniLM-L6-v2"))
            #expect(result.metadata?["dimension"] == .int(384))
        }

        @Test(
            "Feature extraction with multiple inputs",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/embeddings",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "embeddings": [
                                [0.1, 0.2, 0.3, 0.4, 0.5],
                                [0.6, 0.7, 0.8, 0.9, 1.0],
                                [1.1, 1.2, 1.3, 1.4, 1.5]
                            ],
                            "metadata": {
                                "model": "sentence-transformers/all-MiniLM-L6-v2",
                                "dimension": 5,
                                "input_count": 3
                            }
                        }
                        """
                    }
                ]
            )
        )
        func testFeatureExtractionWithMultipleInputs() async throws {
            let client = createClient()
            let result = try await client.featureExtraction(
                model: "sentence-transformers/all-MiniLM-L6-v2",
                inputs: ["First text", "Second text", "Third text"]
            )

            #expect(result.embeddings.count == 3)
            #expect(result.embeddings[0].count == 5)
            #expect(result.embeddings[1].count == 5)
            #expect(result.embeddings[2].count == 5)
            #expect(result.metadata?["input_count"] == .int(3))
        }

        @Test(
            "Feature extraction with all parameters",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/embeddings",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "embeddings": [
                                [0.1, 0.2, 0.3, 0.4, 0.5]
                            ],
                            "metadata": {
                                "model": "sentence-transformers/all-MiniLM-L6-v2",
                                "normalized": true,
                                "truncated": false
                            }
                        }
                        """
                    }
                ]
            )
        )
        func testFeatureExtractionWithAllParameters() async throws {
            let client = createClient()
            let result = try await client.featureExtraction(
                model: "sentence-transformers/all-MiniLM-L6-v2",
                input: "Test text",
                provider: .hfInference,
                normalize: true,
                truncate: false,
                parameters: [
                    "max_length": .int(512)
                ]
            )

            #expect(result.embeddings.count == 1)
            #expect(result.metadata?["normalized"] == .bool(true))
            #expect(result.metadata?["truncated"] == .bool(false))
        }

        @Test(
            "Feature extraction with large embedding dimensions",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/embeddings",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        let largeEmbedding = (0 ..< 1536).map { Double($0) / 1000.0 }
                        let embeddingsJSON = largeEmbedding.map { String($0) }.joined(separator: ", ")
                        return """
                            {
                                "embeddings": [
                                    [\(embeddingsJSON)]
                                ],
                                "metadata": {
                                    "model": "text-embedding-ada-002",
                                    "dimension": 1536
                                }
                            }
                            """
                    }
                ]
            )
        )
        func testFeatureExtractionWithLargeDimensions() async throws {
            let client = createClient()
            let result = try await client.featureExtraction(
                model: "text-embedding-ada-002",
                input: "Large embedding test"
            )

            #expect(result.embeddings.count == 1)
            #expect(result.embeddings[0].count == 1536)
            #expect(result.embeddings[0][0] == 0.0)
            #expect(result.embeddings[0][1535] == 1.535)
            #expect(result.metadata?["dimension"] == .int(1536))
        }

        @Test(
            "Feature extraction with multilingual text",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/embeddings",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "embeddings": [
                                [0.1, 0.2, 0.3, 0.4, 0.5],
                                [0.6, 0.7, 0.8, 0.9, 1.0],
                                [1.1, 1.2, 1.3, 1.4, 1.5]
                            ],
                            "metadata": {
                                "model": "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
                                "language": "mixed",
                                "dimension": 384
                            }
                        }
                        """
                    }
                ]
            )
        )
        func testFeatureExtractionWithMultilingualText() async throws {
            let client = createClient()
            let result = try await client.featureExtraction(
                model: "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
                inputs: [
                    "Hello, world!",
                    "Bonjour, le monde!",
                    "Hola, mundo!",
                ]
            )

            #expect(result.embeddings.count == 3)
            #expect(result.metadata?["language"] == .string("mixed"))
        }

        @Test(
            "Feature extraction handles error response",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/embeddings",
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
        func testFeatureExtractionHandlesError() async throws {
            let client = createClient()

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.featureExtraction(
                    model: "nonexistent-model",
                    input: "Test text"
                )
            }
        }

        @Test(
            "Feature extraction handles invalid input",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/embeddings",
                        400,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "error": "Invalid input format"
                        }
                        """
                    }
                ]
            )
        )
        func testFeatureExtractionHandlesInvalidInput() async throws {
            let client = createClient()

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.featureExtraction(
                    model: "sentence-transformers/all-MiniLM-L6-v2",
                    input: ""
                )
            }
        }

        @Test(
            "Feature extraction with custom parameters",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/embeddings",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "embeddings": [
                                [0.1, 0.2, 0.3, 0.4, 0.5]
                            ],
                            "metadata": {
                                "model": "sentence-transformers/all-MiniLM-L6-v2",
                                "custom_param": "custom_value"
                            }
                        }
                        """
                    }
                ]
            )
        )
        func testFeatureExtractionWithCustomParameters() async throws {
            let client = createClient()
            let result = try await client.featureExtraction(
                model: "sentence-transformers/all-MiniLM-L6-v2",
                input: "Test text",
                parameters: [
                    "custom_param": .string("custom_value"),
                    "batch_size": .int(32),
                ]
            )

            #expect(result.embeddings.count == 1)
            #expect(result.metadata?["custom_param"] == .string("custom_value"))
        }
    }

#endif  // swift(>=6.1)
