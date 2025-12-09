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

    /// Tests for the Feature Extraction API
    @Suite("Feature Extraction Tests", .serialized)
    struct FeatureExtractionTests {
        /// Helper to create a URL session with mock protocol handlers
        func createMockClient() -> InferenceClient {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [MockURLProtocol.self]
            let session = URLSession(configuration: configuration)
            return InferenceClient(
                session: session,
                host: URL(string: "https://router.huggingface.co")!,
                userAgent: "TestClient/1.0"
            )
        }

        @Test("Basic feature extraction with single input", .mockURLSession)
        func testBasicFeatureExtraction() async throws {
            let mockResponse = """
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

            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
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

        @Test("Feature extraction with multiple inputs", .mockURLSession)
        func testFeatureExtractionWithMultipleInputs() async throws {
            let mockResponse = """
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

            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
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

        @Test("Feature extraction with all parameters", .mockURLSession)
        func testFeatureExtractionWithAllParameters() async throws {
            let mockResponse = """
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

            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
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

        @Test("Feature extraction with large embedding dimensions", .mockURLSession)
        func testFeatureExtractionWithLargeDimensions() async throws {
            // Generate a large embedding vector (1536 dimensions like OpenAI's text-embedding-ada-002)
            let largeEmbedding = (0 ..< 1536).map { Double($0) / 1000.0 }
            let embeddingsJSON = largeEmbedding.map { String($0) }.joined(separator: ", ")

            let mockResponse = """
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

            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
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

        @Test("Feature extraction with multilingual text", .mockURLSession)
        func testFeatureExtractionWithMultilingualText() async throws {
            let mockResponse = """
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

            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
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

        @Test("Feature extraction handles error response", .mockURLSession)
        func testFeatureExtractionHandlesError() async throws {
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

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.featureExtraction(
                    model: "nonexistent-model",
                    input: "Test text"
                )
            }
        }

        @Test("Feature extraction handles invalid input", .mockURLSession)
        func testFeatureExtractionHandlesInvalidInput() async throws {
            let errorResponse = """
                {
                    "error": "Invalid input format"
                }
                """

            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 400,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(errorResponse.utf8))
            }

            let client = createMockClient()

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.featureExtraction(
                    model: "sentence-transformers/all-MiniLM-L6-v2",
                    input: ""
                )
            }
        }

        @Test("Feature extraction with custom parameters", .mockURLSession)
        func testFeatureExtractionWithCustomParameters() async throws {
            let mockResponse = """
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

            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
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
