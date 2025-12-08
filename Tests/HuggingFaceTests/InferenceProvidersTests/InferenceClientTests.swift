import Foundation
import Testing

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif


@testable import HuggingFace

#if swift(>=6.1)

    /// Tests for the InferenceClient core functionality
    @Suite("Inference Client Tests", .serialized)
    struct InferenceClientTests {
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

        @Test("Client initialization with custom parameters", .mockURLSession)
        func testClientInitialization() async throws {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [MockURLProtocol.self]
            let session = URLSession(configuration: configuration)
            let host = URL(string: "https://custom.huggingface.co")!
            let client = InferenceClient(
                session: session,
                host: host,
                userAgent: "CustomAgent/1.0",
                bearerToken: "test_token"
            )

            #expect(client.host == host.appendingPathComponent(""))
            #expect(client.userAgent == "CustomAgent/1.0")
            #expect(await client.bearerToken == "test_token")
        }

        @Test("Client sends authorization header when token provided", .mockURLSession)
        func testClientWithBearerToken() async throws {
            let mockResponse = """
                {
                    "id": "test-id",
                    "object": "chat.completion",
                    "created": 1677652288,
                    "model": "test-model",
                    "choices": [{
                        "index": 0,
                        "message": {"role": "assistant", "content": "Hello!"},
                        "finish_reason": "stop"
                    }]
                }
                """

            await MockURLProtocol.setHandler { request in
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
            let client = InferenceClient(
                session: session,
                host: URL(string: "https://router.huggingface.co")!,
                userAgent: "TestClient/1.0",
                bearerToken: "test_token"
            )

            let result = try await client.chatCompletion(
                model: "test-model",
                messages: [ChatCompletion.Message(role: .user, content: .text("Hello"))]
            )

            #expect(result.id == "test-id")
        }

        @Test("Client handles 401 unauthorized error", .mockURLSession)
        func testClientHandlesUnauthorized() async throws {
            let errorResponse = """
                {
                    "error": "Unauthorized"
                }
                """

            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 401,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(errorResponse.utf8))
            }

            let client = createMockClient()
            let messages = [ChatCompletion.Message(role: .user, content: .text("Hello"))]

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.chatCompletion(
                    model: "test-model",
                    messages: messages
                )
            }
        }

        @Test("Client handles 404 not found error", .mockURLSession)
        func testClientHandlesNotFound() async throws {
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
            let messages = [ChatCompletion.Message(role: .user, content: .text("Hello"))]

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.chatCompletion(
                    model: "nonexistent-model",
                    messages: messages
                )
            }
        }

        @Test("Client handles 500 server error", .mockURLSession)
        func testClientHandlesServerError() async throws {
            let errorResponse = """
                {
                    "error": "Internal server error"
                }
                """

            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 500,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(errorResponse.utf8))
            }

            let client = createMockClient()
            let messages = [ChatCompletion.Message(role: .user, content: .text("Hello"))]

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.chatCompletion(
                    model: "test-model",
                    messages: messages
                )
            }
        }

        @Test("Client handles invalid JSON response", .mockURLSession)
        func testClientHandlesInvalidJSON() async throws {
            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data("invalid json".utf8))
            }

            let client = createMockClient()
            let messages = [ChatCompletion.Message(role: .user, content: .text("Hello"))]

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.chatCompletion(
                    model: "test-model",
                    messages: messages
                )
            }
        }

        @Test("Client handles empty response body", .mockURLSession)
        func testClientHandlesEmptyResponse() async throws {
            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data())
            }

            let client = createMockClient()
            let messages = [ChatCompletion.Message(role: .user, content: .text("Hello"))]

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.chatCompletion(
                    model: "test-model",
                    messages: messages
                )
            }
        }

        @Test("Client handles network error", .mockURLSession)
        func testClientHandlesNetworkError() async throws {
            await MockURLProtocol.setHandler { request in
                throw URLError(.notConnectedToInternet)
            }

            let client = createMockClient()
            let messages = [ChatCompletion.Message(role: .user, content: .text("Hello"))]

            await #expect(throws: URLError.self) {
                _ = try await client.chatCompletion(
                    model: "test-model",
                    messages: messages
                )
            }
        }

        @Test("Client validates response type", .mockURLSession)
        func testClientValidatesResponseType() async throws {
            await MockURLProtocol.setHandler { request in
                // Return a non-HTTP response by creating an invalid HTTPURLResponse
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: nil,  // This will make it invalid
                    headerFields: nil
                )!

                return (response, Data())
            }

            let client = createMockClient()
            let messages = [ChatCompletion.Message(role: .user, content: .text("Hello"))]

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.chatCompletion(
                    model: "test-model",
                    messages: messages
                )
            }
        }
    }

#endif  // swift(>=6.1)
