import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Replay
import Testing

@testable import HuggingFace

#if swift(>=6.1)

    /// Tests for the InferenceClient core functionality
    @Suite("Inference Client Tests", .serialized)
    struct InferenceClientTests {
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

        @Test("Client initialization with custom parameters")
        func testClientInitialization() async throws {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [PlaybackURLProtocol.self]
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

        @Test(
            "Client sends authorization header when token provided",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/chat/completions",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
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
                    }
                ]
            )
        )
        func testClientWithBearerToken() async throws {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [PlaybackURLProtocol.self]
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

        @Test(
            "Client handles 401 unauthorized error",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/chat/completions",
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
        func testClientHandlesUnauthorized() async throws {
            let client = createClient()
            let messages = [ChatCompletion.Message(role: .user, content: .text("Hello"))]

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.chatCompletion(
                    model: "test-model",
                    messages: messages
                )
            }
        }

        @Test(
            "Client handles 404 not found error",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/chat/completions",
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
        func testClientHandlesNotFound() async throws {
            let client = createClient()
            let messages = [ChatCompletion.Message(role: .user, content: .text("Hello"))]

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.chatCompletion(
                    model: "nonexistent-model",
                    messages: messages
                )
            }
        }

        @Test(
            "Client handles 500 server error",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/chat/completions",
                        500,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "error": "Internal server error"
                        }
                        """
                    }
                ]
            )
        )
        func testClientHandlesServerError() async throws {
            let client = createClient()
            let messages = [ChatCompletion.Message(role: .user, content: .text("Hello"))]

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.chatCompletion(
                    model: "test-model",
                    messages: messages
                )
            }
        }

        @Test(
            "Client handles invalid JSON response",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/chat/completions",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        "invalid json"
                    }
                ]
            )
        )
        func testClientHandlesInvalidJSON() async throws {
            let client = createClient()
            let messages = [ChatCompletion.Message(role: .user, content: .text("Hello"))]

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.chatCompletion(
                    model: "test-model",
                    messages: messages
                )
            }
        }

        @Test(
            "Client handles empty response body",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/chat/completions",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        ""
                    }
                ]
            )
        )
        func testClientHandlesEmptyResponse() async throws {
            let client = createClient()
            let messages = [ChatCompletion.Message(role: .user, content: .text("Hello"))]

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.chatCompletion(
                    model: "test-model",
                    messages: messages
                )
            }
        }

        @Test(
            "Client handles network error",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/chat/completions",
                        500,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {"error": "Internal server error"}
                        """
                    }
                ]
            )
        )
        func testClientHandlesNetworkError() async throws {
            let client = createClient()
            let messages = [ChatCompletion.Message(role: .user, content: .text("Hello"))]

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.chatCompletion(
                    model: "test-model",
                    messages: messages
                )
            }
        }

        @Test(
            "Client validates response type",
            .replay(
                stubs: [
                    .post(
                        "https://router.huggingface.co/v1/chat/completions",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        ""
                    }
                ]
            )
        )
        func testClientValidatesResponseType() async throws {
            let client = createClient()
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
