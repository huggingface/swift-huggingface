import Foundation
import Testing

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("User Tests", .serialized)
    struct UserTests {
        /// Helper to create a URL session with mock protocol handlers
        func createMockClient(bearerToken: String? = nil) -> HubClient {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [MockURLProtocol.self]
            let session = URLSession(configuration: configuration)
            return HubClient(
                session: session,
                host: URL(string: "https://huggingface.co")!,
                userAgent: "TestClient/1.0",
                bearerToken: bearerToken
            )
        }

        @Test("Get authenticated user info (whoami)", .mockURLSession)
        func testWhoami() async throws {
            let mockResponse = """
                {
                    "name": "johndoe",
                    "fullname": "John Doe",
                    "email": "john@example.com",
                    "avatarUrl": "https://avatars.example.com/johndoe",
                    "isPro": true,
                    "orgs": [
                        {
                            "name": "myorg",
                            "fullname": "My Organization",
                            "avatarUrl": "https://avatars.example.com/myorg"
                        }
                    ]
                }
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/whoami-v2")
                #expect(request.httpMethod == "GET")
                #expect(request.value(forHTTPHeaderField: "Authorization") == "Bearer test_token")

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient(bearerToken: "test_token")
            let user = try await client.whoami()

            #expect(user.name == "johndoe")
            #expect(user.fullName == "John Doe")
            #expect(user.email == "john@example.com")
            #expect(user.isPro == true)
            #expect(user.organizations?.count == 1)
            #expect(user.organizations?[0].name == "myorg")
        }

        @Test("Whoami requires authentication", .mockURLSession)
        func testWhoamiRequiresAuth() async throws {
            let errorResponse = """
                {
                    "error": "Unauthorized"
                }
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/whoami-v2")
                // Verify no authorization header
                #expect(request.value(forHTTPHeaderField: "Authorization") == nil)

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 401,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(errorResponse.utf8))
            }

            let client = createMockClient()  // No bearer token

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.whoami()
            }
        }

        @Test("Whoami with invalid token", .mockURLSession)
        func testWhoamiWithInvalidToken() async throws {
            let errorResponse = """
                {
                    "error": "Invalid token"
                }
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.value(forHTTPHeaderField: "Authorization") == "Bearer invalid_token")

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 403,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(errorResponse.utf8))
            }

            let client = createMockClient(bearerToken: "invalid_token")

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.whoami()
            }
        }
    }

#endif  // swift(>=6.1)
