import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Replay
import Testing

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("User Tests", .serialized)
    struct UserTests {
        private func createClient(bearerToken: String? = nil) -> HubClient {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [PlaybackURLProtocol.self]
            let session = URLSession(configuration: configuration)

            return HubClient(
                session: session,
                host: URL(string: "https://huggingface.co")!,
                userAgent: "TestClient/1.0",
                bearerToken: bearerToken,
                cache: nil
            )
        }

        @Test(
            "Get authenticated user info (whoami)",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/whoami-v2",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
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
                    }
                ]
            )
        )
        func testWhoami() async throws {
            let client = createClient(bearerToken: "test_token")
            let user = try await client.whoami()

            #expect(user.name == "johndoe")
            #expect(user.fullName == "John Doe")
            #expect(user.email == "john@example.com")
            #expect(user.isPro == true)
            #expect(user.organizations?.count == 1)
            #expect(user.organizations?[0].name == "myorg")
        }

        @Test(
            "Whoami requires authentication",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/whoami-v2",
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
        func testWhoamiRequiresAuth() async throws {
            let client = createClient()

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.whoami()
            }
        }

        @Test(
            "Whoami with invalid token",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/whoami-v2",
                        403,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "error": "Invalid token"
                        }
                        """
                    }
                ]
            )
        )
        func testWhoamiWithInvalidToken() async throws {
            let client = createClient(bearerToken: "invalid_token")

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.whoami()
            }
        }
    }

#endif  // swift(>=6.1)
