import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Replay
import Testing
#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("OAuth Client Tests", .serialized)
    struct OAuthClientTests {
        @Test("OAuthClient can be initialized with valid configuration")
        func testOAuthClientInitialization() async throws {
            let config = OAuthClientConfiguration(
                baseURL: URL(string: "https://huggingface.co")!,
                redirectURL: URL(string: "myapp://oauth/callback")!,
                clientID: "test_client_id",
                scope: "openid profile"
            )

            let client = OAuthClient(configuration: config)
            let configuration = await client.configuration
            #expect(configuration.baseURL == config.baseURL)
            #expect(configuration.clientID == config.clientID)
        }

        @Test("OAuthClient generates valid authorization URL")
        func testAuthorizationURLGeneration() async throws {
            let config = OAuthClientConfiguration(
                baseURL: URL(string: "https://huggingface.co")!,
                redirectURL: URL(string: "myapp://oauth/callback")!,
                clientID: "test_client_id",
                scope: "openid profile"
            )

            let client = OAuthClient(configuration: config)

            // Mock the authenticate method to capture the generated URL
            let authURL = try await client.authenticate { url, scheme in
                // Verify the URL components
                let components = URLComponents(url: url, resolvingAgainstBaseURL: false)
                #expect(
                    components?.queryItems?.contains { $0.name == "client_id" && $0.value == "test_client_id" } == true
                )
                #expect(
                    components?.queryItems?.contains {
                        $0.name == "redirect_uri" && $0.value == "myapp://oauth/callback"
                    }
                        == true
                )
                #expect(components?.queryItems?.contains { $0.name == "response_type" && $0.value == "code" } == true)
                #expect(components?.queryItems?.contains { $0.name == "scope" && $0.value == "openid profile" } == true)
                #expect(components?.queryItems?.contains { $0.name == "code_challenge" } == true)
                #expect(
                    components?.queryItems?.contains { $0.name == "code_challenge_method" && $0.value == "S256" }
                        == true
                )
                #expect(components?.queryItems?.contains { $0.name == "state" } == true)

                return "mock_auth_code"
            }

            #expect(authURL == "mock_auth_code")
        }

        @Test(
            "OAuthClient handles token exchange with mocked response",
            .replay(
                stubs: [
                    .post(
                        "https://huggingface.co/oauth/token",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "access_token": "mock_access_token",
                            "refresh_token": "mock_refresh_token",
                            "expires_in": 3600,
                            "token_type": "Bearer"
                        }
                        """
                    }
                ]
            )
        )
        func testTokenExchange() async throws {
            let config = OAuthClientConfiguration(
                baseURL: URL(string: "https://huggingface.co")!,
                redirectURL: URL(string: "myapp://oauth/callback")!,
                clientID: "test_client_id",
                scope: "openid profile"
            )

            // Create a custom URLSession that uses the mock protocol
            let sessionConfig = URLSessionConfiguration.ephemeral
            sessionConfig.protocolClasses = [PlaybackURLProtocol.self]
            let mockSession = URLSession(configuration: sessionConfig)

            let client = OAuthClient(configuration: config, session: mockSession)

            // First authenticate to set up the code verifier
            _ = try await client.authenticate { _, _ in
                return "mock_auth_code"
            }

            // Then exchange the code for a token
            let token = try await client.exchangeCode("mock_auth_code")

            #expect(token.accessToken == "mock_access_token")
            #expect(token.refreshToken == "mock_refresh_token")
            #expect(token.isValid == true)
        }

        @Test(
            "OAuthClient handles token refresh with mocked response",
            .replay(
                stubs: [
                    .post(
                        "https://huggingface.co/oauth/token",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "access_token": "new_access_token",
                            "refresh_token": "new_refresh_token",
                            "expires_in": 3600,
                            "token_type": "Bearer"
                        }
                        """
                    }
                ]
            )
        )
        func testTokenRefresh() async throws {
            let config = OAuthClientConfiguration(
                baseURL: URL(string: "https://huggingface.co")!,
                redirectURL: URL(string: "myapp://oauth/callback")!,
                clientID: "test_client_id",
                scope: "openid profile"
            )

            // Create a custom URLSession that uses the mock protocol
            let sessionConfig = URLSessionConfiguration.ephemeral
            sessionConfig.protocolClasses = [PlaybackURLProtocol.self]
            let mockSession = URLSession(configuration: sessionConfig)

            let client = OAuthClient(configuration: config, session: mockSession)

            // Test token refresh
            let newToken = try await client.refreshToken(using: "mock_refresh_token")

            #expect(newToken.accessToken == "new_access_token")
            #expect(newToken.refreshToken == "new_refresh_token")
            #expect(newToken.isValid == true)
        }

        @Test(
            "OAuthClient handles token exchange failure",
            .replay(
                stubs: [
                    .post(
                        "https://huggingface.co/oauth/token",
                        400,
                        ["Content-Type": "application/json"]
                    ) {
                        ""
                    }
                ]
            )
        )
        func testTokenExchangeFailure() async throws {
            let config = OAuthClientConfiguration(
                baseURL: URL(string: "https://huggingface.co")!,
                redirectURL: URL(string: "myapp://oauth/callback")!,
                clientID: "test_client_id",
                scope: "openid profile"
            )

            // Create a custom URLSession that uses the mock protocol
            let sessionConfig = URLSessionConfiguration.ephemeral
            sessionConfig.protocolClasses = [PlaybackURLProtocol.self]
            let mockSession = URLSession(configuration: sessionConfig)

            let client = OAuthClient(configuration: config, session: mockSession)

            // First authenticate to set up the code verifier
            _ = try await client.authenticate { _, _ in
                return "mock_auth_code"
            }

            // Test that token exchange throws error
            await #expect(throws: OAuthError.tokenExchangeFailed) {
                try await client.exchangeCode("mock_auth_code")
            }
        }

        @Test("OAuthClient handles missing code verifier")
        func testMissingCodeVerifier() async throws {
            let config = OAuthClientConfiguration(
                baseURL: URL(string: "https://huggingface.co")!,
                redirectURL: URL(string: "myapp://oauth/callback")!,
                clientID: "test_client_id",
                scope: "openid profile"
            )

            let client = OAuthClient(configuration: config)

            // Test that exchangeCode throws error when no code verifier is set
            await #expect(throws: OAuthError.missingCodeVerifier) {
                try await client.exchangeCode("mock_auth_code")
            }
        }

        @Test("OAuthToken validates expiration correctly")
        func testTokenValidation() {
            let now = Date()
            let validToken = OAuthToken(
                accessToken: "test_token",
                refreshToken: "test_refresh",
                expiresAt: now.addingTimeInterval(3600)  // 1 hour from now
            )

            let expiredToken = OAuthToken(
                accessToken: "test_token",
                refreshToken: "test_refresh",
                expiresAt: now.addingTimeInterval(-3600)  // 1 hour ago
            )

            #expect(validToken.isValid == true)
            #expect(expiredToken.isValid == false)
        }
    }
#endif  // swift(>=6.1)
