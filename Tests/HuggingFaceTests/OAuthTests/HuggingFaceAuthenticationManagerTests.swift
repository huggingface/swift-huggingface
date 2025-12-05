import Foundation
import Testing

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("HuggingFace Authentication Manager Tests")
    struct HuggingFaceAuthenticationManagerTests {
        @Test("HuggingFaceAuthenticationManager can be initialized with valid parameters")
        @MainActor
        func testManagerInitialization() async throws {
            let manager = try HuggingFaceAuthenticationManager(
                clientID: "test_client_id",
                redirectURL: URL(string: "myapp://oauth/callback")!,
                scope: [.openid, .profile],
                keychainService: "test_service",
                keychainAccount: "test_account"
            )

            #expect(manager.isAuthenticated == false)
            #expect(manager.authToken == nil)
            let configuration = await manager.oauthClient.configuration
            #expect(configuration.baseURL == HuggingFaceAuthenticationManager.defaultBaseURL)
            #expect(configuration.clientID == "test_client_id")
            #expect(configuration.redirectURL == URL(string: "myapp://oauth/callback")!)
            #expect(configuration.scope == "openid profile")
        }

        @Test("HuggingFaceAuthenticationManager validates input parameters")
        @MainActor
        func testManagerValidation() async throws {
            // Test valid initialization
            let validManager = try HuggingFaceAuthenticationManager(
                clientID: "test_client_id",
                redirectURL: URL(string: "myapp://oauth/callback")!,
                scope: [.openid, .profile],
                keychainService: "test_service",
                keychainAccount: "test_account"
            )
            #expect(validManager.isAuthenticated == false)

            // Test invalid client ID
            #expect(throws: OAuthError.invalidConfiguration("Client ID cannot be empty")) {
                try HuggingFaceAuthenticationManager(
                    clientID: "",
                    redirectURL: URL(string: "myapp://oauth/callback")!,
                    scope: [.openid, .profile],
                    keychainService: "test_service",
                    keychainAccount: "test_account"
                )
            }
        }

        @Test("HuggingFaceAuthenticationManager sign out clears state")
        @MainActor
        func testSignOut() async throws {
            let manager = try HuggingFaceAuthenticationManager(
                clientID: "test_client_id",
                redirectURL: URL(string: "myapp://oauth/callback")!,
                scope: [.openid, .profile],
                keychainService: "test_service",
                keychainAccount: "test_account"
            )

            // Initially not authenticated
            #expect(manager.isAuthenticated == false)
            #expect(manager.authToken == nil)

            // Sign out should not throw and should maintain unauthenticated state
            await manager.signOut()

            #expect(manager.isAuthenticated == false)
            #expect(manager.authToken == nil)
        }

        @Test("HuggingFaceAuthenticationManager getValidToken throws when not authenticated")
        @MainActor
        func testGetValidTokenWhenNotAuthenticated() async throws {
            let manager = try HuggingFaceAuthenticationManager(
                clientID: "test_client_id",
                redirectURL: URL(string: "myapp://oauth/callback")!,
                scope: [.openid, .profile],
                keychainService: "test_service",
                keychainAccount: "test_account"
            )

            await #expect(throws: OAuthError.authenticationRequired) {
                try await manager.getValidToken()
            }
        }
    }

    @Suite("Hugging Face OAuth Scope Tests", .serialized)
    struct HuggingFaceScopeTests {
        typealias Scope = HuggingFaceAuthenticationManager.Scope

        @Test("OAuth Scope sets work correctly")
        func testScopeSets() {
            // Test basic scope set
            let basicScopes = Set<Scope>.basic
            #expect(basicScopes.contains(.openid))
            #expect(basicScopes.contains(.profile))
            #expect(basicScopes.contains(.email))

            // Test read access scope set
            let readScopes = Set<Scope>.readAccess
            #expect(readScopes.contains(.readRepos))

            // Test write access scope set
            let writeScopes = Set<Scope>.writeAccess
            #expect(writeScopes.contains(.writeRepos))

            // Test full access scope set
            let fullScopes = Set<Scope>.fullAccess
            #expect(fullScopes.contains(.manageRepos))
            #expect(fullScopes.contains(.inferenceAPI))

            // Test inference only scope set
            let inferenceScopes = Set<Scope>.inferenceOnly
            #expect(inferenceScopes.contains(.openid))
            #expect(inferenceScopes.contains(.inferenceAPI))

            // Test discussions scope set
            let discussionScopes = Set<Scope>.discussions
            #expect(discussionScopes.contains(.writeDiscussions))
        }

        @Test("OAuth Scope raw values are correct")
        func testScopeRawValues() {
            #expect(Scope.openid.rawValue == "openid")
            #expect(Scope.profile.rawValue == "profile")
            #expect(Scope.email.rawValue == "email")
            #expect(Scope.readBilling.rawValue == "read-billing")
            #expect(Scope.readRepos.rawValue == "read-repos")
            #expect(Scope.writeRepos.rawValue == "write-repos")
            #expect(Scope.manageRepos.rawValue == "manage-repos")
            #expect(Scope.inferenceAPI.rawValue == "inference-api")
            #expect(Scope.writeDiscussions.rawValue == "write-discussions")

            // Test custom scope
            let customScope = Scope.other("custom-scope")
            #expect(customScope.rawValue == "custom-scope")
        }

        @Test("OAuth Scope initialization from raw values")
        func testScopeInitializationFromRawValue() {
            #expect(Scope(rawValue: "openid") == .openid)
            #expect(Scope(rawValue: "profile") == .profile)
            #expect(Scope(rawValue: "email") == .email)
            #expect(Scope(rawValue: "read-billing") == .readBilling)
            #expect(Scope(rawValue: "read-repos") == .readRepos)
            #expect(Scope(rawValue: "write-repos") == .writeRepos)
            #expect(Scope(rawValue: "manage-repos") == .manageRepos)
            #expect(Scope(rawValue: "inference-api") == .inferenceAPI)
            #expect(Scope(rawValue: "write-discussions") == .writeDiscussions)

            // Test custom scope
            let customScope = Scope(rawValue: "custom-scope")
            #expect(customScope == .other("custom-scope"))
        }

        @Test("OAuth Scope descriptions are correct")
        func testScopeDescriptions() {
            #expect(Scope.openid.description.contains("ID token"))
            #expect(Scope.profile.description.contains("profile information"))
            #expect(Scope.email.description.contains("email address"))
            #expect(Scope.readBilling.description.contains("payment method"))
            #expect(Scope.readRepos.description.contains("read access"))
            #expect(Scope.writeRepos.description.contains("write/read access"))
            #expect(Scope.manageRepos.description.contains("full access"))
            #expect(Scope.inferenceAPI.description.contains("Inference API"))
            #expect(Scope.writeDiscussions.description.contains("discussions"))

            // Test custom scope description
            let customScope = Scope.other("custom-scope")
            #expect(customScope.description == "custom-scope")
        }

        @Test("OAuth Scope string literal support")
        func testScopeStringLiteral() {
            let scope: Scope = "openid"
            #expect(scope == .openid)

            let customScope: Scope = "custom-scope"
            #expect(customScope == .other("custom-scope"))
        }
    }
#endif  // swift(>=6.1)
