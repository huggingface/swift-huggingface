#if canImport(AuthenticationServices)
    import AuthenticationServices
    import Observation

    /// A manager for handling Hugging Face OAuth authentication.
    ///
    /// - SeeAlso: [Hugging Face OAuth Documentation](https://huggingface.co/docs/api-inference/authentication)
    @available(macOS 14.0, macCatalyst 17.0, iOS 17.0, watchOS 10.0, tvOS 17.0, *)
    @Observable
    @MainActor
    public final class HuggingFaceAuthenticationManager: Sendable {
        /// The default base URL for Hugging Face OAuth endpoints
        public static let defaultBaseURL = URL(string: "https://huggingface.co")!

        /// Whether the user is authenticated.
        public var isAuthenticated = false

        /// The authentication token.
        public var authToken: OAuthToken?

        let oauthClient: OAuthClient
        let tokenStorage: TokenStorage

        /// Initializes a new authentication manager with the specified configuration and token storage.
        /// - Parameters:
        ///   - configuration: The OAuth configuration containing client credentials and endpoints.
        ///   - tokenStorage: The token storage to use for storing and retrieving tokens.
        /// - Returns: A new authentication manager.
        public init(client: OAuthClient, tokenStorage: TokenStorage) {
            self.oauthClient = client
            self.tokenStorage = tokenStorage

            // Try to load existing token
            Task {
                await loadStoredToken()
            }
        }

        /// Initializes a new authentication manager with the specified configuration.
        /// - Parameters:
        ///   - baseURL: The base URL of the OAuth provider. Defaults to HuggingFace OAuth endpoint.
        ///   - clientID: The client ID.
        ///   - redirectURL: The redirect URL.
        ///   - scope: The scopes to request.
        ///   - keychainService: The service name for the keychain.
        ///   - keychainAccount: The account name for the keychain.
        /// - Throws: `OAuthError.invalidConfiguration` if any parameter is invalid.
        public convenience init(
            baseURL: URL = HuggingFaceAuthenticationManager.defaultBaseURL,
            clientID: String,
            redirectURL: URL,
            scope: Set<Scope>,
            keychainService: String,
            keychainAccount: String
        ) throws {
            // Validate parameters at call site
            guard !clientID.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
                throw OAuthError.invalidConfiguration("Client ID cannot be empty")
            }

            guard !scope.isEmpty else {
                throw OAuthError.invalidConfiguration("Scope cannot be empty")
            }

            let configuration = OAuthClientConfiguration(
                baseURL: baseURL,
                redirectURL: redirectURL,
                clientID: clientID,
                scope: scope.map { $0.rawValue }.sorted().joined(separator: " ")
            )

            self.init(
                client: OAuthClient(configuration: configuration),
                tokenStorage: .keychain(service: keychainService, account: keychainAccount)
            )
        }

        /// Signs the user in.
        public func signIn() async throws {
            let code = try await oauthClient.authenticate { @Sendable url, scheme in
                return try await withCheckedThrowingContinuation { continuation in
                    let authSession = ASWebAuthenticationSession(
                        url: url,
                        callbackURLScheme: scheme
                    ) { callbackURL, error in
                        if let error = error {
                            continuation.resume(throwing: error)
                            return
                        }

                        guard let url = callbackURL else {
                            continuation.resume(throwing: OAuthError.invalidCallback)
                            return
                        }

                        guard let code = url.oauthCode else {
                            continuation.resume(throwing: OAuthError.invalidCallback)
                            return
                        }

                        continuation.resume(returning: code)
                    }

                    Task { @MainActor in
                        authSession.prefersEphemeralWebBrowserSession = false
                        authSession.presentationContextProvider =
                            HuggingFaceAuthenticationPresentationContextProvider.shared

                        if !authSession.start() {
                            continuation.resume(throwing: OAuthError.sessionFailedToStart)
                        }
                    }
                }
            }

            let token = try await oauthClient.exchangeCode(code)
            try tokenStorage.store(token)
            self.authToken = token
            self.isAuthenticated = true
        }

        /// Signs the user out.
        public func signOut() async {
            do {
                try tokenStorage.delete()
            } catch {
                // Log error but don't throw - sign out should always succeed
                // In a production app, you might want to use a proper logging framework
                print("Token storage deletion error: \(error)")
            }

            self.isAuthenticated = false
            self.authToken = nil
        }

        /// Gets a valid token.
        /// - Returns: The valid token.
        /// - Throws: `OAuthError.authenticationRequired` if no valid token is available.
        public func getValidToken() async throws -> String {
            if let token = authToken, token.isValid {
                return token.accessToken
            }

            // Token expired, try refresh
            guard let token = authToken,
                let refreshToken = token.refreshToken
            else {
                throw OAuthError.authenticationRequired
            }

            do {
                let newToken = try await oauthClient.refreshToken(using: refreshToken)
                try tokenStorage.store(newToken)
                self.authToken = newToken
                return newToken.accessToken
            } catch {
                // Refresh failed, require re-authentication
                self.isAuthenticated = false
                throw OAuthError.authenticationRequired
            }
        }

        private func loadStoredToken() async {
            do {
                guard let token = try tokenStorage.retrieve(),
                    token.isValid
                else {
                    return
                }

                self.authToken = token
                self.isAuthenticated = true
            } catch {
                // Log error and attempt to clear invalid token from storage
                print("Failed to load stored token: \(error)")
                do {
                    try tokenStorage.delete()
                } catch {
                    // If deletion also fails, log but don't throw
                    print("Failed to clear invalid token from storage: \(error)")
                }
            }
        }
    }

    // MARK: -

    @available(macOS 14.0, macCatalyst 17.0, iOS 17.0, watchOS 10.0, tvOS 17.0, *)
    extension HuggingFaceAuthenticationManager {
        /// OAuth scopes supported by HuggingFace
        public enum Scope: Hashable, Sendable {
            /// Get the ID token in addition to the access token
            case openid

            /// Get the user's profile information (username, avatar, etc.)
            case profile

            /// Get the user's email address
            case email

            /// Know whether the user has a payment method set up
            case readBilling

            /// Get read access to the user's personal repos
            case readRepos

            /// Get write/read access to the user's personal repos
            case writeRepos

            /// Get full access to the user's personal repos. Also grants repo creation and deletion
            case manageRepos

            /// Get access to the Inference API, you will be able to make inference requests on behalf of the user
            case inferenceAPI

            /// Open discussions and Pull Requests on behalf of the user as well as interact with discussions
            case writeDiscussions

            /// A custom or unknown scope
            case other(String)

            /// Human-readable description of the scope
            public var description: String {
                switch self {
                case .openid:
                    return "Get the ID token in addition to the access token"
                case .profile:
                    return "Get the user's profile information (username, avatar, etc.)"
                case .email:
                    return "Get the user's email address"
                case .readBilling:
                    return "Know whether the user has a payment method set up"
                case .readRepos:
                    return "Get read access to the user's personal repos"
                case .writeRepos:
                    return "Get write/read access to the user's personal repos"
                case .manageRepos:
                    return "Get full access to the user's personal repos. Also grants repo creation and deletion"
                case .inferenceAPI:
                    return
                        "Get access to the Inference API, you will be able to make inference requests on behalf of the user"
                case .writeDiscussions:
                    return
                        "Open discussions and Pull Requests on behalf of the user as well as interact with discussions"
                case .other(let value):
                    return value
                }
            }
        }
    }

    @available(macOS 14.0, macCatalyst 17.0, iOS 17.0, watchOS 10.0, tvOS 17.0, *)
    extension HuggingFaceAuthenticationManager.Scope: RawRepresentable {
        public init(rawValue: String) {
            switch rawValue {
            case "openid":
                self = .openid
            case "profile":
                self = .profile
            case "email":
                self = .email
            case "read-billing":
                self = .readBilling
            case "read-repos":
                self = .readRepos
            case "write-repos":
                self = .writeRepos
            case "manage-repos":
                self = .manageRepos
            case "inference-api":
                self = .inferenceAPI
            case "write-discussions":
                self = .writeDiscussions
            default:
                self = .other(rawValue)
            }
        }

        public var rawValue: String {
            switch self {
            case .openid:
                return "openid"
            case .profile:
                return "profile"
            case .email:
                return "email"
            case .readBilling:
                return "read-billing"
            case .readRepos:
                return "read-repos"
            case .writeRepos:
                return "write-repos"
            case .manageRepos:
                return "manage-repos"
            case .inferenceAPI:
                return "inference-api"
            case .writeDiscussions:
                return "write-discussions"
            case .other(let value):
                return value
            }
        }
    }

    @available(macOS 14.0, macCatalyst 17.0, iOS 17.0, watchOS 10.0, tvOS 17.0, *)
    extension HuggingFaceAuthenticationManager.Scope: Codable {
        public init(from decoder: Decoder) throws {
            let container = try decoder.singleValueContainer()
            let rawValue = try container.decode(String.self)
            self.init(rawValue: rawValue)
        }

        public func encode(to encoder: Encoder) throws {
            var container = encoder.singleValueContainer()
            try container.encode(rawValue)
        }
    }

    @available(macOS 14.0, macCatalyst 17.0, iOS 17.0, watchOS 10.0, tvOS 17.0, *)
    extension HuggingFaceAuthenticationManager.Scope: ExpressibleByStringLiteral {
        public init(stringLiteral value: String) {
            self = Self(rawValue: value)
        }
    }

    @available(macOS 14.0, macCatalyst 17.0, iOS 17.0, watchOS 10.0, tvOS 17.0, *)
    extension Set<HuggingFaceAuthenticationManager.Scope> {
        public static var basic: Self { [.openid, .profile, .email] }
        public static var readAccess: Self { [.openid, .profile, .email, .readRepos] }
        public static var writeAccess: Self { [.openid, .profile, .email, .writeRepos] }
        public static var fullAccess: Self { [.openid, .profile, .email, .manageRepos, .inferenceAPI] }
        public static var inferenceOnly: Self { [.openid, .inferenceAPI] }
        public static var discussions: Self { [.openid, .profile, .email, .writeDiscussions] }
    }

    // MARK: -

    @available(macOS 14.0, macCatalyst 17.0, iOS 17.0, watchOS 10.0, tvOS 17.0, *)
    extension HuggingFaceAuthenticationManager {
        /// A mechanism for storing and retrieving OAuth tokens.
        public struct TokenStorage: Sendable {
            /// A function for storing an OAuth token.
            public var store: @Sendable (OAuthToken) throws -> Void

            /// A function for retrieving an OAuth token.
            public var retrieve: @Sendable () throws -> OAuthToken?

            /// A function for deleting an OAuth token.
            public var delete: @Sendable () throws -> Void

            public init(
                store: @escaping @Sendable (OAuthToken) throws -> Void,
                retrieve: @escaping @Sendable () throws -> OAuthToken?,
                delete: @escaping @Sendable () throws -> Void
            ) {
                self.store = store
                self.retrieve = retrieve
                self.delete = delete
            }

            /// A mechanism for storing and retrieving OAuth tokens using the keychain.
            /// - Parameters:
            ///   - service: The service name for the keychain item.
            ///   - account: The account name for the keychain item.
            /// - Returns: A new token storage mechanism.
            public static func keychain(service: String, account: String) -> TokenStorage {
                return TokenStorage(
                    store: { token in
                        let encoder = JSONEncoder()
                        let data = try encoder.encode(token)

                        let query: [String: Any] = [
                            kSecClass as String: kSecClassGenericPassword,
                            kSecAttrService as String: service,
                            kSecAttrAccount as String: account,
                            kSecValueData as String: data,
                            kSecAttrAccessible as String: kSecAttrAccessibleAfterFirstUnlock,
                        ]

                        // Delete existing item if present
                        SecItemDelete(query as CFDictionary)

                        let status = SecItemAdd(query as CFDictionary, nil)
                        guard status == errSecSuccess else {
                            throw OAuthError.tokenStorageError("Keychain storage error: \(status)")
                        }
                    },
                    retrieve: {
                        let query: [String: Any] = [
                            kSecClass as String: kSecClassGenericPassword,
                            kSecAttrService as String: service,
                            kSecAttrAccount as String: account,
                            kSecReturnData as String: true,
                            kSecMatchLimit as String: kSecMatchLimitOne,
                        ]

                        var item: CFTypeRef?
                        let status = SecItemCopyMatching(query as CFDictionary, &item)

                        guard status != errSecItemNotFound else {
                            return nil
                        }

                        guard status == errSecSuccess,
                            let data = item as? Data
                        else {
                            throw OAuthError.tokenStorageError("Keychain retrieval error: \(status)")
                        }

                        let decoder = JSONDecoder()
                        return try decoder.decode(OAuthToken.self, from: data)
                    },
                    delete: {
                        let query: [String: Any] = [
                            kSecClass as String: kSecClassGenericPassword,
                            kSecAttrService as String: service,
                            kSecAttrAccount as String: account,
                        ]

                        let status = SecItemDelete(query as CFDictionary)
                        guard status == errSecSuccess || status == errSecItemNotFound else {
                            throw OAuthError.tokenStorageError("Keychain deletion error: \(status)")
                        }
                    }
                )
            }
        }
    }
#endif  // canImport(AuthenticationServices)

// MARK: -

#if canImport(AppKit) && canImport(AuthenticationServices)
    import AppKit

    @MainActor
    public final class HuggingFaceAuthenticationPresentationContextProvider: NSObject,
        ASWebAuthenticationPresentationContextProviding
    {
        /// A shared instance of the presentation context provider.
        public static let shared: HuggingFaceAuthenticationPresentationContextProvider = .init()

        /// Returns the presentation anchor for the given authentication session.
        public func presentationAnchor(for session: ASWebAuthenticationSession) -> ASPresentationAnchor {
            // Return the first window or the key window, or a default anchor if no windows are available.
            NSApp.keyWindow ?? NSApp.windows.first ?? ASPresentationAnchor()
        }
    }
#endif  // canImport(AppKit) && canImport(AuthenticationServices)

#if canImport(UIKit) && canImport(AuthenticationServices)
    import UIKit

    @MainActor
    public final class HuggingFaceAuthenticationPresentationContextProvider: NSObject,
        ASWebAuthenticationPresentationContextProviding
    {
        /// A shared instance of the presentation context provider.
        public static let shared: HuggingFaceAuthenticationPresentationContextProvider = .init()

        /// Returns the presentation anchor for the given authentication session.
        public func presentationAnchor(for session: ASWebAuthenticationSession) -> ASPresentationAnchor {
            // Return the key window's root view controller, or the first window's root view controller
            if let keyWindow = UIApplication.shared.connectedScenes
                .compactMap({ $0 as? UIWindowScene })
                .flatMap({ $0.windows })
                .first(where: { $0.isKeyWindow })
            {
                return keyWindow
            }

            // Fallback to the first window
            if let firstWindow = UIApplication.shared.connectedScenes
                .compactMap({ $0 as? UIWindowScene })
                .flatMap({ $0.windows })
                .first
            {
                return firstWindow
            }

            // Last resort - return a default anchor
            return ASPresentationAnchor()
        }
    }
#endif  // canImport(UIKit) && canImport(AuthenticationServices)

// MARK: -

private extension URL {
    /// Extracts the OAuth authorization code from a callback URL.
    /// - Returns: The authorization code if found, nil otherwise.
    var oauthCode: String? {
        URLComponents(string: absoluteString)?
            .queryItems?
            .first(where: { $0.name == "code" })?
            .value
    }
}
