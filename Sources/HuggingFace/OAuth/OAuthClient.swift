import Crypto
import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

/// An OAuth 2.0 client for handling authentication flows
/// with support for token caching, refresh, and secure code exchange
/// using PKCE (Proof Key for Code Exchange).
public actor OAuthClient: Sendable {
    /// The OAuth client configuration.
    public let configuration: OAuthClientConfiguration

    /// The URL session to use for network requests.
    let urlSession: URLSession

    private var cachedToken: OAuthToken?
    private var refreshTask: Task<OAuthToken, Error>?
    private var codeVerifier: String?

    /// Initializes a new OAuth client with the specified configuration.
    /// - Parameters:
    ///   - configuration: The OAuth configuration containing client credentials and endpoints.
    ///   - session: The URL session to use for network requests. Defaults to `.shared`.
    public init(configuration: OAuthClientConfiguration, session: URLSession = .shared) {
        self.configuration = configuration
        self.urlSession = session
    }

    /// Retrieves a valid OAuth token, using cached token if available and valid.
    ///
    /// This method first checks for a valid cached token. If no valid token exists and a refresh
    /// is already in progress, it waits for that refresh to complete. If no refresh is in progress,
    /// it throws `OAuthError.authenticationRequired` to indicate that fresh authentication is needed.
    ///
    /// - Returns: A valid OAuth token.
    /// - Throws: `OAuthError.authenticationRequired` if no valid token is available and no refresh is in progress.
    public func getValidToken() async throws -> OAuthToken {
        // Return cached token if valid
        if let token = cachedToken, token.isValid {
            return token
        }

        // If refresh already in progress, wait for it
        if let task = refreshTask {
            return try await task.value
        }

        // No valid token and no refresh in progress - need fresh authentication
        throw OAuthError.authenticationRequired
    }

    /// Initiates the OAuth authentication flow using PKCE (Proof Key for Code Exchange).
    ///
    /// This method generates PKCE values, constructs the authorization URL, and presents
    /// a web authentication session to the user. The user will be redirected to the OAuth
    /// provider's authorization page where they can grant permissions.
    ///
    /// - Parameter handler: A closure that handles the authentication session flow.
    /// - Returns: The authorization code from the OAuth callback.
    /// - Throws: `OAuthError.sessionFailedToStart` if the authentication session cannot be started.
    /// - Throws: `OAuthError.invalidCallback` if the callback URL is invalid or doesn't contain an authorization code.
    public func authenticate(handler: @escaping (URL, String) async throws -> String)
        async throws -> String
    {
        // Generate PKCE values
        let (verifier, challenge) = Self.generatePKCEValues()
        self.codeVerifier = verifier

        // Build authorization URL
        let authURL = configuration.baseURL.appendingPathComponent("oauth/authorize")
        var components = URLComponents(url: authURL, resolvingAgainstBaseURL: false)!
        components.queryItems = [
            .init(name: "client_id", value: configuration.clientID),
            .init(name: "redirect_uri", value: configuration.redirectURL.absoluteString),
            .init(name: "response_type", value: "code"),
            .init(name: "scope", value: configuration.scope),
            .init(name: "code_challenge", value: challenge),
            .init(name: "code_challenge_method", value: "S256"),
            .init(name: "state", value: UUID().uuidString),
        ]

        guard let finalAuthURL = components.url,
            let scheme = configuration.redirectURL.scheme
        else {
            throw OAuthError.sessionFailedToStart
        }

        return try await handler(finalAuthURL, scheme)
    }

    /// Exchanges an authorization code for an OAuth token using PKCE.
    ///
    /// This method takes the authorization code received from the OAuth callback and exchanges
    /// it for an access token and refresh token. The code verifier generated during authentication
    /// is used to complete the PKCE flow for security.
    ///
    /// - Parameter code: The authorization code from the OAuth callback.
    /// - Returns: An OAuth token containing access and refresh tokens.
    /// - Throws: `OAuthError.missingCodeVerifier` if no code verifier is available.
    /// - Throws: `OAuthError.tokenExchangeFailed` if the token exchange request fails.
    public func exchangeCode(_ code: String) async throws -> OAuthToken {
        guard let verifier = codeVerifier else {
            throw OAuthError.missingCodeVerifier
        }

        let tokenURL = configuration.baseURL.appendingPathComponent("oauth/token")
        var request = URLRequest(url: tokenURL)
        request.httpMethod = "POST"
        request.setValue("application/x-www-form-urlencoded", forHTTPHeaderField: "Content-Type")

        var components = URLComponents()
        components.queryItems = [
            .init(name: "grant_type", value: "authorization_code"),
            .init(name: "code", value: code),
            .init(name: "redirect_uri", value: configuration.redirectURL.absoluteString),
            .init(name: "client_id", value: configuration.clientID),
            .init(name: "code_verifier", value: verifier),
        ]
        request.httpBody = components.percentEncodedQuery?.data(using: .utf8)

        let (data, response) = try await urlSession.data(for: request)

        guard let httpResponse = response as? HTTPURLResponse,
            (200 ... 299).contains(httpResponse.statusCode)
        else {
            throw OAuthError.tokenExchangeFailed
        }

        let tokenResponse = try JSONDecoder().decode(TokenResponse.self, from: data)
        let token = OAuthToken(
            accessToken: tokenResponse.accessToken,
            refreshToken: tokenResponse.refreshToken,
            expiresAt: Date().addingTimeInterval(TimeInterval(tokenResponse.expiresIn))
        )

        self.cachedToken = token
        self.codeVerifier = nil

        return token
    }

    /// Refreshes an OAuth token using a refresh token.
    ///
    /// This method prevents multiple concurrent refresh operations by tracking an active refresh task.
    /// If a refresh is already in progress, it waits for that refresh to complete rather than
    /// starting a new one.
    ///
    /// - Parameter refreshToken: The refresh token to use for obtaining a new access token.
    /// - Returns: A new OAuth token with updated access and refresh tokens.
    /// - Throws: `OAuthError.tokenExchangeFailed` if the refresh request fails.
    public func refreshToken(using refreshToken: String) async throws -> OAuthToken {
        // Start refresh task if not already running
        if let task = refreshTask {
            return try await task.value
        }

        let task = Task<OAuthToken, Error> {
            try await performRefresh(refreshToken: refreshToken)
        }
        refreshTask = task

        defer {
            Task { clearRefreshTask() }
        }

        return try await task.value
    }

    private func clearRefreshTask() {
        refreshTask = nil
    }

    private func performRefresh(refreshToken: String) async throws -> OAuthToken {
        let tokenURL = configuration.baseURL.appendingPathComponent("oauth/token")
        var request = URLRequest(url: tokenURL)
        request.httpMethod = "POST"
        request.setValue("application/x-www-form-urlencoded", forHTTPHeaderField: "Content-Type")

        var components = URLComponents()
        components.queryItems = [
            .init(name: "grant_type", value: "refresh_token"),
            .init(name: "refresh_token", value: refreshToken),
            .init(name: "client_id", value: configuration.clientID),
        ]
        request.httpBody = components.percentEncodedQuery?.data(using: .utf8)

        let (data, response) = try await urlSession.data(for: request)

        guard let httpResponse = response as? HTTPURLResponse,
            (200 ... 299).contains(httpResponse.statusCode)
        else {
            throw OAuthError.tokenExchangeFailed
        }

        let tokenResponse = try JSONDecoder().decode(TokenResponse.self, from: data)
        let token = OAuthToken(
            accessToken: tokenResponse.accessToken,
            refreshToken: tokenResponse.refreshToken ?? refreshToken,
            expiresAt: Date().addingTimeInterval(TimeInterval(tokenResponse.expiresIn))
        )

        self.cachedToken = token
        return token
    }

    /// Generates PKCE code verifier and challenge values as a tuple.
    /// - Returns: A tuple containing the code verifier and its corresponding challenge.
    private static func generatePKCEValues() -> (verifier: String, challenge: String) {
        // Generate a cryptographically secure random code verifier
        var buffer = [UInt8](repeating: 0, count: 32)
        #if os(macOS) || os(iOS) || os(watchOS) || os(tvOS)
            _ = SecRandomCopyBytes(kSecRandomDefault, buffer.count, &buffer)
        #else
            // This should be cryptographically secure, see: https://forums.swift.org/t/random-data-uint8-random-or-secrandomcopybytes/56165/9
            var generator = SystemRandomNumberGenerator()
            buffer = buffer.map { _ in UInt8.random(in: 0 ... 255, using: &generator) }
        #endif

        let verifier = Data(buffer).urlSafeBase64EncodedString()
            .trimmingCharacters(in: .whitespaces)

        // Generate SHA256 hash of the verifier for the challenge
        let data = Data(verifier.utf8)
        let hashed = SHA256.hash(data: data)
        let challenge = Data(hashed).urlSafeBase64EncodedString()

        return (verifier, challenge)
    }
}

// MARK: -

/// Configuration for OAuth authentication client
public struct OAuthClientConfiguration: Sendable {
    /// The base URL for OAuth endpoints
    public let baseURL: URL

    /// The redirect URL for OAuth callbacks
    public let redirectURL: URL

    /// The OAuth client ID
    public let clientID: String

    /// The scopes for OAuth requests as a space-separated string
    public let scope: String

    /// Initializes a new OAuth configuration with the specified parameters.
    /// - Parameters:
    ///   - baseURL: The base URL for OAuth endpoints.
    ///   - redirectURL: The redirect URL for OAuth callbacks.
    ///   - clientID: The OAuth client ID.
    ///   - scope: The scopes for OAuth requests.
    public init(
        baseURL: URL,
        redirectURL: URL,
        clientID: String,
        scope: String
    ) {
        self.baseURL = baseURL
        self.redirectURL = redirectURL
        self.clientID = clientID
        self.scope = scope
    }
}

// MARK: -

/// OAuth token containing access and refresh tokens
public struct OAuthToken: Sendable, Codable {
    /// The access token
    public let accessToken: String

    /// The refresh token
    public let refreshToken: String?

    /// The expiration date of the token
    public let expiresAt: Date

    /// Whether the token is valid
    public var isValid: Bool {
        Date() < expiresAt.addingTimeInterval(-300)  // 5 min buffer
    }

    /// Initializes a new OAuth token with the specified parameters.
    /// - Parameters:
    ///   - accessToken: The access token.
    ///   - refreshToken: The refresh token.
    ///   - expiresAt: The expiration date of the token.
    public init(accessToken: String, refreshToken: String?, expiresAt: Date) {
        self.accessToken = accessToken
        self.refreshToken = refreshToken
        self.expiresAt = expiresAt
    }

    enum CodingKeys: String, CodingKey {
        case accessToken = "access_token"
        case refreshToken = "refresh_token"
        case expiresAt = "expires_at"
    }
}

/// OAuth error enum
public enum OAuthError: LocalizedError, Equatable, Sendable {
    /// Authentication required
    case authenticationRequired

    /// Invalid callback
    case invalidCallback

    /// Session failed to start
    case sessionFailedToStart

    /// Missing code verifier
    case missingCodeVerifier

    /// Token exchange failed
    case tokenExchangeFailed

    /// Token storage error
    case tokenStorageError(String)

    /// Invalid configuration
    case invalidConfiguration(String)

    /// The error description
    public var errorDescription: String? {
        switch self {
        case .authenticationRequired: return "Authentication required"
        case .invalidCallback: return "Invalid callback"
        case .sessionFailedToStart: return "Session failed to start"
        case .missingCodeVerifier: return "Missing code verifier"
        case .tokenExchangeFailed: return "Token exchange failed"
        case .tokenStorageError(let error): return "Token storage error: \(error)"
        case .invalidConfiguration(let error): return "Invalid configuration: \(error)"
        }
    }
}

private struct TokenResponse: Sendable, Codable {
    let accessToken: String
    let refreshToken: String?
    let expiresIn: Int
    let tokenType: String

    enum CodingKeys: String, CodingKey {
        case accessToken = "access_token"
        case refreshToken = "refresh_token"
        case expiresIn = "expires_in"
        case tokenType = "token_type"
    }
}

// MARK: -

private extension Data {
    /// Returns a URL-safe Base64 encoded string suitable for use in URLs and OAuth flows.
    ///
    /// This method applies the standard Base64 encoding and then replaces characters
    /// that are not URL-safe (+ becomes -, / becomes _, = padding is removed).
    /// - Returns: A URL-safe Base64 encoded string.
    func urlSafeBase64EncodedString() -> String {
        base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }
}
