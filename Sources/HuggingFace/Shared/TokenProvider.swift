import Foundation

/// A token provider for Hugging Face API authentication.
///
/// `TokenProvider` provides a flexible, composable way to handle authentication
/// tokens for Hugging Face API requests. You can use it with fixed tokens,
/// environment-based detection, OAuth flows, or custom implementations.
///
/// ## Environment-Based Authentication
///
/// For automatic token detection from environment variables and files:
///
/// ```swift
/// let client = HubClient(tokenProvider: .environment)
/// ```
///
/// The `.environment` case automatically detects tokens from multiple sources
/// in priority order:
///
/// 1. `HF_TOKEN` environment variable
/// 2. `HUGGING_FACE_HUB_TOKEN` environment variable
/// 3. File at path specified by `HF_TOKEN_PATH` environment variable
/// 4. File at `$HF_HOME/token`
/// 5. File at `~/.cache/huggingface/token` (standard HF CLI location)
/// 6. File at `~/.huggingface/token` (fallback location)
///
/// ## Fixed Token Authentication
///
/// For a fixed token, use the `.fixed` case with a string literal:
///
/// ```swift
/// let client = HubClient(tokenProvider: "hf_abc123")
/// ```
///
/// ## OAuth Authentication
///
/// For OAuth-based authentication (requires macOS 14+, iOS 17+), use the `.oauth(manager:)` factory method:
///
/// ```swift
/// let authManager = try HuggingFaceAuthenticationManager(
///     clientID: "your-client-id",
///     redirectURL: URL(string: "myapp://oauth")!,
///     scope: .basic,
///     keychainService: "com.example.app",
///     keychainAccount: "huggingface"
/// )
/// let client = HubClient(tokenProvider: .oauth(manager: authManager))
/// ```
///
/// ## Composite Authentication
///
/// Combine multiple authentication strategies using array literals.
/// The provider tries each strategy in order until one succeeds:
///
/// ```swift
/// let client = HubClient(tokenProvider: [
///     .oauth(manager: authManager), // Try OAuth first
///     .environment,                 // Fall back to environment detection
///     "hf_abc123"                   // Final fallback
/// ])
/// ```
///
/// You can also use the explicit `.composite` case:
///
/// ```swift
/// let tokenProvider = TokenProvider.composite([
///     .oauth(manager: authManager),
///     .environment,
///     .fixed(token: "fallback")
/// ])
/// ```
///
/// ## Custom Token Providers
///
/// For custom authentication logic, use the `.custom` case:
///
/// ```swift
/// let customProvider = TokenProvider.custom {
///     // Your custom token retrieval logic
///     return try await fetchTokenFromKeychain()
/// }
/// let client = HubClient(tokenProvider: customProvider)
/// ```
///
/// ## No Authentication
///
/// To explicitly disable authentication, use the `.none` case:
///
/// ```swift
/// let client = HubClient(tokenProvider: .none)
/// ```
public indirect enum TokenProvider: Sendable {
    /// A fixed token provider that returns a static token.
    ///
    /// Use string literals for the most common usage:
    /// ```swift
    /// let client = HubClient(tokenProvider: "hf_abc123")
    /// ```
    ///
    /// For explicit case usage or when you need `nil` tokens:
    /// ```swift
    /// let client = HubClient(tokenProvider: .fixed(token: "hf_abc123"))
    /// let noAuthClient = HubClient(tokenProvider: .fixed(token: nil))
    /// ```
    ///
    /// - Parameter token: The bearer token to use for authentication, or `nil` for no authentication.
    case fixed(token: String)

    /// An environment-based token provider that auto-detects tokens from standard locations.
    ///
    /// This provider automatically detects tokens from multiple sources in priority order:
    /// 1. `HF_TOKEN` environment variable
    /// 2. `HUGGING_FACE_HUB_TOKEN` environment variable
    /// 3. File at path specified by `HF_TOKEN_PATH` environment variable
    /// 4. File at `$HF_HOME/token`
    /// 5. File at `~/.cache/huggingface/token` (standard HF CLI location)
    /// 6. File at `~/.huggingface/token` (fallback location)
    ///
    /// This is the default behavior for most Hugging Face clients and follows
    /// the same token detection logic as the Hugging Face CLI.
    case environment

    /// An OAuth token provider that retrieves tokens asynchronously.
    ///
    /// Use this case for OAuth-based authentication flows. Create instances using
    /// the `TokenProvider.oauth(manager:)` factory method when using `HuggingFaceAuthenticationManager`.
    ///
    /// - Parameter getToken: A closure that retrieves a valid OAuth token.
    case oauth(getToken: @Sendable () async throws -> String)

    /// A composite token provider that tries multiple providers in order.
    ///
    /// Use array literals for the most common usage:
    /// ```swift
    /// let client = HubClient(tokenProvider: [.oauth(manager: authManager), .environment, "fallback"])
    /// ```
    ///
    /// For explicit case usage:
    /// ```swift
    /// let client = HubClient(tokenProvider: .composite([.oauth(manager: authManager), .environment]))
    /// ```
    ///
    /// This provider attempts to get a token from each provider in the array
    /// until one succeeds. If all providers fail, it returns `nil`.
    ///
    /// - Parameter providers: An array of token providers to try in order.
    case composite([TokenProvider])

    /// A custom token provider with a user-defined implementation.
    ///
    /// Use this case when you need custom token retrieval logic, such as
    /// fetching from a keychain, making API calls, or implementing
    /// custom authentication flows.
    ///
    /// - Parameter implementation: A custom token retrieval function that returns a token or `nil`.
    case custom(@Sendable () async throws -> String?)

    /// A token provider that returns `nil` for all requests.
    ///
    /// Use this case when you need to disable authentication.
    ///
    /// ```swift
    /// let client = HubClient(tokenProvider: .none)
    /// ```
    case none

    /// Gets a token from the token provider.
    ///
    /// This method retrieves an authentication token based on the provider's
    /// configuration. For composite providers, it tries each provider in order
    /// until one succeeds.
    ///
    /// - Returns: A valid bearer token, or `nil` if no authentication is available.
    /// - Throws: An error if token retrieval fails.
    ///
    /// ## Example
    ///
    /// ```swift
    /// let provider = TokenProvider.fixed(token: "hf_abc123")
    /// let token = try await provider.getToken()
    /// // Returns: "hf_abc123"
    /// ```
    public func getToken() throws -> String? {
        switch self {
        case .fixed(let token):
            return token

        case .environment:
            return try getTokenFromEnvironment()

        case .oauth:
            fatalError(
                "OAuth token provider requires async context. Use getToken() in an async context or switch to a synchronous provider."
            )

        case .composite(let providers):
            for provider in providers {
                if let token = try provider.getToken() {
                    return token
                }
            }
            return nil

        case .custom(let implementation):
            fatalError(
                "Custom async token provider requires async context. Use getToken() in an async context or switch to a synchronous provider."
            )

        case .none:
            return nil
        }
    }
}

// MARK: - OAuth Factory

#if canImport(AuthenticationServices)
    import Observation

    extension TokenProvider {
        /// Creates an OAuth token provider using HuggingFaceAuthenticationManager.
        ///
        /// Use this factory method for OAuth-based authentication flows. The authentication
        /// manager handles the complete OAuth flow including token refresh.
        ///
        /// ```swift
        /// let authManager = try HuggingFaceAuthenticationManager(
        ///     clientID: "your-client-id",
        ///     redirectURL: URL(string: "myapp://oauth")!,
        ///     scope: .basic,
        ///     keychainService: "com.example.app",
        ///     keychainAccount: "huggingface"
        /// )
        /// let client = HubClient(tokenProvider: .oauth(manager: authManager))
        /// ```
        ///
        /// - Parameter manager: The OAuth authentication manager that handles token retrieval and refresh.
        /// - Returns: A token provider that retrieves tokens from the authentication manager.
        @available(macOS 14.0, macCatalyst 17.0, iOS 17.0, watchOS 10.0, tvOS 17.0, *)
        public static func oauth(manager: HuggingFaceAuthenticationManager) -> TokenProvider {
            return .oauth(getToken: { @MainActor in
                try await manager.getValidToken()
            })
        }
    }
#endif

// MARK: - ExpressibleByStringLiteral & ExpressibleByStringInterpolation

extension TokenProvider: ExpressibleByStringLiteral, ExpressibleByStringInterpolation {
    public typealias StringLiteralType = String

    public init(stringLiteral value: String) {
        self = .fixed(token: value)
    }

    public init(stringInterpolation: DefaultStringInterpolation) {
        self = .fixed(token: String(stringInterpolation.description))
    }
}

// MARK: - ExpressibleByArrayLiteral

extension TokenProvider: ExpressibleByArrayLiteral {
    public typealias ArrayLiteralElement = TokenProvider

    public init(arrayLiteral elements: TokenProvider...) {
        self = .composite(elements)
    }
}

// MARK: -

/// Reads a token from the specified file path.
///
/// This function reads a token from a file, expanding tilde paths and handling
/// file reading errors gracefully. It's used by the environment token detection
/// logic to read tokens from various file locations.
///
/// - Parameters:
///   - path: The path to the file containing the token. Supports tilde expansion.
///   - fileManager: The file manager to use for file operations. Defaults to `.default`.
/// - Returns: The token read from the file, or `nil` if the file does not exist or cannot be read.
private func readTokenFromPath(_ path: String, fileManager: FileManager = .default) -> String? {
    let expandedPath = NSString(string: path).expandingTildeInPath
    return try? String(contentsOfFile: expandedPath, encoding: .utf8)
}

/// Detects tokens from environment variables and files.
///
/// This method implements the standard Hugging Face token detection logic,
/// checking multiple sources in priority order. It follows the same detection
/// pattern as the Hugging Face CLI and Python library.
///
/// The detection order is:
/// 1. `HF_TOKEN` environment variable
/// 2. `HUGGING_FACE_HUB_TOKEN` environment variable
/// 3. File at path specified by `HF_TOKEN_PATH` environment variable
/// 4. File at `$HF_HOME/token`
/// 5. File at `~/.cache/huggingface/token` (standard HF CLI location)
/// 6. File at `~/.huggingface/token` (fallback location)
///
/// - Parameters:
///   - env: The environment variables to check. Defaults to `ProcessInfo.processInfo.environment`.
///   - fileManager: The file manager to use for file operations. Defaults to `.default`.
/// - Returns: The first valid token found, or `nil` if no token is available.
private func getTokenFromEnvironment(
    _ env: [String: String] = ProcessInfo.processInfo.environment,
    fileManager: FileManager = .default
) throws -> String? {
    let tokenSources: [() -> String?] = [
        { env["HF_TOKEN"] },
        { env["HUGGING_FACE_HUB_TOKEN"] },
        {
            if let tokenPath = env["HF_TOKEN_PATH"] {
                return readTokenFromPath(tokenPath, fileManager: fileManager)
            }
            return nil
        },
        {
            if let hfHome = env["HF_HOME"] {
                let expandedPath = NSString(string: hfHome).expandingTildeInPath
                return readTokenFromPath("\(expandedPath)/token", fileManager: fileManager)
            }
            return nil
        },
        { readTokenFromPath("~/.cache/huggingface/token", fileManager: fileManager) },
        { readTokenFromPath("~/.huggingface/token", fileManager: fileManager) },
    ]

    return tokenSources
        .lazy
        .compactMap { $0()?.trimmingCharacters(in: .whitespacesAndNewlines) }
        .first { !$0.isEmpty }
}
