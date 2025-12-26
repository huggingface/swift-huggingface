import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

/// A Hugging Face Hub API client.
///
/// This client provides methods to interact with the Hugging Face Hub API,
/// allowing you to list and retrieve information about models, datasets, and spaces,
/// as well as manage repositories.
///
/// ## Endpoint
///
/// By default, the client connects to `https://huggingface.co`.
/// You can customize the endpoint by passing a `host` parameter,
/// or by setting the `HF_ENDPOINT` environment variable.
///
/// ## Authentication
///
/// Authentication tokens are auto-detected from standard locations (in order of priority):
///
/// 1. `HF_TOKEN` environment variable
/// 2. `HUGGING_FACE_HUB_TOKEN` environment variable
/// 3. `HF_TOKEN_PATH` environment variable (path to token file)
/// 4. `$HF_HOME/token` file
/// 5. `~/.cache/huggingface/token` (standard HF CLI location)
/// 6. `~/.huggingface/token` (fallback location)
///
/// You can also provide an explicit token via the `bearerToken` parameter,
/// or use a custom ``TokenProvider`` for more advanced authentication flows.
///
/// ## Caching
///
/// Downloaded files are cached using a Python-compatible cache structure,
/// allowing seamless cache reuse between Swift and Python Hugging Face clients.
/// The cache location is auto-detected from (in order of priority):
///
/// 1. `HF_HUB_CACHE` environment variable
/// 2. `HF_HOME` environment variable + `/hub`
/// 3. `~/.cache/huggingface/hub` (standard location)
///
/// To disable caching, pass `cache: nil` when initializing the client.
///
/// - SeeAlso: ``HubCache`` for direct cache management.
/// - SeeAlso: [Hub API Documentation](https://huggingface.co/docs/hub/api)
public final class HubClient: Sendable {
    /// The default host URL for the Hugging Face Hub API.
    public static let defaultHost = URL(string: "https://huggingface.co")!

    /// A default client instance with auto-detected configuration.
    ///
    /// This client uses environment-based detection for
    /// the endpoint, authentication token, and cache location.
    /// See the ``HubClient`` class documentation for details on the detection order.
    public static let `default` = HubClient()

    /// The underlying HTTP client.
    internal let httpClient: HTTPClient

    /// The cache for downloaded files, or `nil` if caching is disabled.
    ///
    /// When set, downloaded files are stored in a Python-compatible cache structure,
    /// allowing cache reuse between Swift and Python Hugging Face clients.
    public let cache: HubCache?

    /// Override for offline mode detection.
    ///
    /// When `true`, the client will only use cached files and skip network requests.
    /// When `false`, the client will always attempt network requests.
    /// When `nil` (default), offline mode is auto-detected based on network connectivity.
    public let useOfflineMode: Bool?

    /// The host URL for requests made by the client.
    public var host: URL {
        httpClient.host
    }

    /// The value for the `User-Agent` header sent in requests, if any.
    public var userAgent: String? {
        httpClient.userAgent
    }

    /// The underlying client session.
    var session: URLSession {
        httpClient.session
    }

    /// The Bearer token for authentication, if any.
    public var bearerToken: String? {
        get async {
            try? await httpClient.tokenProvider.getToken()
        }
    }

    /// Creates a client with auto-detected configuration.
    ///
    /// This initializer uses environment-based detection for the endpoint and authentication token.
    /// See the class documentation for details on the detection order.
    ///
    /// - Parameters:
    ///   - session: The URL session for network requests.
    ///   - userAgent: The value for the `User-Agent` header, if any.
    ///   - cache: The cache for downloaded files. Pass `nil` to disable caching.
    ///   - useOfflineMode: Override for offline mode detection. When `nil`, auto-detects based on network.
    public convenience init(
        session: URLSession = URLSession(configuration: .default),
        userAgent: String? = nil,
        cache: HubCache? = .default,
        useOfflineMode: Bool? = nil
    ) {
        self.init(
            session: session,
            host: Self.detectHost(),
            userAgent: userAgent,
            tokenProvider: .environment,
            cache: cache,
            useOfflineMode: useOfflineMode
        )
    }

    /// Creates a client with the specified session, host, user agent, and authentication token.
    ///
    /// - Parameters:
    ///   - session: The underlying client session. Defaults to `URLSession(configuration: .default)`.
    ///   - host: The host URL to use for requests.
    ///   - userAgent: The value for the `User-Agent` header sent in requests, if any. Defaults to `nil`.
    ///   - bearerToken: The Bearer token for authentication, if any. Defaults to `nil`.
    ///   - cache: The cache for downloaded files. Defaults to `HubCache.default`.
    ///            Pass `nil` to disable caching.
    ///   - useOfflineMode: Override for offline mode detection. When `nil`, auto-detects based on network.
    public convenience init(
        session: URLSession = URLSession(configuration: .default),
        host: URL,
        userAgent: String? = nil,
        bearerToken: String? = nil,
        cache: HubCache? = .default,
        useOfflineMode: Bool? = nil
    ) {
        self.init(
            session: session,
            host: host,
            userAgent: userAgent,
            tokenProvider: bearerToken.map { .fixed(token: $0) } ?? .none,
            cache: cache,
            useOfflineMode: useOfflineMode
        )
    }

    /// Creates a client with the specified session, host, user agent, and token provider.
    ///
    /// - Parameters:
    ///   - session: The underlying client session. Defaults to `URLSession(configuration: .default)`.
    ///   - host: The host URL to use for requests.
    ///   - userAgent: The value for the `User-Agent` header sent in requests, if any. Defaults to `nil`.
    ///   - tokenProvider: The token provider for authentication.
    ///   - cache: The cache for downloaded files. Defaults to `HubCache.default`.
    ///            Pass `nil` to disable caching.
    ///   - useOfflineMode: Override for offline mode detection. When `nil`, auto-detects based on network.
    public init(
        session: URLSession = URLSession(configuration: .default),
        host: URL,
        userAgent: String? = nil,
        tokenProvider: TokenProvider,
        cache: HubCache? = .default,
        useOfflineMode: Bool? = nil
    ) {
        self.httpClient = HTTPClient(
            host: host,
            userAgent: userAgent,
            tokenProvider: tokenProvider,
            session: session
        )
        self.cache = cache
        self.useOfflineMode = useOfflineMode
        // Start network monitoring for auto-detection
        NetworkMonitor.shared.startMonitoring()
    }

    // MARK: - Auto-detection

    /// Detects the Hugging Face Hub endpoint from environment variables.
    ///
    /// Checks the `HF_ENDPOINT` environment variable, defaulting to https://huggingface.co.
    ///
    /// - Returns: The detected or default endpoint URL.
    private static func detectHost() -> URL {
        if let endpoint = ProcessInfo.processInfo.environment["HF_ENDPOINT"],
            let url = URL(string: endpoint)
        {
            return url
        }
        return defaultHost
    }

    // MARK: - Offline Mode

    /// Determines whether offline mode should be used.
    ///
    /// Returns `true` if:
    /// - `useOfflineMode` is explicitly set to `true`, or
    /// - `useOfflineMode` is `nil` and the network is unavailable
    ///
    /// - Returns: Whether to operate in offline mode.
    func shouldUseOfflineMode() async -> Bool {
        if let override = useOfflineMode {
            return override
        }
        return await NetworkMonitor.shared.state.shouldUseOfflineMode()
    }
}
