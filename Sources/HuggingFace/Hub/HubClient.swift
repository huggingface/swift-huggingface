import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

#if canImport(Xet)
    import Xet
#endif

/// A Hugging Face Hub API client.
///
/// This client provides methods to interact with the Hugging Face Hub API,
/// allowing you to list and retrieve information about models, datasets, and spaces,
/// as well as manage repositories.
///
/// The client automatically detects authentication tokens from standard locations (in order of priority):
/// - `HF_TOKEN` environment variable
/// - `HUGGING_FACE_HUB_TOKEN` environment variable
/// - `HF_TOKEN_PATH` environment variable (path to token file)
/// - `HF_HOME/token` file
/// - `~/.cache/huggingface/token` file (standard HF CLI location)
/// - `~/.huggingface/token` file (fallback location)
///
/// The endpoint can be customized via the `HF_ENDPOINT` environment variable.
///
/// - SeeAlso: [Hub API Documentation](https://huggingface.co/docs/hub/api)
public final class HubClient: Sendable {
    /// The default host URL for the Hugging Face Hub API.
    public static let defaultHost = URL(string: "https://huggingface.co")!

    /// A default client instance with auto-detected host and bearer token.
    ///
    /// This client automatically detects the authentication token from environment variables
    /// or standard token file locations, and uses the endpoint specified by `HF_ENDPOINT`
    /// environment variable (defaults to https://huggingface.co).
    public static let `default` = HubClient()

    /// Indicates whether Xet acceleration is enabled for this client.
    public let isXetEnabled: Bool

    /// The underlying HTTP client.
    internal let httpClient: HTTPClient
    
    #if canImport(Xet)
        /// Xet client instance for connection reuse (created once during initialization)
        private let xetClient: XetClient?
        
        /// Thread-safe JWT cache for CAS access tokens
        private let jwtCache: JwtCache
    #endif

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

    /// Creates a client with auto-detected host and bearer token.
    ///
    /// This initializer automatically detects the authentication token from standard locations
    /// and uses the endpoint specified by the `HF_ENDPOINT` environment variable.
    ///
    /// - Parameters:
    ///   - session: The underlying client session. Defaults to `URLSession(configuration: .default)`.
    ///   - userAgent: The value for the `User-Agent` header sent in requests, if any. Defaults to `nil`.
    public convenience init(
        session: URLSession = URLSession(configuration: .default),
        userAgent: String? = nil,
        enableXet: Bool = HubClient.isXetSupported
    ) {
        self.init(
            session: session,
            host: Self.detectHost(),
            userAgent: userAgent,
            tokenProvider: .environment,
            enableXet: enableXet
        )
    }

    /// Creates a client with the specified session, host, user agent, and authentication token.
    ///
    /// - Parameters:
    ///   - session: The underlying client session. Defaults to `URLSession(configuration: .default)`.
    ///   - host: The host URL to use for requests.
    ///   - userAgent: The value for the `User-Agent` header sent in requests, if any. Defaults to `nil`.
    ///   - bearerToken: The Bearer token for authentication, if any. Defaults to `nil`.
    public convenience init(
        session: URLSession = URLSession(configuration: .default),
        host: URL,
        userAgent: String? = nil,
        bearerToken: String? = nil,
        enableXet: Bool = HubClient.isXetSupported
    ) {
        self.init(
            session: session,
            host: host,
            userAgent: userAgent,
            tokenProvider: bearerToken.map { .fixed(token: $0) } ?? .none,
            enableXet: enableXet
        )
    }

    /// Creates a client with the specified session, host, user agent, and token provider.
    ///
    /// - Parameters:
    ///   - session: The underlying client session. Defaults to `URLSession(configuration: .default)`.
    ///   - host: The host URL to use for requests.
    ///   - userAgent: The value for the `User-Agent` header sent in requests, if any. Defaults to `nil`.
    ///   - tokenProvider: The token provider for authentication.
    public init(
        session: URLSession = URLSession(configuration: .default),
        host: URL,
        userAgent: String? = nil,
        tokenProvider: TokenProvider,
        enableXet: Bool = HubClient.isXetSupported
    ) {
        self.isXetEnabled = enableXet && HubClient.isXetSupported
        self.httpClient = HTTPClient(
            host: host,
            userAgent: userAgent,
            tokenProvider: tokenProvider,
            session: session
        )
        
        #if canImport(Xet)
            self.jwtCache = JwtCache()
            
            if self.isXetEnabled {
                // Create XetClient once during initialization
                let token = try? tokenProvider.getToken()
                self.xetClient = try? (token.map { try XetClient.withToken(token: $0) } ?? XetClient())
            } else {
                self.xetClient = nil
            }
        #endif
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

    public static var isXetSupported: Bool {
        #if canImport(Xet)
            return true
        #else
            return false
        #endif
    }

    // MARK: - Xet Client

    #if canImport(Xet)
        /// Thread-safe cache for CAS JWT tokens
        private final class JwtCache: @unchecked Sendable {
            private struct CacheKey: Hashable {
                let repo: String
                let revision: String
            }
            
            private struct CachedJwt {
                let jwt: CasJwtInfo
                let expiresAt: Date
                
                var isExpired: Bool {
                    Date() >= expiresAt
                }
            }
            
            private var cache: [CacheKey: CachedJwt] = [:]
            private let lock = NSLock()
            
            func get(repo: String, revision: String) -> CasJwtInfo? {
                lock.lock()
                defer { lock.unlock() }
                
                let key = CacheKey(repo: repo, revision: revision)
                if let cached = cache[key], !cached.isExpired {
                    return cached.jwt
                }
                return nil
            }
            
            func set(jwt: CasJwtInfo, repo: String, revision: String) {
                lock.lock()
                defer { lock.unlock() }
                
                let key = CacheKey(repo: repo, revision: revision)
                // Cache with expiration (5 minutes before actual expiry for safety)
                let expiresAt = Date(timeIntervalSince1970: TimeInterval(jwt.exp())) - 300
                cache[key] = CachedJwt(jwt: jwt, expiresAt: expiresAt)
            }
        }
    
        /// Returns the Xet client for faster downloads.
        ///
        /// The client is created once during initialization and reused across downloads
        /// to enable connection pooling and avoid reinitialization overhead.
        ///
        /// - Returns: A Xet client instance.
        internal func getXetClient() throws -> XetClient {
            guard isXetEnabled, let client = xetClient else {
                throw HTTPClientError.requestError("Xet support is disabled for this client.")
            }
            return client
        }
        
        /// Gets or fetches a CAS JWT for the given repository and revision.
        ///
        /// JWTs are cached to avoid redundant API calls.
        ///
        /// - Parameters:
        ///   - xetClient: The Xet client to use for fetching the JWT
        ///   - repo: Repository identifier
        ///   - revision: Git revision
        ///   - isUpload: Whether this JWT is for upload (true) or download (false)
        /// - Returns: A CAS JWT info object
        internal func getCachedJwt(
            xetClient: XetClient,
            repo: String,
            revision: String,
            isUpload: Bool
        ) throws -> CasJwtInfo {
            // Check cache first
            if let cached = jwtCache.get(repo: repo, revision: revision) {
                return cached
            }
            
            // Fetch a new JWT
            let jwt = try xetClient.getCasJwt(
                repo: repo,
                revision: revision,
                isUpload: isUpload
            )
            
            // Cache it
            jwtCache.set(jwt: jwt, repo: repo, revision: revision)
            
            return jwt
        }
    #endif
    }
