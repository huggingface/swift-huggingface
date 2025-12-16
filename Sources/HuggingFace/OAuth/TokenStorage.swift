import Foundation

/// A cross-platform mechanism for storing and retrieving OAuth tokens.
///
/// This provides a file-based storage implementation that works on all platforms,
/// including Linux. For Apple platforms, the `HuggingFaceAuthenticationManager`
/// provides keychain-based storage through its own `TokenStorage` type.
///
/// Example usage:
/// ```swift
/// let storage = FileTokenStorage.default
/// try storage.store(token)
/// let retrieved = try storage.retrieve()
/// ```
public struct FileTokenStorage: Sendable {
    private let fileURL: URL

    /// Creates a new file-based token storage at the specified URL.
    /// - Parameter fileURL: The URL where tokens will be stored.
    public init(fileURL: URL) {
        self.fileURL = fileURL
    }

    /// The default token storage location.
    ///
    /// On Linux/Unix: `~/.cache/huggingface/token.json`
    /// On macOS: `~/Library/Caches/huggingface/token.json`
    public static var `default`: FileTokenStorage {
        let cacheDir: URL
        #if os(macOS) || os(iOS) || os(tvOS) || os(watchOS) || os(visionOS)
            cacheDir =
                FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask).first
                ?? FileManager.default.temporaryDirectory
        #else
            // Linux/Unix: Use XDG_CACHE_HOME or ~/.cache
            if let xdgCache = ProcessInfo.processInfo.environment["XDG_CACHE_HOME"] {
                cacheDir = URL(fileURLWithPath: xdgCache)
            } else {
                let home =
                    ProcessInfo.processInfo.environment["HOME"]
                    ?? NSHomeDirectory()
                cacheDir = URL(fileURLWithPath: home).appendingPathComponent(".cache")
            }
        #endif

        let tokenDir = cacheDir.appendingPathComponent("huggingface")
        let tokenFile = tokenDir.appendingPathComponent("token.json")

        return FileTokenStorage(fileURL: tokenFile)
    }

    /// Stores an OAuth token to the file.
    /// - Parameter token: The token to store.
    /// - Throws: An error if the token cannot be encoded or written.
    public func store(_ token: OAuthToken) throws {
        // Create directory if needed
        let directory = fileURL.deletingLastPathComponent()
        try FileManager.default.createDirectory(
            at: directory,
            withIntermediateDirectories: true
        )

        // Encode and write
        let encoder = JSONEncoder()
        encoder.outputFormatting = .prettyPrinted
        let data = try encoder.encode(token)
        try data.write(to: fileURL, options: .atomic)

        // Set file permissions to owner-only (0600) on Unix systems
        #if !os(Windows)
            try FileManager.default.setAttributes(
                [.posixPermissions: 0o600],
                ofItemAtPath: fileURL.path
            )
        #endif
    }

    /// Retrieves the stored OAuth token.
    /// - Returns: The stored token, or `nil` if no token is stored.
    /// - Throws: An error if the token file exists but cannot be read or decoded.
    public func retrieve() throws -> OAuthToken? {
        guard FileManager.default.fileExists(atPath: fileURL.path) else {
            return nil
        }

        let data = try Data(contentsOf: fileURL)
        let decoder = JSONDecoder()
        return try decoder.decode(OAuthToken.self, from: data)
    }

    /// Deletes the stored OAuth token.
    /// - Throws: An error if the token file exists but cannot be deleted.
    public func delete() throws {
        guard FileManager.default.fileExists(atPath: fileURL.path) else {
            return
        }
        try FileManager.default.removeItem(at: fileURL)
    }

    /// Whether a token is currently stored.
    public var hasStoredToken: Bool {
        FileManager.default.fileExists(atPath: fileURL.path)
    }
}

// MARK: - Environment Token Storage

/// A simple token storage that reads from an environment variable.
///
/// This is useful for server-side applications and CI/CD environments
/// where tokens are provided via environment variables.
public struct EnvironmentTokenStorage: Sendable {
    private let variableName: String

    /// Creates a new environment token storage.
    /// - Parameter variableName: The name of the environment variable containing the token.
    ///   Defaults to `HF_TOKEN`.
    public init(variableName: String = "HF_TOKEN") {
        self.variableName = variableName
    }

    /// Retrieves the token from the environment variable.
    /// - Returns: An OAuth token with the access token from the environment, or `nil` if not set.
    public func retrieve() -> OAuthToken? {
        guard let token = ProcessInfo.processInfo.environment[variableName],
            !token.isEmpty
        else {
            return nil
        }

        // Environment tokens don't expire and don't have refresh tokens
        return OAuthToken(
            accessToken: token,
            refreshToken: nil,
            expiresAt: Date.distantFuture
        )
    }
}

// MARK: - Composite Token Storage

/// A token storage that tries multiple storage backends in order.
///
/// This is useful for applications that want to support multiple token sources,
/// such as checking environment variables first, then falling back to file storage.
public struct CompositeTokenStorage: Sendable {
    private let storages: [@Sendable () throws -> OAuthToken?]
    private let primaryStorage: FileTokenStorage?

    /// Creates a composite token storage with the specified backends.
    /// - Parameters:
    ///   - environment: Whether to check environment variables first.
    ///   - file: The file storage to use, or `nil` to skip file storage.
    public init(
        environment: Bool = true,
        file: FileTokenStorage? = .default
    ) {
        var storages: [@Sendable () throws -> OAuthToken?] = []

        if environment {
            let envStorage = EnvironmentTokenStorage()
            storages.append { envStorage.retrieve() }
        }

        if let file = file {
            storages.append { try file.retrieve() }
        }

        self.storages = storages
        self.primaryStorage = file
    }

    /// Retrieves a token from the first storage that has one.
    /// - Returns: The first available token, or `nil` if none found.
    public func retrieve() throws -> OAuthToken? {
        for storage in storages {
            if let token = try storage() {
                return token
            }
        }
        return nil
    }

    /// Stores a token to the primary (file) storage.
    /// - Parameter token: The token to store.
    public func store(_ token: OAuthToken) throws {
        try primaryStorage?.store(token)
    }

    /// Deletes the token from the primary (file) storage.
    public func delete() throws {
        try primaryStorage?.delete()
    }
}
