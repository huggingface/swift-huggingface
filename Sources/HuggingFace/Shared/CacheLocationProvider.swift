import Foundation

/// A provider for determining the Hugging Face cache directory location.
///
/// `CacheLocationProvider` provides a flexible, composable way to configure
/// where cached files are stored. You can use environment-based detection,
/// fixed paths, or custom implementations.
///
/// ## Environment-Based Detection (Default)
///
/// For automatic cache directory detection from environment variables:
///
/// ```swift
/// let cache = HubCache(location: .environment)
/// ```
///
/// The `.environment` case automatically detects the cache directory
/// from multiple sources in priority order:
///
/// 1. `HF_HUB_CACHE` environment variable
/// 2. `HF_HOME` environment variable + `/hub`
/// 3. `~/.cache/huggingface/hub` (standard location)
///
/// ## Fixed Path
///
/// For a specific cache directory:
///
/// ```swift
/// let cache = HubCache(location: .fixed(directory: URL(fileURLWithPath: "/custom/cache")))
/// ```
///
/// ## Disable Caching
///
/// To disable file caching entirely:
///
/// ```swift
/// let client = HubClient(cache: nil)
/// ```
///
/// ## Custom Location Provider
///
/// For custom cache directory logic:
///
/// ```swift
/// let provider = CacheLocationProvider.custom {
///     // Your custom logic
///     return FileManager.default.temporaryDirectory.appendingPathComponent("hf-cache")
/// }
/// let cache = HubCache(location: provider)
/// ```
///
/// ## Composite Providers
///
/// Combine multiple providers using array literals. The first provider
/// that returns a valid path is used:
///
/// ```swift
/// let provider: CacheLocationProvider = [
///     .fixed(directory: URL(fileURLWithPath: "/preferred/cache")),
///     .environment
/// ]
/// ```
public indirect enum CacheLocationProvider: Sendable {
    /// A fixed cache location at a specific directory.
    ///
    /// Use this case when you want to specify an exact cache directory:
    ///
    /// ```swift
    /// let cache = HubCache(location: .fixed(directory: URL(fileURLWithPath: "/path/to/cache")))
    /// ```
    ///
    /// - Parameter directory: The URL of the cache directory.
    case fixed(directory: URL)

    /// An environment-based cache location provider that auto-detects from standard locations.
    ///
    /// This provider automatically detects the cache directory from multiple sources
    /// in priority order:
    ///
    /// 1. `HF_HUB_CACHE` environment variable
    /// 2. `HF_HOME` environment variable + `/hub`
    /// 3. `~/.cache/huggingface/hub` (macOS) or `Library/Caches/huggingface/hub` (other platforms)
    ///
    /// This follows the same detection logic as the Python `huggingface_hub` library,
    /// enabling cache sharing between Swift and Python clients.
    case environment

    /// A composite cache location provider that tries multiple providers in order.
    ///
    /// Use array literals for convenient composition:
    ///
    /// ```swift
    /// let provider: CacheLocationProvider = [
    ///     .fixed(directory: URL(fileURLWithPath: "/preferred/cache")),
    ///     .environment
    /// ]
    /// ```
    ///
    /// The first provider that returns a valid path is used.
    ///
    /// - Parameter providers: An array of providers to try in order.
    case composite([CacheLocationProvider])

    /// A custom cache location provider with user-defined logic.
    ///
    /// Use this case when you need custom cache directory resolution:
    ///
    /// ```swift
    /// let provider = CacheLocationProvider.custom {
    ///     // Custom logic to determine cache location
    ///     return URL.cachesDirectory.appendingPathComponent("HuggingFace")
    /// }
    /// ```
    ///
    /// - Parameter implementation: A closure that returns the cache directory URL.
    case custom(@Sendable () -> URL?)

    /// Resolves the cache directory from this provider.
    ///
    /// - Returns: The resolved cache directory URL, or `nil` if resolution fails.
    ///
    /// ## Example
    ///
    /// ```swift
    /// let provider = CacheLocationProvider.environment
    /// if let cacheDir = provider.resolve() {
    ///     print("Cache at: \(cacheDir.path)")
    /// }
    /// ```
    public func resolve() -> URL? {
        switch self {
        case .fixed(let directory):
            return directory

        case .environment:
            return resolveFromEnvironment()

        case .composite(let providers):
            for provider in providers {
                if let url = provider.resolve() {
                    return url
                }
            }
            return nil

        case .custom(let implementation):
            return implementation()
        }
    }

    // MARK: - Environment Resolution

    /// Resolves the cache directory from environment variables.
    ///
    /// Checks in order:
    /// 1. `HF_HUB_CACHE` environment variable
    /// 2. `HF_HOME` environment variable + `/hub`
    /// 3. `~/.cache/huggingface/hub` (macOS) or `Library/Caches/huggingface/hub` (other platforms)
    private func resolveFromEnvironment(
        _ env: [String: String] = ProcessInfo.processInfo.environment
    ) -> URL? {
        let locationSources: [() -> URL?] = [
            // 1. HF_HUB_CACHE environment variable
            {
                guard let hubCache = env["HF_HUB_CACHE"] else { return nil }
                let expandedPath = NSString(string: hubCache).expandingTildeInPath
                return URL(fileURLWithPath: expandedPath)
            },
            // 2. HF_HOME environment variable + /hub
            {
                guard let hfHome = env["HF_HOME"] else { return nil }
                let expandedPath = NSString(string: hfHome).expandingTildeInPath
                return URL(fileURLWithPath: expandedPath).appendingPathComponent("hub")
            },
            // 3. Default: ~/.cache/huggingface/hub (macOS) or Caches/huggingface/hub (iOS)
            {
                #if os(macOS)
                    let homeDirectory = FileManager.default.homeDirectoryForCurrentUser
                    return
                        homeDirectory
                        .appendingPathComponent(".cache")
                        .appendingPathComponent("huggingface")
                        .appendingPathComponent("hub")
                #else
                    return URL.cachesDirectory
                        .appendingPathComponent("huggingface")
                        .appendingPathComponent("hub")
                #endif
            },
        ]

        return locationSources
            .lazy
            .compactMap { $0() }
            .first
    }
}

// MARK: - ExpressibleByArrayLiteral

extension CacheLocationProvider: ExpressibleByArrayLiteral {
    public typealias ArrayLiteralElement = CacheLocationProvider

    public init(arrayLiteral elements: CacheLocationProvider...) {
        self = .composite(elements)
    }
}

// MARK: - Convenience Initializers

extension CacheLocationProvider {
    /// Creates a fixed cache location provider from a URL.
    init(_ url: URL) {
        self = .fixed(directory: url)
    }

    /// Creates a fixed cache location provider from a file path string.
    ///
    /// ```swift
    /// let cache = HubCache(location: .init(path: "/path/to/cache"))
    /// ```
    ///
    /// - Parameter path: The file path to the cache directory. Supports tilde expansion.
    public init(path: String) {
        let expandedPath = NSString(string: path).expandingTildeInPath
        self = .fixed(directory: URL(fileURLWithPath: expandedPath))
    }
}
