import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Testing

@testable import HuggingFace

@Suite("CacheLocationProvider Tests")
struct CacheLocationProviderTests {
    // MARK: - Fixed Location Tests

    @Test("Fixed location returns provided directory")
    func fixedLocation() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("fixed-cache")
        let provider = CacheLocationProvider.fixed(directory: directory)

        let resolved = provider.resolve()

        #expect(resolved == directory)
    }

    @Test("Fixed location via URL initializer")
    func fixedLocationURLInit() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("url-cache")
        let provider = CacheLocationProvider(directory)

        let resolved = provider.resolve()

        #expect(resolved == directory)
    }

    @Test("Fixed location via path initializer")
    func fixedLocationPathInit() throws {
        let path = "/tmp/path-cache"
        let provider = CacheLocationProvider(path: path)

        let resolved = provider.resolve()

        #expect(resolved?.path == path)
    }

    @Test("Path initializer expands tilde")
    func pathInitializerExpandsTilde() throws {
        let provider = CacheLocationProvider(path: "~/my-cache")

        let resolved = provider.resolve()

        #expect(resolved != nil)
        #expect(resolved?.path.contains("~") == false)
        #expect(resolved?.path.hasSuffix("/my-cache") == true)
    }

    // MARK: - Environment Location Tests

    @Test("Environment provider returns valid path")
    func environmentProvider() throws {
        let provider = CacheLocationProvider.environment

        let resolved = provider.resolve()

        // Should always resolve to something (at least the default)
        #expect(resolved != nil)
        #expect(resolved?.path.contains("huggingface") == true || resolved?.path.contains("hub") == true)
    }

    @Test("Environment provider default path structure")
    func environmentProviderDefaultPath() throws {
        // When no environment variables are set, should use default
        let provider = CacheLocationProvider.environment

        let resolved = provider.resolve()

        #expect(resolved != nil)
        // Default path should end with hub
        #expect(resolved?.lastPathComponent == "hub")
    }

    @Test("Environment provider prioritizes HF_HUB_CACHE over HF_HOME")
    func environmentProviderPrioritizesHubCache() throws {
        let provider = CacheLocationProvider.environment
        let resolved = provider.resolveFromEnvironment([
            "HF_HUB_CACHE": "/tmp/hf-hub-cache",
            "HF_HOME": "/tmp/hf-home",
        ])

        #expect(resolved?.path == "/tmp/hf-hub-cache")
    }

    @Test("Environment provider uses HF_HOME when HF_HUB_CACHE is missing")
    func environmentProviderUsesHFHome() throws {
        let provider = CacheLocationProvider.environment
        let resolved = provider.resolveFromEnvironment([
            "HF_HOME": "/tmp/hf-home"
        ])

        #expect(resolved?.path == "/tmp/hf-home/hub")
    }

    #if os(macOS)
        @Test("Default cache directory uses home cache on non-sandboxed macOS")
        func defaultCacheDirectoryNonSandboxedMacOS() throws {
            let resolved = CacheLocationProvider.defaultCacheDirectory(
                environment: [:],
                homeDirectory: URL(fileURLWithPath: "/Users/test-user"),
                cachesDirectory: URL(fileURLWithPath: "/Users/test-user/Library/Caches")
            )

            #expect(resolved.path == "/Users/test-user/.cache/huggingface/hub")
        }

        @Test("Default cache directory uses app caches when sandboxed")
        func defaultCacheDirectorySandboxedMacOS() throws {
            let resolved = CacheLocationProvider.defaultCacheDirectory(
                environment: ["APP_SANDBOX_CONTAINER_ID": "com.example.app"],
                homeDirectory: URL(fileURLWithPath: "/Users/test-user"),
                cachesDirectory: URL(fileURLWithPath: "/sandbox/Library/Caches")
            )

            #expect(resolved.path == "/sandbox/Library/Caches/huggingface/hub")
        }
    #else
        @Test("Default cache directory uses caches directory on non-macOS")
        func defaultCacheDirectoryNonMacOS() throws {
            let resolved = CacheLocationProvider.defaultCacheDirectory(
                environment: ["APP_SANDBOX_CONTAINER_ID": "ignored"],
                homeDirectory: URL(fileURLWithPath: "/home/test-user"),
                cachesDirectory: URL(fileURLWithPath: "/platform/Library/Caches")
            )

            #expect(resolved.path == "/platform/Library/Caches/huggingface/hub")
        }
    #endif

    // MARK: - Composite Location Tests

    @Test("Composite provider returns first valid result")
    func compositeFirstValid() throws {
        let firstDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("first")
        let secondDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("second")

        let provider = CacheLocationProvider.composite([
            .fixed(directory: firstDir),
            .fixed(directory: secondDir),
        ])

        let resolved = provider.resolve()

        #expect(resolved == firstDir)
    }

    @Test("Composite provider skips nil results")
    func compositeSkipsNil() throws {
        let validDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("valid")

        let provider = CacheLocationProvider.composite([
            .custom { nil },
            .fixed(directory: validDir),
        ])

        let resolved = provider.resolve()

        #expect(resolved == validDir)
    }

    @Test("Composite provider returns nil when all fail")
    func compositeAllFail() throws {
        let provider = CacheLocationProvider.composite([
            .custom { nil },
            .custom { nil },
        ])

        let resolved = provider.resolve()

        #expect(resolved == nil)
    }

    @Test("Composite provider via array literal")
    func compositeArrayLiteral() throws {
        let primaryDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("primary")
        let fallbackDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("fallback")

        let provider: CacheLocationProvider = [
            .fixed(directory: primaryDir),
            .fixed(directory: fallbackDir),
        ]

        let resolved = provider.resolve()

        #expect(resolved == primaryDir)
    }

    @Test("Empty composite returns nil")
    func emptyComposite() throws {
        let provider = CacheLocationProvider.composite([])

        let resolved = provider.resolve()

        #expect(resolved == nil)
    }

    // MARK: - Custom Location Tests

    @Test("Custom provider executes closure")
    func customProvider() throws {
        let customDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("custom-\(UUID().uuidString)")

        let provider = CacheLocationProvider.custom {
            customDir
        }

        let resolved = provider.resolve()

        #expect(resolved == customDir)
    }

    @Test("Custom provider can return nil")
    func customProviderNil() throws {
        let provider = CacheLocationProvider.custom {
            nil
        }

        let resolved = provider.resolve()

        #expect(resolved == nil)
    }

    @Test("Custom provider closure is called each time")
    func customProviderCalledEachTime() throws {
        let callCounter = CallCounter()
        let provider = CacheLocationProvider.custom {
            callCounter.increment()
            return FileManager.default.temporaryDirectory
        }

        _ = provider.resolve()
        _ = provider.resolve()
        _ = provider.resolve()

        #expect(callCounter.count == 3)
    }

    @Test("Custom provider with dynamic logic")
    func customProviderDynamicLogic() throws {
        let primaryDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("primary")
        let alternateDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("alternate")

        let toggle = Toggle()

        let provider = CacheLocationProvider.custom {
            toggle.value ? alternateDir : primaryDir
        }

        #expect(provider.resolve() == primaryDir)

        toggle.value = true
        #expect(provider.resolve() == alternateDir)
    }

    // MARK: - Nested Composite Tests

    @Test("Nested composite providers resolve correctly")
    func nestedComposite() throws {
        let innerDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("inner")
        let outerDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("outer")

        let innerComposite = CacheLocationProvider.composite([
            .custom { nil },
            .fixed(directory: innerDir),
        ])

        let outerComposite = CacheLocationProvider.composite([
            .custom { nil },
            innerComposite,
            .fixed(directory: outerDir),
        ])

        let resolved = outerComposite.resolve()

        #expect(resolved == innerDir)
    }

    // MARK: - HubCache Integration Tests

    @Test("HubCache uses location provider")
    func hubCacheUsesProvider() throws {
        let customDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("hf-cache-\(UUID().uuidString)")

        let cache = HubCache(location: .fixed(directory: customDir))

        #expect(cache.cacheDirectory == customDir)
        #expect(cache.locationProvider.resolve() == customDir)
    }

    @Test("HubCache with composite provider")
    func hubCacheCompositeProvider() throws {
        let primaryDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("primary-cache")

        let provider: CacheLocationProvider = [
            .custom { nil },  // First provider fails
            .fixed(directory: primaryDir),  // Second succeeds
        ]

        let cache = HubCache(location: provider)

        #expect(cache.cacheDirectory == primaryDir)
    }

    @Test("HubCache with custom provider")
    func hubCacheCustomProvider() throws {
        let dynamicDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("dynamic-cache")

        let cache = HubCache(
            location: .custom {
                dynamicDir
            }
        )

        #expect(cache.cacheDirectory == dynamicDir)
    }

    @Test("HubCache fallback when provider returns nil")
    func hubCacheFallback() throws {
        let cache = HubCache(location: .custom { nil })

        // Should fall back to default location
        #expect(cache.cacheDirectory.path.contains("huggingface"))
        #expect(cache.cacheDirectory.lastPathComponent == "hub")
    }
}

// MARK: -

/// A thread-safe counter for testing closure invocations.
private final class CallCounter: @unchecked Sendable {
    private var _count = 0
    private let lock = NSLock()

    var count: Int {
        lock.lock()
        defer { lock.unlock() }
        return _count
    }

    func increment() {
        lock.lock()
        defer { lock.unlock() }
        _count += 1
    }
}

/// A thread-safe boolean toggle for testing dynamic behavior.
private final class Toggle: @unchecked Sendable {
    private var _value = false
    private let lock = NSLock()

    var value: Bool {
        get {
            lock.lock()
            defer { lock.unlock() }
            return _value
        }
        set {
            lock.lock()
            defer { lock.unlock() }
            _value = newValue
        }
    }
}
