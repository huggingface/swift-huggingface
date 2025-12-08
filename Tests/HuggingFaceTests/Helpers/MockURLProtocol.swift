import Foundation
import Testing

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

@testable import HuggingFace

// MARK: - Request Handler Storage

/// Stores and manages handlers for MockURLProtocol's request handling.
private actor RequestHandlerStorage {
    private var requestHandler: (@Sendable (URLRequest) async throws -> (HTTPURLResponse, Data))?
    private var isLocked = false

    func setHandler(
        _ handler: @Sendable @escaping (URLRequest) async throws -> (HTTPURLResponse, Data)
    ) async {
        // Wait for any existing handler to be released
        while isLocked {
            try? await Task.sleep(for: .milliseconds(10))
        }
        requestHandler = handler
        isLocked = true
    }

    func clearHandler() async {
        requestHandler = nil
        isLocked = false
    }

    func executeHandler(for request: URLRequest) async throws -> (HTTPURLResponse, Data) {
        guard let handler = requestHandler else {
            throw NSError(
                domain: "MockURLProtocolError",
                code: 0,
                userInfo: [NSLocalizedDescriptionKey: "No request handler set"]
            )
        }
        return try await handler(request)
    }
}

// MARK: - Mock URL Protocol

/// Custom URLProtocol for testing network requests
final class MockURLProtocol: URLProtocol, @unchecked Sendable {
    /// Storage for request handlers
    fileprivate static let requestHandlerStorage = RequestHandlerStorage()

    /// Set a handler to process mock requests
    static func setHandler(
        _ handler: @Sendable @escaping (URLRequest) async throws -> (HTTPURLResponse, Data)
    ) async {
        await requestHandlerStorage.setHandler(handler)
    }

    /// Execute the stored handler for a request
    func executeHandler(for request: URLRequest) async throws -> (HTTPURLResponse, Data) {
        return try await Self.requestHandlerStorage.executeHandler(for: request)
    }

    override class func canInit(with request: URLRequest) -> Bool {
        return true
    }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest {
        return request
    }

    override func startLoading() {
        Task {
            do {
                let (response, data) = try await self.executeHandler(for: request)
                client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
                client?.urlProtocol(self, didLoad: data)
                client?.urlProtocolDidFinishLoading(self)
            } catch {
                client?.urlProtocol(self, didFailWithError: error)
            }
        }
    }

    override func stopLoading() {
        // No-op
    }
}

#if swift(>=6.1)
    // MARK: - Mock URL Session Test Trait

    /// Global async lock for MockURLProtocol tests
    ///
    /// Provides mutual exclusion across async test execution to prevent
    /// interference between parallel test suites using shared mock handlers.
    ///
    /// Note: We can't use `NSLock` or `OSAllocatedUnfairLock` here because:
    /// - They're synchronous locks designed for very short critical sections
    /// - They block threads (bad for Swift concurrency's cooperative thread pool)
    /// - They can't be held across suspension points (await calls)
    ///
    /// An actor-based lock is idiomatic for Swift's async/await model.
    private actor MockURLProtocolLock {
        static let shared = MockURLProtocolLock()

        private var isLocked = false

        private init() {}

        func withLock<T: Sendable>(_ operation: @Sendable () async throws -> T) async rethrows -> T {
            // Wait for lock to be available
            while isLocked {
                try? await Task.sleep(for: .milliseconds(10))
            }

            // Acquire lock
            isLocked = true

            // Execute operation and ensure lock is released even on error
            do {
                let result = try await operation()
                isLocked = false
                return result
            } catch {
                isLocked = false
                throw error
            }
        }
    }

    /// A test trait to set up and clean up mock URL protocol handlers
    struct MockURLSessionTestTrait: TestTrait, TestScoping {
        func provideScope(
            for test: Test,
            testCase: Test.Case?,
            performing function: @Sendable () async throws -> Void
        ) async throws {
            // Serialize all MockURLProtocol tests to prevent interference
            try await MockURLProtocolLock.shared.withLock {
                // Clear handler before test
                await MockURLProtocol.requestHandlerStorage.clearHandler()

                // Execute the test
                try await function()

                // Clear handler after test
                await MockURLProtocol.requestHandlerStorage.clearHandler()
            }
        }
    }

    extension Trait where Self == MockURLSessionTestTrait {
        static var mockURLSession: Self { Self() }
    }

#endif  // swift(>=6.1)
