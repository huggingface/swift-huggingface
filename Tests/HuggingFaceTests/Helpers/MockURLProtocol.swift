import Foundation
import Testing

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

@testable import HuggingFace

// MARK: - Request Handler Storage

/// Stores and manages handlers for MockURLProtocol's request handling.
private final class RequestHandlerStorage: @unchecked Sendable {
    private let lock = NSLock()
    private var requestHandler: (@Sendable (URLRequest) throws -> (HTTPURLResponse, Data))?

    func setHandler(
        _ handler: @Sendable @escaping (URLRequest) throws -> (HTTPURLResponse, Data)
    ) {
        lock.lock()
        requestHandler = handler
        lock.unlock()
    }

    func clearHandler() {
        lock.lock()
        requestHandler = nil
        lock.unlock()
    }

    func executeHandler(for request: URLRequest) throws -> (HTTPURLResponse, Data) {
        lock.lock()
        let handler = requestHandler
        lock.unlock()

        guard let handler else {
            throw NSError(
                domain: "MockURLProtocolError",
                code: 0,
                userInfo: [NSLocalizedDescriptionKey: "No request handler set"]
            )
        }
        return try handler(request)
    }
}

// MARK: - Mock URL Protocol

/// Custom URLProtocol for testing network requests
final class MockURLProtocol: URLProtocol {
    /// Storage for request handlers
    fileprivate static let requestHandlerStorage = RequestHandlerStorage()

    /// Set a handler to process mock requests
    static func setHandler(
        _ handler: @Sendable @escaping (URLRequest) throws -> (HTTPURLResponse, Data)
    ) async {
        requestHandlerStorage.setHandler(handler)
    }

    override class func canInit(with request: URLRequest) -> Bool {
        return true
    }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest {
        return request
    }

    override func startLoading() {
        do {
            let (response, data) = try Self.requestHandlerStorage.executeHandler(for: self.request)
            self.client?.urlProtocol(
                self,
                didReceive: response,
                cacheStoragePolicy: .notAllowed
            )
            self.client?.urlProtocol(self, didLoad: data)
            self.client?.urlProtocolDidFinishLoading(self)
        } catch {
            self.client?.urlProtocol(self, didFailWithError: error)
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
    private actor MockURLProtocolLock {
        static let shared = MockURLProtocolLock()

        private var waiters: [CheckedContinuation<Void, Never>] = []
        private var isLocked = false

        private init() {}

        func acquire() async {
            if isLocked {
                await withCheckedContinuation { continuation in
                    waiters.append(continuation)
                }
            } else {
                isLocked = true
            }
        }

        func release() {
            if let next = waiters.first {
                waiters.removeFirst()
                next.resume()
            } else {
                isLocked = false
            }
        }

        func withLock<T: Sendable>(_ operation: @Sendable () async throws -> T) async rethrows -> T {
            await acquire()
            do {
                let result = try await operation()
                release()
                return result
            } catch {
                release()
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
                MockURLProtocol.requestHandlerStorage.clearHandler()

                // Execute the test
                try await function()

                // Clear handler after test
                MockURLProtocol.requestHandlerStorage.clearHandler()
            }
        }
    }

    extension Trait where Self == MockURLSessionTestTrait {
        static var mockURLSession: Self { Self() }
    }

#endif  // swift(>=6.1)
