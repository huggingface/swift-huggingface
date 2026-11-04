import Foundation
import Testing

@testable import HuggingFace

#if swift(>=6.1) && !canImport(FoundationNetworking)
    private final class Recorder: @unchecked Sendable {
        private let lock = NSLock()
        private var _messages: [String] = []

        func record(_ message: String) {
            lock.lock()
            _messages.append(message)
            lock.unlock()
        }

        var messages: [String] {
            lock.lock()
            defer { lock.unlock() }
            return _messages
        }
    }

    @Suite("Background Session Tests", .serialized)
    struct BackgroundSessionTests {
        private func makeClient(
            session: URLSession,
            warnings: Recorder,
            cache: HubCache? = nil
        ) -> HubClient {
            HubClient(
                session: session,
                host: URL(string: "https://huggingface.co")!,
                userAgent: "TestClient/1.0",
                tokenProvider: .fixed(token: "test_token"),
                cache: cache,
                warn: { warnings.record($0) }
            )
        }

        @Test("Background session falls back to a regular session", .mockURLSession)
        func testBackgroundSessionFallsBack() async throws {
            let configuration = URLSessionConfiguration.background(
                withIdentifier: "co.huggingface.tests.\(UUID().uuidString)"
            )
            configuration.protocolClasses = [MockURLProtocol.self]
            configuration.httpAdditionalHeaders = ["X-Test-Header": "background"]
            configuration.timeoutIntervalForRequest = 42
            configuration.timeoutIntervalForResource = 4200
            configuration.requestCachePolicy = .reloadIgnoringLocalCacheData
            configuration.allowsCellularAccess = false
            configuration.allowsConstrainedNetworkAccess = false
            configuration.waitsForConnectivity = true
            let backgroundSession = URLSession(configuration: configuration)
            defer { backgroundSession.invalidateAndCancel() }

            let cacheDirectory = FileManager.default.temporaryDirectory
                .appendingPathComponent("hf-cache-\(UUID().uuidString)", isDirectory: true)
            defer { try? FileManager.default.removeItem(at: cacheDirectory) }

            let warnings = Recorder()
            let client = makeClient(
                session: backgroundSession,
                warnings: warnings,
                cache: HubCache(cacheDirectory: cacheDirectory)
            )

            #expect(client.session !== backgroundSession)
            #expect(client.session.configuration.identifier == nil)
            #expect(client.metadataSession.configuration.identifier == nil)

            let copied = client.session.configuration
            #expect(copied.httpAdditionalHeaders?["X-Test-Header"] as? String == "background")
            #expect(copied.timeoutIntervalForRequest == 42)
            #expect(copied.timeoutIntervalForResource == 4200)
            #expect(copied.requestCachePolicy == .reloadIgnoringLocalCacheData)
            #expect(copied.allowsCellularAccess == false)
            #expect(copied.allowsConstrainedNetworkAccess == false)
            #expect(copied.waitsForConnectivity == true)

            let payload = Data("background fallback".utf8)
            let methods = Recorder()
            await MockURLProtocol.setHandler { request in
                methods.record(request.httpMethod ?? "")
                #expect(request.value(forHTTPHeaderField: "X-Test-Header") == "background")
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: [
                        "Content-Length": String(payload.count),
                        "ETag": "\"\(request.url!.lastPathComponent)\"",
                        "X-Repo-Commit": "0123456789abcdef0123456789abcdef01234567",
                    ]
                )!
                return (response, request.httpMethod == "HEAD" ? Data() : payload)
            }

            let contents = try await client.downloadContentsOfFile(at: "test.txt", from: "user/model")
            #expect(contents == payload)

            let destination = FileManager.default.temporaryDirectory
                .appendingPathComponent(UUID().uuidString)
            defer { try? FileManager.default.removeItem(at: destination) }
            let file = try await client.downloadFile(at: "other.txt", from: "user/model", to: destination)
            #expect(try Data(contentsOf: file) == payload)

            // The cache-aware download paths send a HEAD request for file metadata.
            #expect(methods.messages == ["HEAD", "GET", "HEAD", "GET"])

            #expect(warnings.messages.count == 1)
            #expect(warnings.messages.first?.contains("does not support background URL sessions") == true)
        }

        @Test("Regular session is used as is without a warning")
        func testRegularSessionIsUnchanged() {
            let session = URLSession(configuration: .ephemeral)
            defer { session.invalidateAndCancel() }

            let warnings = Recorder()
            let client = makeClient(session: session, warnings: warnings)

            #expect(client.session === session)
            #expect(warnings.messages.isEmpty)
        }
    }
#endif
