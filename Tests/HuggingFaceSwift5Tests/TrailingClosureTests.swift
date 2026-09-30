import Foundation
import Testing

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

import HuggingFace

/// These tests build in Swift 5 language mode, where an unlabeled trailing
/// closure can bind to the last closure parameter instead of the first.
/// Existing calls like `downloadSnapshot(of:) { _ in ... }` must keep
/// binding to `progressHandler`, not `fileProgressHandler`.
@Suite("downloadSnapshot trailing closures in Swift 5 mode", .serialized)
struct TrailingClosureTests {
    @Test("An unlabeled trailing closure binds to progressHandler")
    func unlabeledTrailingClosureBindsToProgressHandler() async throws {
        let (client, cacheDirectory) = makeClient()
        defer { try? FileManager.default.removeItem(at: cacheDirectory) }

        let recorder = ArgumentRecorder()
        // The closure type-checks against either handler, so only the
        // overload's parameter list decides which one it binds to.
        _ = try await client.downloadSnapshot(of: "user/model") { value in
            recorder.record(value)
        }

        #expect(!recorder.values.isEmpty)
        #expect(recorder.values.allSatisfy { $0 is Progress })
    }

    @Test("An unlabeled trailing closure binds to progressHandler with a destination")
    func unlabeledTrailingClosureBindsToProgressHandlerWithDestination() async throws {
        let (client, cacheDirectory) = makeClient()
        defer { try? FileManager.default.removeItem(at: cacheDirectory) }
        let destination = FileManager.default.temporaryDirectory
            .appendingPathComponent("hf-swift5-dest-\(UUID().uuidString)", isDirectory: true)
        defer { try? FileManager.default.removeItem(at: destination) }

        let recorder = ArgumentRecorder()
        _ = try await client.downloadSnapshot(of: "user/model", to: destination) { value in
            recorder.record(value)
        }

        #expect(!recorder.values.isEmpty)
        #expect(recorder.values.allSatisfy { $0 is Progress })
    }

    private func makeClient() -> (HubClient, URL) {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [SnapshotURLProtocol.self]
        let cacheDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent("hf-swift5-cache-\(UUID().uuidString)", isDirectory: true)
        let client = HubClient(
            session: URLSession(configuration: configuration),
            host: URL(string: "https://huggingface.co")!,
            userAgent: "TestClient/1.0",
            bearerToken: "test_token",
            cache: HubCache(cacheDirectory: cacheDirectory)
        )
        return (client, cacheDirectory)
    }
}

private final class ArgumentRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var _values: [Any] = []

    func record(_ value: Any) {
        lock.lock()
        _values.append(value)
        lock.unlock()
    }

    var values: [Any] {
        lock.lock()
        defer { lock.unlock() }
        return _values
    }
}

/// Serves a one-file repository: its tree listing and `config.json`.
private final class SnapshotURLProtocol: URLProtocol {
    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func stopLoading() {}

    override func startLoading() {
        let url = request.url!
        let status: Int
        var headers: [String: String] = [:]
        var body = Data()
        if url.path.hasPrefix("/api/models/user/model/tree/main") {
            status = 200
            headers["Content-Type"] = "application/json"
            body = Data(#"[{"path": "config.json", "type": "file", "oid": "abc", "size": 11}]"#.utf8)
        } else if url.path == "/user/model/resolve/main/config.json" {
            status = 200
            if request.httpMethod == "HEAD" {
                headers["ETag"] = "\"etag-config\""
                headers["X-Repo-Commit"] = "1234567890123456789012345678901234567890"
            } else {
                headers["Content-Type"] = "application/json"
                body = Data(#"{"ok":true}"#.utf8)
            }
        } else {
            status = 404
        }
        let response = HTTPURLResponse(url: url, statusCode: status, httpVersion: "HTTP/1.1", headerFields: headers)!
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: body)
        client?.urlProtocolDidFinishLoading(self)
    }
}
