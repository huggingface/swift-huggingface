#if swift(>=6.1) && canImport(Network) && HUGGINGFACE_ENABLE_XET
    import Foundation
    import Network
    import Testing
    import Xet

    @testable import HuggingFace

    @Suite("Xet endpoint tests")
    struct XetEndpointTests {
        @Test(
            "Downloads preserve endpoint prefixes and encode revisions",
            arguments: [
                (Repo.Kind.model, "", "models"),
                (Repo.Kind.dataset, "/datasets", "datasets"),
                (Repo.Kind.space, "/spaces", "spaces"),
            ],
            ["/hf", "/hf/"]
        )
        func customEndpoint(location: (Repo.Kind, String, String), prefix: String) async throws {
            let (kind, repositoryPrefix, apiKind) = location
            let resolvePath = "/hf\(repositoryPrefix)/org/repo/resolve/refs%2Fpr%2F123/macos/weights%20file.bin"
            let refreshPath = "/hf/api/\(apiKind)/org/repo/xet-read-token/refs%2Fpr%2F123"
            let server = try XetEndpointServer(resolvePath: resolvePath, refreshPath: refreshPath)
            defer { server.stop() }
            let port = try await server.start()

            let session = URLSession(configuration: .ephemeral)
            defer { session.invalidateAndCancel() }
            let client = HubClient(
                session: session,
                host: try #require(URL(string: "http://127.0.0.1:\(port)\(prefix)")),
                tokenProvider: .none,
                cache: nil
            )
            let destination = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            defer { try? FileManager.default.removeItem(at: destination) }

            for transport in [FileDownloadTransport.automatic, .xet] {
                for downloadToFile in [false, true] {
                    server.clearRequests()
                    do {
                        let data: Data
                        if downloadToFile {
                            let file = try await client.downloadFile(
                                at: "macos/weights file.bin",
                                from: "org/repo",
                                to: destination,
                                kind: kind,
                                revision: "refs/pr/123",
                                transport: transport
                            )
                            data = try Data(contentsOf: file)
                        } else {
                            data = try await client.downloadContentsOfFile(
                                at: "macos/weights file.bin",
                                from: "org/repo",
                                kind: kind,
                                revision: "refs/pr/123",
                                transport: transport
                            )
                        }
                        #expect(transport == .automatic)
                        #expect(data == XetEndpointServer.payload)
                    } catch let error as XetDownloaderError {
                        #expect(transport == .xet)
                        guard case .tokenRequestFailed(statusCode: 401, body: _) = error else {
                            throw error
                        }
                    }

                    var expected = ["HEAD \(resolvePath)", "GET \(refreshPath)"]
                    if transport == .automatic {
                        expected.append("GET \(resolvePath)")
                    }
                    // Exact request targets also reject requests outside the configured prefix.
                    #expect(server.requests == expected)
                }
            }
        }
    }

    /// Serves metadata and rejects token refresh to test HTTP fallback without a CAS server.
    private final class XetEndpointServer: @unchecked Sendable {
        static let payload = Data("endpoint fixture".utf8)

        private let listener: NWListener
        private let queue = DispatchQueue(label: "XetEndpointServer")
        private let resolvePath: String
        private let refreshPath: String
        private let lock = NSLock()
        private var recordedRequests: [String] = []

        init(resolvePath: String, refreshPath: String) throws {
            self.resolvePath = resolvePath
            self.refreshPath = refreshPath
            let parameters = NWParameters.tcp
            parameters.requiredLocalEndpoint = .hostPort(host: "127.0.0.1", port: .any)
            listener = try NWListener(using: parameters)
        }

        var requests: [String] {
            lock.lock()
            defer { lock.unlock() }
            return recordedRequests
        }

        func clearRequests() {
            lock.lock()
            defer { lock.unlock() }
            recordedRequests.removeAll()
        }

        func start() async throws -> UInt16 {
            try await withCheckedThrowingContinuation { continuation in
                listener.stateUpdateHandler = { [self] state in
                    switch state {
                    case .ready:
                        listener.stateUpdateHandler = nil
                        continuation.resume(returning: listener.port!.rawValue)
                    case .failed(let error):
                        listener.stateUpdateHandler = nil
                        continuation.resume(throwing: error)
                    default:
                        break
                    }
                }
                listener.newConnectionHandler = { [self] connection in
                    connection.start(queue: queue)
                    receiveRequest(on: connection)
                }
                listener.start(queue: queue)
            }
        }

        func stop() {
            listener.cancel()
            listener.newConnectionHandler = nil
            listener.stateUpdateHandler = nil
        }

        private func receiveRequest(on connection: NWConnection, buffer: Data = Data()) {
            connection.receive(minimumIncompleteLength: 1, maximumLength: 16 * 1024) { [self] data, _, done, error in
                let buffer = buffer + (data ?? Data())
                if let text = String(data: buffer, encoding: .utf8), text.contains("\r\n\r\n") {
                    let fields = text.components(separatedBy: "\r\n")[0].split(separator: " ")
                    guard fields.count == 3 else {
                        connection.cancel()
                        return
                    }
                    respond(to: "\(fields[0]) \(fields[1])", on: connection)
                } else if done || error != nil {
                    connection.cancel()
                } else {
                    receiveRequest(on: connection, buffer: buffer)
                }
            }
        }

        private func respond(to request: String, on connection: NWConnection) {
            lock.lock()
            recordedRequests.append(request)
            lock.unlock()

            let status: String
            var headers = ""
            var body = Data()
            switch request {
            case "HEAD \(resolvePath)":
                status = "200 OK"
                headers = "X-Xet-Hash: \(String(repeating: "a", count: 64))\r\nX-Linked-Size: 16777216\r\n"
            case "GET \(refreshPath)":
                // A permanent error stops Xet before any CAS request.
                status = "401 Unauthorized"
            case "GET \(resolvePath)":
                status = "200 OK"
                body = Self.payload
            default:
                status = "404 Not Found"
            }
            let response =
                Data(
                    ("HTTP/1.1 \(status)\r\n\(headers)Content-Length: \(body.count)\r\nConnection: close\r\n\r\n").utf8
                ) + body
            connection.send(content: response, completion: .contentProcessed { _ in connection.cancel() })
        }
    }
#endif
