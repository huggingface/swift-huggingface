#if swift(>=6.1) && canImport(Network) && HUGGINGFACE_ENABLE_XET
    import Foundation
    import Network
    import Testing
    import Xet

    @testable import HuggingFace

    @Suite("Bucket download tests")
    struct BucketDownloadTests {
        private static let resolvePath = "/hf/buckets/org/bucket/resolve/macos/weights%20file.bin"
        private static let refreshPath = "/hf/api/buckets/org/bucket/xet-read-token"

        @Test("Downloads by path resolve Xet metadata, then request a token", arguments: ["/hf", "/hf/"])
        func downloadByPath(prefix: String) async throws {
            let server = try BucketDownloadServer(routes: [
                "HEAD \(Self.resolvePath)": (
                    "200 OK", "X-Xet-Hash: \(String(repeating: "a", count: 64))\r\nX-Linked-Size: 16\r\n"
                ),
                // A permanent error stops Xet before any CAS request.
                "GET \(Self.refreshPath)": ("401 Unauthorized", ""),
            ])
            defer { server.stop() }
            let port = try await server.start()
            let (client, session) = try makeClient(port: port, prefix: prefix)
            defer { session.invalidateAndCancel() }

            await #expect {
                try await client.downloadBucketFile(
                    at: "macos/weights file.bin",
                    in: "org/bucket",
                    to: temporaryDestination()
                )
            } throws: { error in
                let error = error as? XetDownloaderError
                return error?.code == .tokenRequestFailed && error?.statusCode == 401
            }
            #expect(server.requests == ["HEAD \(Self.resolvePath)", "GET \(Self.refreshPath)"])
        }

        @Test("Downloads by path report the status of a failed resolve")
        func downloadByPathNotFound() async throws {
            let server = try BucketDownloadServer(routes: [:])
            defer { server.stop() }
            let port = try await server.start()
            let (client, session) = try makeClient(port: port, prefix: "/hf")
            defer { session.invalidateAndCancel() }

            await #expect {
                try await client.downloadBucketFile(
                    at: "macos/weights file.bin",
                    in: "org/bucket",
                    to: temporaryDestination()
                )
            } throws: { error in
                let error = error as? HTTPClientError
                return error?.code == .responseError && error?.statusCode == 404
            }
            #expect(server.requests == ["HEAD \(Self.resolvePath)"])
        }

        @Test("Downloads of a listed file skip the resolve request")
        func downloadListedFile() async throws {
            let server = try BucketDownloadServer(routes: [
                "GET \(Self.refreshPath)": ("401 Unauthorized", "")
            ])
            defer { server.stop() }
            let port = try await server.start()
            let (client, session) = try makeClient(port: port, prefix: "/hf")
            defer { session.invalidateAndCancel() }

            let json = Data(
                """
                {"type": "file", "path": "macos/weights file.bin", "size": 16, "xetHash": "\(String(repeating: "a", count: 64))"}
                """.utf8
            )
            let file = try JSONDecoder().decode(Bucket.File.self, from: json)

            await #expect {
                try await client.downloadBucketFile(file, in: "org/bucket", to: temporaryDestination())
            } throws: { error in
                let error = error as? XetDownloaderError
                return error?.code == .tokenRequestFailed && error?.statusCode == 401
            }
            #expect(server.requests == ["GET \(Self.refreshPath)"])
        }

        private func makeClient(port: UInt16, prefix: String) throws -> (HubClient, URLSession) {
            let session = URLSession(configuration: .ephemeral)
            let client = HubClient(
                session: session,
                host: try #require(URL(string: "http://127.0.0.1:\(port)\(prefix)")),
                tokenProvider: .none,
                cache: nil
            )
            return (client, session)
        }

        private func temporaryDestination() -> URL {
            FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        }
    }

    /// Answers each request from a table of status lines and headers, and 404 otherwise.
    private final class BucketDownloadServer: @unchecked Sendable {
        private let listener: NWListener
        private let queue = DispatchQueue(label: "BucketDownloadServer")
        private let routes: [String: (status: String, headers: String)]
        private let lock = NSLock()
        private var recordedRequests: [String] = []

        init(routes: [String: (status: String, headers: String)]) throws {
            self.routes = routes
            let parameters = NWParameters.tcp
            parameters.requiredLocalEndpoint = .hostPort(host: "127.0.0.1", port: .any)
            listener = try NWListener(using: parameters)
        }

        var requests: [String] {
            lock.lock()
            defer { lock.unlock() }
            return recordedRequests
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

            let (status, headers) = routes[request] ?? ("404 Not Found", "")
            let response = Data("HTTP/1.1 \(status)\r\n\(headers)Content-Length: 0\r\nConnection: close\r\n\r\n".utf8)
            connection.send(content: response, completion: .contentProcessed { _ in connection.cancel() })
        }
    }
#endif
