#if canImport(Network)
    import Foundation
    import Network

    /// Serves synthetic HTTP responses on loopback without external dependencies.
    final class SlowDownloadServer: @unchecked Sendable {
        struct Response: Sendable {
            var status = 200
            var headers: [String: String] = [:]
            var body = Data(repeating: 0xAB, count: 512 * 1024)
            var chunkSize = 32 * 1024
            var delay: TimeInterval = 0.1
            var declaredSize: Int? = nil
            var includesLength = true
        }

        private let listener: NWListener
        // All connection state and callbacks run on this serial queue.
        private let queue = DispatchQueue(label: "SlowDownloadServer")
        private var connections: [NWConnection] = []
        private let handler: @Sendable (String) -> Response

        init(handler: @escaping @Sendable (String) -> Response = { _ in Response() }) throws {
            let parameters = NWParameters.tcp
            parameters.requiredLocalEndpoint = .hostPort(host: "127.0.0.1", port: .any)
            listener = try NWListener(using: parameters)
            self.handler = handler
        }

        func start() async throws -> URL {
            try await withCheckedThrowingContinuation { continuation in
                listener.stateUpdateHandler = { [self] state in
                    switch state {
                    case .ready:
                        listener.stateUpdateHandler = nil
                        continuation.resume(returning: URL(string: "http://127.0.0.1:\(listener.port!.rawValue)")!)
                    case .failed(let error):
                        listener.stateUpdateHandler = nil
                        continuation.resume(throwing: error)
                    default:
                        break
                    }
                }
                listener.newConnectionHandler = { [self] connection in
                    connections.append(connection)
                    connection.start(queue: queue)
                    receiveRequest(connection, data: Data())
                }
                listener.start(queue: queue)
            }
        }

        func stop() {
            queue.sync {
                listener.cancel()
                listener.newConnectionHandler = nil
                listener.stateUpdateHandler = nil
                for connection in connections {
                    connection.cancel()
                }
                connections.removeAll()
            }
        }

        private func receiveRequest(_ connection: NWConnection, data: Data) {
            connection.receive(minimumIncompleteLength: 1, maximumLength: 16 * 1024) { [self] bytes, _, done, error in
                var data = data
                data.append(bytes ?? Data())
                guard let request = String(data: data, encoding: .utf8), request.contains("\r\n\r\n") else {
                    if error == nil, !done {
                        receiveRequest(connection, data: data)
                    }
                    return
                }
                let response = handler(request)
                var headers = response.headers
                headers["Connection"] = "close"
                if response.includesLength {
                    headers["Content-Length"] = "\(response.declaredSize ?? response.body.count)"
                }
                let head =
                    "HTTP/1.1 \(response.status) Response\r\n"
                    + headers.map { "\($0.key): \($0.value)\r\n" }.joined() + "\r\n"
                connection.send(
                    content: Data(head.utf8),
                    completion: .contentProcessed { [self] error in
                        guard error == nil else { return }
                        if request.hasPrefix("HEAD ") {
                            connection.cancel()
                        } else {
                            sendBody(response, offset: 0, connection: connection)
                        }
                    }
                )
            }
        }

        private func sendBody(_ response: Response, offset: Int, connection: NWConnection) {
            guard offset < response.body.count else {
                connection.cancel()
                return
            }
            let end = min(offset + response.chunkSize, response.body.count)
            connection.send(
                content: response.body.subdata(in: offset ..< end),
                completion: .contentProcessed { [self] error in
                    guard error == nil else { return }
                    queue.asyncAfter(deadline: .now() + response.delay) { [self] in
                        sendBody(response, offset: end, connection: connection)
                    }
                }
            )
        }
    }
#endif
