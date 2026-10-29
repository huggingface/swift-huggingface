#if canImport(Network)
    import Foundation
    import Testing

    @testable import HuggingFace

    private final class RequestLog: @unchecked Sendable {
        private let lock = NSLock()
        private var storage: [String] = []

        func append(_ request: String) {
            lock.lock()
            storage.append(request)
            lock.unlock()
        }

        var requests: [String] {
            lock.lock()
            defer { lock.unlock() }
            return storage
        }

        func rangeOffsets() -> [Int] {
            requests.compactMap { Self.rangeOffset(in: $0) }
        }

        static func rangeOffset(in request: String) -> Int? {
            guard let range = request.lowercased().range(of: "range: bytes=") else { return nil }
            let tail = request[range.upperBound...]
            let digits = tail.prefix { $0.isNumber }
            return Int(digits)
        }
    }

    @Suite("Resumable downloads", .timeLimit(.minutes(1)))
    struct ResumableDownloadTests {
        private let commit = "1234567890123456789012345678901234567890"
        private let fullSize = 512 * 1024

        private func pattern(_ size: Int) -> Data {
            Data((0 ..< size).map { UInt8($0 % 251) })
        }

        private func temporaryDirectory() throws -> URL {
            let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
            return directory
        }

        private func client(host: URL, cache: HubCache? = nil) -> HubClient {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.timeoutIntervalForRequest = 10
            return HubClient(
                session: URLSession(configuration: configuration),
                host: host,
                tokenProvider: .none,
                cache: cache
            )
        }

        private func waitForIntermediateProgress(_ progress: Progress) async throws {
            for _ in 0 ..< 150 {
                if progress.completedUnitCount > 0, progress.completedUnitCount < progress.totalUnitCount {
                    return
                }
                try await Task.sleep(for: .milliseconds(20))
            }
            Issue.record("No intermediate progress: \(progress.completedUnitCount)/\(progress.totalUnitCount)")
        }

        private func seedPartial(_ data: Data, cache: HubCache) throws -> URL {
            let incomplete = try cache.incompleteBlobPath(repo: "test/model", kind: .model, etag: "weights")
            try FileManager.default.createDirectory(
                at: incomplete.deletingLastPathComponent(),
                withIntermediateDirectories: true
            )
            try data.write(to: incomplete)
            return incomplete
        }

        /// Starts a download against a slow server, cancels it partway, and
        /// returns the size of the incomplete blob the cancellation left.
        private func cancelPartway(
            host: URL,
            cache: HubCache,
            directory: URL
        ) async throws -> Int {
            let client = client(host: host, cache: cache)
            let progress = Progress(totalUnitCount: 0)
            let download = Task {
                try await client.downloadFile(
                    at: "weights.bin",
                    from: "test/model",
                    to: directory.appendingPathComponent("weights.bin"),
                    progress: progress,
                    transport: .lfs
                )
            }
            try await waitForIntermediateProgress(progress)
            download.cancel()
            do {
                _ = try await download.value
                Issue.record("Canceled download succeeded")
            } catch {
                #expect(error is CancellationError)
            }
            let incomplete = try cache.incompleteBlobPath(repo: "test/model", kind: .model, etag: "weights")
            #expect(FileManager.default.fileExists(atPath: incomplete.path))
            let data = try Data(contentsOf: incomplete)
            #expect(data.count > 0)
            #expect(data.count < fullSize)
            #expect(data == pattern(fullSize).prefix(data.count))
            return data.count
        }

        private func makeServer(
            log: RequestLog,
            honorRange: Bool,
            slow: Bool = true
        ) throws -> SlowDownloadServer {
            let body = pattern(fullSize)
            let commit = commit
            return try SlowDownloadServer { request in
                log.append(request)
                if request.hasPrefix("HEAD ") {
                    return .init(
                        headers: ["ETag": "weights", "X-Repo-Commit": commit],
                        body: Data(),
                        delay: 0,
                        declaredSize: body.count
                    )
                }
                if honorRange, let offset = RequestLog.rangeOffset(in: request), offset > 0, offset < body.count {
                    return .init(
                        status: 206,
                        headers: ["Content-Range": "bytes \(offset)-\(body.count - 1)/\(body.count)"],
                        body: body.subdata(in: offset ..< body.count),
                        delay: slow ? 0.05 : 0
                    )
                }
                return .init(body: body, delay: slow ? 0.05 : 0)
            }
        }

        @Test("A cancelled download leaves an incomplete file of the received size")
        func cancelledDownloadLeavesIncomplete() async throws {
            let log = RequestLog()
            let server = try makeServer(log: log, honorRange: true)
            let host = try await server.start()
            defer { server.stop() }
            let directory = try temporaryDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let cache = HubCache(cacheDirectory: directory.appendingPathComponent("cache"))
            let received = try await cancelPartway(host: host, cache: cache, directory: directory)
            #expect(received > 0)
            #expect(received < fullSize)
        }

        @Test("The next call sends Range with the received offset and completes")
        func resumeSendsRangeAndCompletes() async throws {
            let log = RequestLog()
            let server = try makeServer(log: log, honorRange: true)
            let host = try await server.start()
            defer { server.stop() }
            let directory = try temporaryDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let cache = HubCache(cacheDirectory: directory.appendingPathComponent("cache"))
            let received = try await cancelPartway(host: host, cache: cache, directory: directory)

            let file = try await client(host: host, cache: cache).downloadFile(
                at: "weights.bin",
                from: "test/model",
                to: directory.appendingPathComponent("weights.bin"),
                transport: .lfs
            )
            #expect(log.rangeOffsets().contains(received), "Range offsets: \(log.rangeOffsets()), expected \(received)")
            #expect(try Data(contentsOf: file) == pattern(fullSize))
            let incomplete = try cache.incompleteBlobPath(repo: "test/model", kind: .model, etag: "weights")
            #expect(!FileManager.default.fileExists(atPath: incomplete.path))
        }

        @Test("A server that ignores Range restarts cleanly")
        func rangeIgnoredRestartsCleanly() async throws {
            let log = RequestLog()
            let server = try makeServer(log: log, honorRange: false, slow: false)
            let host = try await server.start()
            defer { server.stop() }
            let directory = try temporaryDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let cache = HubCache(cacheDirectory: directory.appendingPathComponent("cache"))
            let incomplete = try cache.incompleteBlobPath(repo: "test/model", kind: .model, etag: "weights")
            try FileManager.default.createDirectory(
                at: incomplete.deletingLastPathComponent(),
                withIntermediateDirectories: true
            )
            // Partial bytes that differ from the real prefix, so an append would corrupt the result.
            try Data(repeating: 0xFF, count: 128 * 1024).write(to: incomplete)

            let file = try await client(host: host, cache: cache).downloadFile(
                at: "weights.bin",
                from: "test/model",
                to: directory.appendingPathComponent("weights.bin"),
                transport: .lfs
            )
            #expect(log.rangeOffsets().contains(128 * 1024))
            #expect(try Data(contentsOf: file) == pattern(fullSize))
        }

        @Test(
            "Invalid resumed ranges restart from zero",
            arguments: [
                "missing", "bytes 0-511/512", "bytes 128-127/512", "bytes 128-512/512", "bytes 128-511/*",
            ]
        )
        func invalidRangeRestarts(contentRange: String) async throws {
            let body = pattern(512)
            let log = RequestLog()
            let server = try SlowDownloadServer { [commit] request in
                log.append(request)
                if request.hasPrefix("HEAD ") {
                    return .init(
                        headers: ["ETag": "weights", "X-Repo-Commit": commit],
                        body: Data(),
                        delay: 0,
                        declaredSize: body.count
                    )
                }
                if RequestLog.rangeOffset(in: request) != nil {
                    return .init(
                        status: 206,
                        headers: contentRange == "missing" ? [:] : ["Content-Range": contentRange],
                        body: body,
                        delay: 0
                    )
                }
                return .init(body: body, delay: 0)
            }
            let host = try await server.start()
            defer { server.stop() }
            let directory = try temporaryDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let cache = HubCache(cacheDirectory: directory.appendingPathComponent("cache"))
            let incomplete = try seedPartial(Data(repeating: 0xFF, count: 128), cache: cache)
            let file = try await client(host: host, cache: cache).downloadFile(
                at: "weights.bin",
                from: "test/model",
                transport: .lfs
            )
            #expect(log.rangeOffsets() == [128])
            #expect(log.requests.filter { $0.hasPrefix("GET ") }.count == 2)
            #expect(try Data(contentsOf: file) == body)
            #expect(!FileManager.default.fileExists(atPath: incomplete.path))
        }

        @Test(
            "Short or changed files are never published",
            arguments: ["shortRange", "shortRangeWithoutHeadSize", "shortRangeBody", "changedTotal", "shortBody"]
        )
        func invalidSizeIsNotPublished(mode: String) async throws {
            let body = pattern(512)
            let server = try SlowDownloadServer { [commit] request in
                if request.hasPrefix("HEAD ") {
                    return .init(
                        headers: ["ETag": "weights", "X-Repo-Commit": commit],
                        body: Data(),
                        delay: 0,
                        declaredSize: body.count,
                        includesLength: mode != "shortRangeWithoutHeadSize"
                    )
                }
                if mode == "shortBody" {
                    return .init(body: Data(body.prefix(256)), delay: 0)
                }
                return .init(
                    status: 206,
                    headers: [
                        "Content-Range": mode == "changedTotal"
                            ? "bytes 128-255/256"
                            : (mode == "shortRangeBody" ? "bytes 128-511/512" : "bytes 128-255/512")
                    ],
                    body: body.subdata(in: 128 ..< 256),
                    delay: 0
                )
            }
            let host = try await server.start()
            defer { server.stop() }
            let directory = try temporaryDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let cache = HubCache(cacheDirectory: directory.appendingPathComponent("cache"))
            let incomplete = try seedPartial(Data(body.prefix(128)), cache: cache)
            let progress = Progress(totalUnitCount: 0)
            await #expect(throws: StreamingDownloadError.sizeMismatch) {
                try await client(host: host, cache: cache).downloadFile(
                    at: "weights.bin",
                    from: "test/model",
                    progress: progress,
                    transport: .lfs
                )
            }
            let blob = try cache.blobPath(repo: "test/model", kind: .model, etag: "weights")
            #expect(!FileManager.default.fileExists(atPath: blob.path))
            #expect(!FileManager.default.fileExists(atPath: incomplete.path))
            #expect(
                cache.cachedFilePath(repo: "test/model", kind: .model, revision: "main", filename: "weights.bin") == nil
            )
            #expect(!progress.isFinished)
        }

        @Test("GET uses the preflight commit when the branch changes")
        func movingRevisionIsPinned() async throws {
            let body = pattern(512)
            let log = RequestLog()
            let server = try SlowDownloadServer { [commit] request in
                log.append(request)
                if request.hasPrefix("HEAD ") {
                    return .init(
                        headers: ["ETag": "weights", "X-Repo-Commit": commit],
                        body: Data(),
                        delay: 0,
                        declaredSize: body.count
                    )
                }
                let pinned = request.hasPrefix("GET /test/model/resolve/\(commit)/weights.bin ")
                let bytes = pinned ? body : Data(repeating: 0xFF, count: body.count)
                return .init(
                    status: 206,
                    headers: ["ETag": pinned ? "weights" : "changed", "Content-Range": "bytes 128-511/512"],
                    body: bytes.subdata(in: 128 ..< 512),
                    delay: 0
                )
            }
            let host = try await server.start()
            defer { server.stop() }
            let directory = try temporaryDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let cache = HubCache(cacheDirectory: directory.appendingPathComponent("cache"))
            _ = try seedPartial(Data(body.prefix(128)), cache: cache)
            let file = try await client(host: host, cache: cache).downloadFile(
                at: "weights.bin",
                from: "test/model",
                transport: .lfs
            )
            #expect(try Data(contentsOf: file) == body)
            #expect(log.requests.contains { $0.hasPrefix("HEAD /test/model/resolve/main/weights.bin ") })
            #expect(log.requests.contains { $0.hasPrefix("GET /test/model/resolve/\(commit)/weights.bin ") })
            #expect(log.requests.contains { $0.lowercased().contains("if-range: \"weights\"") })
        }

        @Test("Changed response identity discards the partial file and retries", arguments: ["etag", "commit"])
        func identityMismatchRestarts(field: String) async throws {
            let body = pattern(512)
            let log = RequestLog()
            let server = try SlowDownloadServer { [commit] request in
                log.append(request)
                if request.hasPrefix("HEAD ") {
                    return .init(
                        headers: ["ETag": "weights", "X-Repo-Commit": commit],
                        body: Data(),
                        delay: 0,
                        declaredSize: body.count
                    )
                }
                if RequestLog.rangeOffset(in: request) != nil {
                    return .init(
                        status: 206,
                        headers: [
                            "ETag": field == "etag" ? "changed" : "weights",
                            "X-Repo-Commit": field == "commit" ? String(repeating: "a", count: 40) : commit,
                            "Content-Range": "bytes 128-511/512",
                        ],
                        body: Data(repeating: 0xFF, count: 384),
                        delay: 0
                    )
                }
                return .init(headers: ["ETag": "weights", "X-Repo-Commit": commit], body: body, delay: 0)
            }
            let host = try await server.start()
            defer { server.stop() }
            let directory = try temporaryDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let cache = HubCache(cacheDirectory: directory.appendingPathComponent("cache"))
            _ = try seedPartial(Data(repeating: 0xEE, count: 128), cache: cache)
            let file = try await client(host: host, cache: cache).downloadFile(
                at: "weights.bin",
                from: "test/model",
                transport: .lfs
            )
            #expect(try Data(contentsOf: file) == body)
            #expect(log.requests.filter { $0.hasPrefix("GET ") }.count == 2)
            #expect(log.rangeOffsets() == [128])
        }

        @Test("Persistent identity mismatch fails without caching changed bytes", arguments: [false, true])
        func persistentIdentityMismatchFails(redirect: Bool) async throws {
            let server = try SlowDownloadServer { [commit] request in
                .init(
                    status: redirect ? 302 : 200,
                    headers: [
                        redirect ? "X-Linked-Etag" : "ETag": request.hasPrefix("HEAD ") ? "weights" : "changed",
                        "X-Repo-Commit": commit,
                        "X-Linked-Size": "512",
                        "Location": "http://localhost:1/should-not-be-requested",
                    ],
                    body: request.hasPrefix("HEAD ") ? Data() : Data(repeating: 0xFF, count: 512),
                    delay: 0,
                    declaredSize: 512
                )
            }
            let host = try await server.start()
            defer { server.stop() }
            let directory = try temporaryDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let cache = HubCache(cacheDirectory: directory.appendingPathComponent("cache"))
            let incomplete = try seedPartial(pattern(128), cache: cache)
            await #expect(throws: StreamingDownloadError.identityMismatch) {
                try await client(host: host, cache: cache).downloadFile(
                    at: "weights.bin",
                    from: "test/model",
                    transport: .lfs
                )
            }
            let blob = try cache.blobPath(repo: "test/model", kind: .model, etag: "weights")
            #expect(!FileManager.default.fileExists(atPath: blob.path))
            #expect(!FileManager.default.fileExists(atPath: incomplete.path))
        }

        @Test("CDN redirects retain the Hub size and tolerate different CDN ETags", arguments: [false, true])
        func cdnRedirect(linked: Bool) async throws {
            let body = pattern(512)
            let cdn = try SlowDownloadServer { request in
                #expect(!request.lowercased().contains("if-range:"))
                #expect(RequestLog.rangeOffset(in: request) == 128)
                return .init(
                    status: 206,
                    headers: ["ETag": "opaque-cdn-validator", "Content-Range": "bytes 128-511/512"],
                    body: body.subdata(in: 128 ..< 512),
                    delay: 0
                )
            }
            let cdnHost = try await cdn.start()
            defer { cdn.stop() }
            let location = cdnHost.absoluteString.replacingOccurrences(of: "127.0.0.1", with: "localhost") + "/file"
            let server = try SlowDownloadServer { [commit] request in
                if request.hasPrefix("GET ") {
                    #expect(request.contains("/resolve/\(commit)/"))
                }
                var headers = ["Location": location, "X-Repo-Commit": commit]
                headers[linked ? "X-Linked-Etag" : "ETag"] = "weights"
                headers["X-Linked-Size"] = "512"
                return .init(status: 302, headers: headers, body: Data(), delay: 0)
            }
            let host = try await server.start()
            defer { server.stop() }
            let directory = try temporaryDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let cache = HubCache(cacheDirectory: directory.appendingPathComponent("cache"))
            _ = try seedPartial(Data(body.prefix(128)), cache: cache)
            let file = try await client(host: host, cache: cache).downloadFile(
                at: "weights.bin",
                from: "test/model",
                transport: .lfs
            )
            #expect(try Data(contentsOf: file) == body)
        }

        @Test("A CDN response without a pinned or comparable identity restarts from zero")
        func unverifiedCdnResumeRestarts() async throws {
            let body = pattern(512)
            let log = RequestLog()
            let cdn = try SlowDownloadServer { [commit] request in
                log.append(request)
                if RequestLog.rangeOffset(in: request) != nil {
                    return .init(
                        status: 206,
                        headers: ["Content-Range": "bytes 128-511/512", "ETag": "opaque-cdn-validator"],
                        body: Data(repeating: 0xFF, count: 384),
                        delay: 0
                    )
                }
                return .init(headers: ["ETag": "opaque-cdn-validator", "X-Repo-Commit": commit], body: body, delay: 0)
            }
            let cdnHost = try await cdn.start()
            defer { cdn.stop() }
            let location = cdnHost.absoluteString.replacingOccurrences(of: "127.0.0.1", with: "localhost") + "/file"
            let server = try SlowDownloadServer { _ in
                .init(
                    status: 302,
                    headers: ["Location": location, "X-Linked-Etag": "weights", "X-Linked-Size": "512"],
                    body: Data(),
                    delay: 0
                )
            }
            let host = try await server.start()
            defer { server.stop() }
            let directory = try temporaryDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let cache = HubCache(cacheDirectory: directory.appendingPathComponent("cache"))
            _ = try seedPartial(Data(repeating: 0xEE, count: 128), cache: cache)
            let file = try await client(host: host, cache: cache).downloadFile(
                at: "weights.bin",
                from: "test/model",
                to: directory.appendingPathComponent("weights.bin"),
                transport: .lfs
            )
            #expect(try Data(contentsOf: file) == body)
            #expect(log.rangeOffsets() == [128])
            #expect(log.requests.count == 2)
            let blob = try cache.blobPath(repo: "test/model", kind: .model, etag: "weights")
            #expect(!FileManager.default.fileExists(atPath: blob.path))
        }

        @Test("No-etag cancellation removes the operation's temporary file")
        func noEtagCancellationCleansUp() async throws {
            let server = try SlowDownloadServer()
            let host = try await server.start()
            defer { server.stop() }
            let directory = try temporaryDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let tempDirectory = directory.appendingPathComponent("temporary")
            try FileManager.default.createDirectory(at: tempDirectory, withIntermediateDirectories: true)
            try await HubClient.$downloadTemporaryDirectory.withValue(tempDirectory) {
                let progress = Progress(totalUnitCount: 0)
                let download = Task {
                    try await client(host: host).downloadFile(
                        at: "weights.bin",
                        from: "test/model",
                        to: directory.appendingPathComponent("weights.bin"),
                        progress: progress,
                        transport: .lfs
                    )
                }
                try await waitForIntermediateProgress(progress)
                let files = try FileManager.default.contentsOfDirectory(atPath: tempDirectory.path)
                #expect(files.count == 1)
                #expect(files.first?.hasPrefix("hf-download-") == true)
                download.cancel()
                await #expect(throws: CancellationError.self) { try await download.value }
            }
            #expect(try FileManager.default.contentsOfDirectory(atPath: tempDirectory.path).isEmpty)
        }

        @Test(
            "Destination errors after transfer remove owned temporary files",
            arguments: ["missing", "parent", "move"]
        )
        func destinationErrorCleansUp(mode: String) async throws {
            let server = try SlowDownloadServer { _ in .init(body: Data([1, 2, 3]), delay: 0) }
            let host = try await server.start()
            defer { server.stop() }
            let directory = try temporaryDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let tempDirectory = directory.appendingPathComponent("temporary")
            try FileManager.default.createDirectory(at: tempDirectory, withIntermediateDirectories: true)
            let parentFile = directory.appendingPathComponent("file")
            try Data([0]).write(to: parentFile)
            let destination: URL? =
                switch mode {
                case "parent": parentFile.appendingPathComponent("weights.bin")
                case "move": directory.appendingPathComponent(String(repeating: "x", count: 300))
                default: nil
                }
            let progress = Progress(totalUnitCount: 0)
            await HubClient.$downloadTemporaryDirectory.withValue(tempDirectory) {
                await #expect(throws: (any Error).self) {
                    try await client(host: host).downloadFile(
                        at: "weights.bin",
                        from: "test/model",
                        to: destination,
                        progress: progress,
                        transport: .lfs
                    )
                }
            }
            // The complete body arrived before the destination operation failed.
            #expect(progress.completedUnitCount == 3)
            #expect(try FileManager.default.contentsOfDirectory(atPath: tempDirectory.path).isEmpty)
        }
    }
#endif
