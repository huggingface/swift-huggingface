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

        private func client(host: URL, cache: HubCache) -> HubClient {
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

        private func downloadTempFiles() -> Set<String> {
            let contents =
                (try? FileManager.default.contentsOfDirectory(atPath: FileManager.default.temporaryDirectory.path))
                ?? []
            return Set(contents.filter { $0.hasPrefix("CFNetworkDownload_") || $0.hasPrefix("hf-download-") })
        }

        private func newDownloadTempFiles(since before: Set<String>) async -> Set<String> {
            for _ in 0 ..< 20 {
                let new = downloadTempFiles().subtracting(before)
                if new.isEmpty { return new }
                try? await Task.sleep(for: .milliseconds(100))
            }
            return downloadTempFiles().subtracting(before)
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
                #expect(error is CancellationError || (error as? URLError)?.code == .cancelled)
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
                        delay: 0
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

        @Test("Cancellation leaves no temporary download file behind")
        func cancellationLeavesNoTempFile() async throws {
            let log = RequestLog()
            let server = try makeServer(log: log, honorRange: true)
            let host = try await server.start()
            defer { server.stop() }
            let directory = try temporaryDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let cache = HubCache(cacheDirectory: directory.appendingPathComponent("cache"))
            let before = downloadTempFiles()
            _ = try await cancelPartway(host: host, cache: cache, directory: directory)
            let leaked = await newDownloadTempFiles(since: before)
            #expect(leaked.isEmpty, "Leaked download temp files: \(leaked)")
        }
    }
#endif
