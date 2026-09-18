#if canImport(Network)
    import Foundation
    import Testing

    @testable import HuggingFace

    private final class DownloadProgressSamples: @unchecked Sendable {
        private let lock = NSLock()
        private var completed: [Int64] = []

        func record(_ progress: Progress) {
            lock.lock()
            completed.append(progress.completedUnitCount)
            lock.unlock()
        }

        var values: [Int64] {
            lock.lock()
            defer { lock.unlock() }
            return completed
        }
    }

    @Suite("HTTP download progress", .timeLimit(.minutes(1)))
    struct DownloadProgressTests {
        private let commit = "1234567890123456789012345678901234567890"

        private func temporaryDirectory() throws -> URL {
            let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
            return directory
        }

        private func client(host: URL, cache: HubCache? = nil) -> HubClient {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.timeoutIntervalForRequest = 5
            return HubClient(
                session: URLSession(configuration: configuration),
                host: host,
                tokenProvider: .none,
                cache: cache
            )
        }

        private func waitForIntermediateProgress(_ progress: Progress, above offset: Int64 = 0) async throws {
            for _ in 0 ..< 150 {
                if progress.completedUnitCount > offset, progress.completedUnitCount < progress.totalUnitCount {
                    return
                }
                try await Task.sleep(for: .milliseconds(20))
            }
            Issue.record(
                "No intermediate progress above \(offset): \(progress.completedUnitCount)/\(progress.totalUnitCount)"
            )
        }

        @Test("A slow 512 KiB HTTP download reports intermediate bytes")
        func slowDownload() async throws {
            let server = try SlowDownloadServer()
            let host = try await server.start()
            defer { server.stop() }
            let directory = try temporaryDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let client = client(host: host)
            let progress = Progress(totalUnitCount: 0)
            let samples = DownloadProgressSamples()
            let observation = progress.observe(\.fractionCompleted) { progress, _ in samples.record(progress) }
            defer { observation.invalidate() }
            async let download = client.downloadFile(
                at: "weights.bin",
                from: "test/model",
                to: directory.appendingPathComponent("weights.bin"),
                progress: progress,
                transport: .lfs
            )
            try await waitForIntermediateProgress(progress)
            let file = try await download
            #expect(try Data(contentsOf: file) == Data(repeating: 0xAB, count: 512 * 1024))
            #expect(progress.completedUnitCount == 512 * 1024)
            #expect(progress.totalUnitCount == 512 * 1024)
            #expect(samples.values.contains { $0 > 0 && $0 < 512 * 1024 })
        }

        @Test("Short responses report the actual file size", arguments: [true, false])
        func shortDownload(includesLength: Bool) async throws {
            let body = Data("hello".utf8)
            let server = try SlowDownloadServer { _ in
                .init(body: body, delay: 0, includesLength: includesLength)
            }
            let host = try await server.start()
            defer { server.stop() }
            let directory = try temporaryDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let progress = Progress(totalUnitCount: 999)
            let file = try await client(host: host).downloadFile(
                at: "short.txt",
                from: "test/model",
                to: directory.appendingPathComponent("short.txt"),
                progress: progress,
                transport: .lfs
            )
            #expect(try Data(contentsOf: file) == body)
            #expect(progress.completedUnitCount == Int64(body.count))
            #expect(progress.totalUnitCount == Int64(body.count))
        }

        @Test("Cache hits report bytes with an initially unknown total", arguments: [false, true])
        func cacheHit(copy: Bool) async throws {
            let directory = try temporaryDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let cache = HubCache(cacheDirectory: directory)
            let body = Data("cached file".utf8)
            try await cache.storeData(
                body,
                repo: "test/model",
                kind: .model,
                revision: commit,
                filename: "cached.txt",
                etag: "cached",
                ref: "main"
            )
            let progress = Progress(totalUnitCount: 0)
            let file = try await client(host: URL(string: "http://127.0.0.1:1")!, cache: cache).downloadFile(
                at: "cached.txt",
                from: "test/model",
                to: copy ? directory.appendingPathComponent("copy.txt") : nil,
                progress: progress,
                transport: .lfs,
                localFilesOnly: true
            )
            #expect(try Data(contentsOf: file) == body)
            #expect(progress.completedUnitCount == Int64(body.count))
            #expect(progress.totalUnitCount == Int64(body.count))
        }

        @Test("Ranged downloads count the offset only for HTTP 206", arguments: [200, 206])
        func rangedDownload(status: Int) async throws {
            let fullSize = 512 * 1024
            let offset = 128 * 1024
            let server = try SlowDownloadServer { [commit] request in
                if request.hasPrefix("HEAD ") {
                    return .init(headers: ["ETag": "weights", "X-Repo-Commit": commit], body: Data())
                }
                #expect(request.lowercased().contains("range: bytes=\(offset)-"))
                let headers = status == 206 ? ["Content-Range": "bytes \(offset)-\(fullSize - 1)/\(fullSize)"] : [:]
                return .init(
                    status: status,
                    headers: headers,
                    body: Data(repeating: 0xAB, count: status == 206 ? fullSize - offset : fullSize)
                )
            }
            let host = try await server.start()
            defer { server.stop() }
            let directory = try temporaryDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let cache = HubCache(cacheDirectory: directory)
            let incomplete = try cache.incompleteBlobPath(repo: "test/model", kind: .model, etag: "weights")
            try FileManager.default.createDirectory(
                at: incomplete.deletingLastPathComponent(),
                withIntermediateDirectories: true
            )
            try Data(repeating: 0xAB, count: offset).write(to: incomplete)
            let progress = Progress(totalUnitCount: Int64(fullSize))
            progress.completedUnitCount = Int64(offset)
            let samples = DownloadProgressSamples()
            let observation = progress.observe(\.completedUnitCount) { progress, _ in samples.record(progress) }
            defer { observation.invalidate() }
            let client = client(host: host, cache: cache)
            async let download = client.downloadFile(
                at: "weights.bin",
                from: "test/model",
                progress: progress,
                transport: .lfs
            )
            try await waitForIntermediateProgress(progress, above: status == 206 ? Int64(offset) : 0)
            let file = try await download
            #expect(try Data(contentsOf: file) == Data(repeating: 0xAB, count: fullSize))
            #expect(progress.completedUnitCount == Int64(fullSize))
            #expect(progress.totalUnitCount == Int64(fullSize))
            if status == 206 {
                #expect(samples.values.contains { $0 > offset && $0 < fullSize })
                #expect(samples.values.allSatisfy { $0 >= offset && $0 <= fullSize })
            } else {
                #expect(samples.values.contains { $0 > 0 && $0 < offset })
                #expect(samples.values.allSatisfy { $0 <= fullSize })
            }
        }

        @Test("Cancellation before task registration prevents a download")
        func alreadyCancelled() async throws {
            let server = try SlowDownloadServer { _ in
                Issue.record("An already canceled task sent a request")
                return .init()
            }
            let host = try await server.start()
            defer { server.stop() }
            let progress = Progress(totalUnitCount: 512 * 1024)
            let download = Task {
                withUnsafeCurrentTask { $0?.cancel() }
                return try await URLSession.shared.hfAsyncDownload(for: URLRequest(url: host), progress: progress)
            }
            do {
                _ = try await download.value
                Issue.record("An already canceled download succeeded")
            } catch {
                #expect(error is CancellationError || (error as? URLError)?.code == .cancelled)
            }
            #expect(progress.completedUnitCount == 0)
        }

        @Test("A truncated response stops progress without completing")
        func truncatedResponse() async throws {
            let server = try SlowDownloadServer { _ in
                .init(body: Data(repeating: 0xAB, count: 128 * 1024), declaredSize: 512 * 1024)
            }
            let host = try await server.start()
            defer { server.stop() }
            let directory = try temporaryDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let progress = Progress(totalUnitCount: 0)
            let destination = directory.appendingPathComponent("weights.bin")
            await #expect(throws: (any Error).self) {
                try await client(host: host).downloadFile(
                    at: "weights.bin",
                    from: "test/model",
                    to: destination,
                    progress: progress,
                    transport: .lfs
                )
            }
            let completed = progress.completedUnitCount
            try await Task.sleep(for: .milliseconds(250))
            #expect(progress.completedUnitCount == completed)
            #expect(completed > 0 && completed < progress.totalUnitCount)
            #expect(!FileManager.default.fileExists(atPath: destination.path))
        }

        @Test("Cancellation stops progress updates")
        func cancellation() async throws {
            let server = try SlowDownloadServer()
            let host = try await server.start()
            defer { server.stop() }
            let directory = try temporaryDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let client = client(host: host)
            let progress = Progress(totalUnitCount: 0)
            let destination = directory.appendingPathComponent("weights.bin")
            let download = Task {
                try await client.downloadFile(
                    at: "weights.bin",
                    from: "test/model",
                    to: destination,
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
            let completed = progress.completedUnitCount
            try await Task.sleep(for: .milliseconds(250))
            #expect(progress.completedUnitCount == completed)
            #expect(completed < progress.totalUnitCount)
            #expect(!FileManager.default.fileExists(atPath: destination.path))
        }

        @Test("HTTP failures do not report successful completion")
        func httpFailure() async throws {
            let server = try SlowDownloadServer { _ in .init(status: 500, body: Data("error".utf8), delay: 0) }
            let host = try await server.start()
            defer { server.stop() }
            let directory = try temporaryDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let progress = Progress(totalUnitCount: 512 * 1024)
            await #expect(throws: (any Error).self) {
                try await client(host: host).downloadFile(
                    at: "weights.bin",
                    from: "test/model",
                    to: directory.appendingPathComponent("weights.bin"),
                    progress: progress,
                    transport: .lfs
                )
            }
            let completed = progress.completedUnitCount
            try await Task.sleep(for: .milliseconds(250))
            #expect(progress.completedUnitCount == completed)
            #expect(completed < progress.totalUnitCount)
        }

        @Test("Concurrent downloads keep separate byte counts")
        func concurrentDownloads() async throws {
            let server = try SlowDownloadServer { request in
                .init(
                    body: Data(
                        repeating: request.contains("small.bin") ? 0x01 : 0x02,
                        count: request.contains("small.bin") ? 256 * 1024 : 512 * 1024
                    )
                )
            }
            let host = try await server.start()
            defer { server.stop() }
            let directory = try temporaryDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let client = client(host: host)
            let small = Progress(totalUnitCount: 0)
            let large = Progress(totalUnitCount: 0)
            async let smallFile = client.downloadFile(
                at: "small.bin",
                from: "test/model",
                to: directory.appendingPathComponent("small"),
                progress: small,
                transport: .lfs
            )
            async let largeFile = client.downloadFile(
                at: "large.bin",
                from: "test/model",
                to: directory.appendingPathComponent("large"),
                progress: large,
                transport: .lfs
            )
            try await waitForIntermediateProgress(small)
            try await waitForIntermediateProgress(large)
            #expect(small.totalUnitCount == 256 * 1024)
            #expect(large.totalUnitCount == 512 * 1024)
            let files = try await (smallFile, largeFile)
            #expect(try Data(contentsOf: files.0) == Data(repeating: 0x01, count: 256 * 1024))
            #expect(try Data(contentsOf: files.1) == Data(repeating: 0x02, count: 512 * 1024))
            #expect(small.completedUnitCount == 256 * 1024)
            #expect(large.completedUnitCount == 512 * 1024)
        }
    }
#endif
