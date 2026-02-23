import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Testing

@testable import HuggingFace

#if swift(>=6.1) && !os(Linux)
    private final class ProgressCallCounter: @unchecked Sendable {
        private let lock = NSLock()
        private var _count = 0
        var count: Int {
            lock.lock()
            defer { lock.unlock() }
            return _count
        }
        func increment() {
            lock.lock()
            _count += 1
            lock.unlock()
        }
    }

    private final class ProgressValueRecorder: @unchecked Sendable {
        private let lock = NSLock()
        private var _values: [Double] = []

        func append(_ value: Double) {
            lock.lock()
            _values.append(value)
            lock.unlock()
        }

        var values: [Double] {
            lock.lock()
            defer { lock.unlock() }
            return _values
        }
    }

    @Suite("File Operations Tests", .serialized)
    struct FileOperationsTests {
        func createMockClient(bearerToken: String? = "test_token") -> HubClient {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [MockURLProtocol.self]
            let session = URLSession(configuration: configuration)
            return HubClient(
                session: session,
                host: URL(string: "https://huggingface.co")!,
                userAgent: "TestClient/1.0",
                bearerToken: bearerToken
            )
        }

        func createMockClientWithCache(bearerToken: String? = "test_token") -> (HubClient, URL) {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [MockURLProtocol.self]
            let session = URLSession(configuration: configuration)
            let cacheDirectory = FileManager.default.temporaryDirectory
                .appendingPathComponent("hf-cache-\(UUID().uuidString)", isDirectory: true)
            let cache = HubCache(cacheDirectory: cacheDirectory)
            let client = HubClient(
                session: session,
                host: URL(string: "https://huggingface.co")!,
                userAgent: "TestClient/1.0",
                bearerToken: bearerToken,
                cache: cache
            )
            return (client, cacheDirectory)
        }

        func createMockClientWithoutCache(bearerToken: String? = "test_token") -> HubClient {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [MockURLProtocol.self]
            let session = URLSession(configuration: configuration)
            return HubClient(
                session: session,
                host: URL(string: "https://huggingface.co")!,
                userAgent: "TestClient/1.0",
                bearerToken: bearerToken,
                cache: nil
            )
        }

        // MARK: - List Files Tests

        @Test("List files in repository", .mockURLSession)
        func testListFiles() async throws {
            let mockResponse = """
                [
                    {
                        "path": "README.md",
                        "type": "file",
                        "oid": "abc123",
                        "size": 1234
                    },
                    {
                        "path": "config.json",
                        "type": "file",
                        "oid": "def456",
                        "size": 567
                    },
                    {
                        "path": "model",
                        "type": "directory"
                    }
                ]
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/models/facebook/bart-large/tree/main")
                #expect(request.url?.query?.contains("recursive=true") == true)
                #expect(request.httpMethod == "GET" || request.httpMethod == "HEAD")

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
            let repoID: Repo.ID = "facebook/bart-large"
            let files = try await client.listFiles(in: repoID, kind: .model, revision: "main")

            #expect(files.count == 3)
            #expect(files[0].path == "README.md")
            #expect(files[0].type == .file)
            #expect(files[1].path == "config.json")
            #expect(files[2].path == "model")
            #expect(files[2].type == .directory)
        }

        @Test("List files without recursive", .mockURLSession)
        func testListFilesNonRecursive() async throws {
            let mockResponse = """
                [
                    {
                        "path": "README.md",
                        "type": "file",
                        "oid": "abc123",
                        "size": 1234
                    }
                ]
                """

            await MockURLProtocol.setHandler { request in
                // Verify recursive is NOT in query
                #expect(
                    request.url?.query?.contains("recursive") == false
                        || request.url?.query == nil
                )

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
            let repoID: Repo.ID = "user/repo"
            let files = try await client.listFiles(in: repoID, kind: .model, recursive: false)

            #expect(files.count == 1)
        }

        // MARK: - File Info Tests

        @Test("Get file info - file exists", .mockURLSession)
        func testFileInfoExists() async throws {
            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/facebook/bart-large/resolve/main/README.md")
                #expect(request.httpMethod == "HEAD")
                #expect(request.value(forHTTPHeaderField: "Range") == "bytes=0-0")

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 206,
                    httpVersion: "HTTP/1.1",
                    headerFields: [
                        "Content-Length": "12345",
                        "ETag": "\"abc123def\"",
                        "X-Repo-Commit": "commit-sha-123",
                    ]
                )!

                return (response, Data())
            }

            let client = createMockClient()
            let repoID: Repo.ID = "facebook/bart-large"
            let info = try await client.getFile(
                at: "README.md",
                in: repoID,
                kind: .model,
                revision: "main"
            )

            #expect(info.exists == true)
            #expect(info.size == 12345)
            #expect(info.etag == "\"abc123def\"")
            #expect(info.revision == "commit-sha-123")
            #expect(info.isLFS == false)
        }

        @Test("Get file info - LFS file", .mockURLSession)
        func testFileInfoLFS() async throws {
            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: [
                        "Content-Length": "100000000",
                        "X-Linked-Size": "100000000",
                    ]
                )!

                return (response, Data())
            }

            let client = createMockClient()
            let repoID: Repo.ID = "user/model"
            let info = try await client.getFile(at: "pytorch_model.bin", in: repoID)

            #expect(info.exists == true)
            #expect(info.isLFS == true)
        }

        @Test("Get file info - file does not exist", .mockURLSession)
        func testFileInfoNotExists() async throws {
            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 404,
                    httpVersion: "HTTP/1.1",
                    headerFields: [:]
                )!

                return (response, Data())
            }

            let client = createMockClient()
            let repoID: Repo.ID = "user/model"
            let info = try await client.getFile(at: "nonexistent.txt", in: repoID)

            #expect(info.exists == false)
        }

        // MARK: - File Exists Tests

        @Test("Check if file exists - true", .mockURLSession)
        func testFileExists() async {
            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: [:]
                )!

                return (response, Data())
            }

            let client = createMockClient()
            let repoID: Repo.ID = "user/model"
            let exists = await client.fileExists(at: "README.md", in: repoID)

            #expect(exists == true)
        }

        @Test("Check if file exists - false", .mockURLSession)
        func testFileNotExists() async {
            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 404,
                    httpVersion: "HTTP/1.1",
                    headerFields: [:]
                )!

                return (response, Data())
            }

            let client = createMockClient()
            let repoID: Repo.ID = "user/model"
            let exists = await client.fileExists(at: "nonexistent.txt", in: repoID)

            #expect(exists == false)
        }

        // MARK: - Download Tests

        @Test("Download file data", .mockURLSession)
        func testDownloadData() async throws {
            let expectedData = "Hello, World!".data(using: .utf8)!

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/user/model/resolve/main/test.txt")
                #expect(request.httpMethod == "GET" || request.httpMethod == "HEAD")

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "text/plain"]
                )!

                return (response, expectedData)
            }

            let client = createMockClient()
            let repoID: Repo.ID = "user/model"
            let data = try await client.downloadContentsOfFile(at: "test.txt", from: repoID)

            #expect(data == expectedData)
        }

        @Test("Download with raw endpoint", .mockURLSession)
        func testDownloadRaw() async throws {
            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/user/model/raw/main/test.txt")

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: [:]
                )!

                return (response, Data())
            }

            let client = createMockClient()
            let repoID: Repo.ID = "user/model"
            _ = try await client.downloadContentsOfFile(at: "test.txt", from: repoID, endpoint: .raw)
        }

        @Test("downloadFile localFilesOnly returns cached file without network", .mockURLSession)
        func testDownloadFileLocalFilesOnlyUsesCache() async throws {
            let commit = "1234567890123456789012345678901234567890"
            let fileBody = Data("cached-file".utf8)
            await MockURLProtocol.setHandler { request in
                let path = request.url?.path ?? ""
                if path == "/user/model/resolve/main/test.txt" {
                    let headers: [String: String] =
                        if request.httpMethod == "HEAD" {
                            [
                                "ETag": "\"etag-123\"",
                                "X-Repo-Commit": commit,
                            ]
                        } else {
                            ["Content-Type": "text/plain"]
                        }
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 200,
                        httpVersion: "HTTP/1.1",
                        headerFields: headers
                    )!
                    return (response, request.httpMethod == "HEAD" ? Data() : fileBody)
                }
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 404,
                    httpVersion: "HTTP/1.1",
                    headerFields: [:]
                )!
                return (response, Data())
            }

            let (client, cacheDirectory) = createMockClientWithCache()
            defer { try? FileManager.default.removeItem(at: cacheDirectory) }
            let repoID: Repo.ID = "user/model"

            _ = try await client.downloadContentsOfFile(
                at: "test.txt",
                from: repoID,
                revision: "main"
            )

            await MockURLProtocol.setHandler { _ in
                throw NSError(domain: NSURLErrorDomain, code: NSURLErrorNotConnectedToInternet)
            }
            let cachedPath = try await client.downloadContentsOfFile(
                at: "test.txt",
                from: repoID,
                revision: "main",
                localFilesOnly: true
            )
            let cachedData = try Data(contentsOf: cachedPath)
            #expect(cachedData == fileBody)
        }

        @Test("downloadFile resumes from incomplete blob with range request", .mockURLSession)
        func testDownloadFileResumesFromIncompleteBlob() async throws {
            let commit = "1234567890123456789012345678901234567890"
            let partial = Data("hello ".utf8)
            let remainder = Data("world".utf8)
            let full = Data("hello world".utf8)
            let expectedRange = "bytes=\(partial.count)-"

            await MockURLProtocol.setHandler { request in
                let path = request.url?.path ?? ""
                if path == "/user/model/resolve/main/test.txt" {
                    if request.httpMethod == "HEAD" {
                        let response = HTTPURLResponse(
                            url: request.url!,
                            statusCode: 200,
                            httpVersion: "HTTP/1.1",
                            headerFields: [
                                "ETag": "\"etag-123\"",
                                "X-Repo-Commit": commit,
                            ]
                        )!
                        return (response, Data())
                    }
                    #expect(request.value(forHTTPHeaderField: "Range") == expectedRange)
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 206,
                        httpVersion: "HTTP/1.1",
                        headerFields: [
                            "Content-Type": "text/plain",
                            "Content-Length": "\(remainder.count)",
                        ]
                    )!
                    return (response, remainder)
                }
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 404,
                    httpVersion: "HTTP/1.1",
                    headerFields: [:]
                )!
                return (response, Data())
            }

            let (client, cacheDirectory) = createMockClientWithCache()
            defer { try? FileManager.default.removeItem(at: cacheDirectory) }
            let repoID: Repo.ID = "user/model"
            let cache = HubCache(cacheDirectory: cacheDirectory)
            let blobsDir = cache.blobsDirectory(repo: repoID, kind: .model)
            try FileManager.default.createDirectory(at: blobsDir, withIntermediateDirectories: true)
            let incompletePath = blobsDir.appendingPathComponent("etag-123.incomplete")
            try partial.write(to: incompletePath, options: .atomic)

            let cachedPath = try await client.downloadContentsOfFile(
                at: "test.txt",
                from: repoID,
                to: nil,
                revision: "main"
            )
            let blobPath = blobsDir.appendingPathComponent("etag-123")
            #expect(FileManager.default.fileExists(atPath: blobPath.path))
            #expect(FileManager.default.fileExists(atPath: incompletePath.path) == false)
            #expect(try Data(contentsOf: blobPath) == full)
            #expect(try Data(contentsOf: cachedPath) == full)
        }

        #if !canImport(FoundationNetworking)
            // Disabled on Linux: FoundationNetworking URLProtocol client can crash during finishLoading.
            @Test("downloadSnapshot invokes progressHandler during file download", .mockURLSession)
            func testDownloadSnapshotProgressHandlerCalledDuringDownload() async throws {
                let listResponse = """
                    [
                        {"path": "large.bin", "type": "file", "oid": "abc", "size": 500}
                    ]
                    """
                let fileBody = Data(repeating: 0xAB, count: 500)

                MockURLProtocol.setChunkSize(100)
                await MockURLProtocol.setHandler { request in
                    let path = request.url?.path ?? ""
                    if path.contains("/api/models/user/model/tree/") {
                        let response = HTTPURLResponse(
                            url: request.url!,
                            statusCode: 200,
                            httpVersion: "HTTP/1.1",
                            headerFields: ["Content-Type": "application/json"]
                        )!
                        return (response, Data(listResponse.utf8))
                    }
                    if path == "/user/model/resolve/main/large.bin" {
                        let response = HTTPURLResponse(
                            url: request.url!,
                            statusCode: 200,
                            httpVersion: "HTTP/1.1",
                            headerFields: ["Content-Type": "application/octet-stream"]
                        )!
                        return (response, fileBody)
                    }
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 404,
                        httpVersion: "HTTP/1.1",
                        headerFields: [:]
                    )!
                    return (response, Data())
                }

                let callCount = ProgressCallCounter()
                let (client, cacheDirectory) = createMockClientWithCache()
                defer { try? FileManager.default.removeItem(at: cacheDirectory) }
                let destination = FileManager.default.temporaryDirectory
                    .appendingPathComponent(UUID().uuidString, isDirectory: true)
                defer { try? FileManager.default.removeItem(at: destination) }

                let result = try await client.downloadSnapshot(
                    of: "user/model",
                    kind: .model,
                    to: destination,
                    revision: "main",
                    matching: [],
                    progressHandler: { _ in callCount.increment() }
                )

                #expect(result == destination)
                #expect(FileManager.default.fileExists(atPath: destination.appendingPathComponent("large.bin").path))
                #if canImport(FoundationNetworking)
                    let minimumExpectedCalls = 2
                #else
                    let minimumExpectedCalls = 3
                #endif
                #expect(
                    callCount.count >= minimumExpectedCalls,
                    "progressHandler should be called at least \(minimumExpectedCalls) times; got \(callCount.count)"
                )
            }
        #endif

        @Test("downloadSnapshot rejects path traversal entries", .mockURLSession)
        func testDownloadSnapshotRejectsPathTraversalEntry() async throws {
            let listResponse = """
                [
                    {"path": "../outside.txt", "type": "file", "oid": "abc", "size": 10}
                ]
                """

            await MockURLProtocol.setHandler { request in
                let path = request.url?.path ?? ""
                #expect(path.contains("/api/models/user/model/tree/"))

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!
                return (response, Data(listResponse.utf8))
            }

            let (client, cacheDirectory) = createMockClientWithCache()
            defer { try? FileManager.default.removeItem(at: cacheDirectory) }

            await #expect(throws: HubCacheError.self) {
                _ = try await client.downloadSnapshot(
                    of: "user/model",
                    kind: .model,
                    revision: "main"
                )
            }
        }

        @Test("downloadSnapshot requires cache or destination", .mockURLSession)
        func testDownloadSnapshotRequiresCacheOrDestination() async throws {
            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 500,
                    httpVersion: "HTTP/1.1",
                    headerFields: [:]
                )!
                return (response, Data("network should not be reached".utf8))
            }

            let client = createMockClientWithoutCache()
            await #expect(throws: HubCacheError.self) {
                _ = try await client.downloadSnapshot(
                    of: "user/model",
                    kind: .model,
                    revision: "main",
                    matching: ["*.json"]
                )
            }
        }

        #if !canImport(FoundationNetworking)
            // Disabled on Linux: FoundationNetworking URLProtocol client can crash during finishLoading.
            @Test("downloadSnapshot progress does not regress", .mockURLSession)
            func testDownloadSnapshotProgressDoesNotRegress() async throws {
                let listResponse = """
                    [
                        {"path": "large.bin", "type": "file", "oid": "abc", "size": 500}
                    ]
                    """
                let fileBody = Data(repeating: 0xAB, count: 500)

                MockURLProtocol.setChunkSize(100)
                await MockURLProtocol.setHandler { request in
                    let path = request.url?.path ?? ""
                    if path.contains("/api/models/user/model/tree/") {
                        let response = HTTPURLResponse(
                            url: request.url!,
                            statusCode: 200,
                            httpVersion: "HTTP/1.1",
                            headerFields: ["Content-Type": "application/json"]
                        )!
                        return (response, Data(listResponse.utf8))
                    }
                    if path == "/user/model/resolve/main/large.bin" {
                        let response = HTTPURLResponse(
                            url: request.url!,
                            statusCode: 200,
                            httpVersion: "HTTP/1.1",
                            headerFields: ["Content-Type": "application/octet-stream"]
                        )!
                        return (response, fileBody)
                    }
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 404,
                        httpVersion: "HTTP/1.1",
                        headerFields: [:]
                    )!
                    return (response, Data())
                }

                let recorder = ProgressValueRecorder()
                let (client, cacheDirectory) = createMockClientWithCache()
                defer { try? FileManager.default.removeItem(at: cacheDirectory) }
                let destination = FileManager.default.temporaryDirectory
                    .appendingPathComponent(UUID().uuidString, isDirectory: true)
                defer { try? FileManager.default.removeItem(at: destination) }

                let result = try await client.downloadSnapshot(
                    of: "user/model",
                    kind: .model,
                    to: destination,
                    revision: "main",
                    matching: [],
                    progressHandler: { progress in
                        recorder.append(progress.fractionCompleted)
                    }
                )

                #expect(result == destination)
                let values = recorder.values
                #expect(values.isEmpty == false)
                for index in 1 ..< values.count {
                    #expect(
                        values[index] >= values[index - 1],
                        "progress fraction should be non-decreasing"
                    )
                }
                #expect(values.last ?? 0.0 >= 1.0 - 0.0001)
            }
        #endif

        @Test("downloadSnapshot copies to destination when provided", .mockURLSession)
        func testDownloadSnapshotCopiesToDestinationWhenProvided() async throws {
            let commit = "1234567890123456789012345678901234567890"
            let listResponse = """
                [
                    {"path": "config.json", "type": "file", "oid": "abc", "size": 12}
                ]
                """
            let fileBody = Data("{\"ok\":true}".utf8)
            await MockURLProtocol.setHandler { request in
                let path = request.url?.path ?? ""
                if path.contains("/api/models/user/model/tree/\(commit)") {
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 200,
                        httpVersion: "HTTP/1.1",
                        headerFields: ["Content-Type": "application/json"]
                    )!
                    return (response, Data(listResponse.utf8))
                }
                if path == "/user/model/resolve/\(commit)/config.json" {
                    let headers: [String: String] =
                        if request.httpMethod == "HEAD" {
                            [
                                "ETag": "\"etag-123\"",
                                "X-Repo-Commit": commit,
                            ]
                        } else {
                            ["Content-Type": "application/json"]
                        }
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 200,
                        httpVersion: "HTTP/1.1",
                        headerFields: headers
                    )!
                    return (response, request.httpMethod == "HEAD" ? Data() : fileBody)
                }
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 404,
                    httpVersion: "HTTP/1.1",
                    headerFields: [:]
                )!
                return (response, Data())
            }

            let (client, cacheDirectory) = createMockClientWithCache()
            defer { try? FileManager.default.removeItem(at: cacheDirectory) }
            let destination = FileManager.default.temporaryDirectory
                .appendingPathComponent("hf-destination-\(UUID().uuidString)", isDirectory: true)
            defer { try? FileManager.default.removeItem(at: destination) }

            let result = try await client.downloadSnapshot(
                of: "user/model",
                kind: .model,
                to: destination,
                revision: commit
            )

            #expect(result == destination)
            #expect(FileManager.default.fileExists(atPath: destination.appendingPathComponent("config.json").path))
        }

        @Test("downloadSnapshot can return cache path instead of copying", .mockURLSession)
        func testDownloadSnapshotReturnsCachePathWhenRequested() async throws {
            let commit = "1234567890123456789012345678901234567890"
            let listResponse = """
                [
                    {"path": "config.json", "type": "file", "oid": "abc", "size": 12}
                ]
                """
            let fileBody = Data("{\"ok\":true}".utf8)
            await MockURLProtocol.setHandler { request in
                let path = request.url?.path ?? ""
                if path.contains("/api/models/user/model/tree/\(commit)") {
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 200,
                        httpVersion: "HTTP/1.1",
                        headerFields: ["Content-Type": "application/json"]
                    )!
                    return (response, Data(listResponse.utf8))
                }
                if path == "/user/model/resolve/\(commit)/config.json" {
                    let headers: [String: String] =
                        if request.httpMethod == "HEAD" {
                            ["ETag": "\"etag-123\"", "X-Repo-Commit": commit]
                        } else {
                            ["Content-Type": "application/json"]
                        }
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 200,
                        httpVersion: "HTTP/1.1",
                        headerFields: headers
                    )!
                    return (response, request.httpMethod == "HEAD" ? Data() : fileBody)
                }
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 404,
                    httpVersion: "HTTP/1.1",
                    headerFields: [:]
                )!
                return (response, Data())
            }

            let (client, cacheDirectory) = createMockClientWithCache()
            defer { try? FileManager.default.removeItem(at: cacheDirectory) }
            let cache = HubCache(cacheDirectory: cacheDirectory)
            let destination = FileManager.default.temporaryDirectory
                .appendingPathComponent("hf-destination-\(UUID().uuidString)", isDirectory: true)
            defer { try? FileManager.default.removeItem(at: destination) }

            let result = try await client.downloadSnapshot(
                of: "user/model",
                kind: .model,
                to: destination,
                revision: commit,
                useSnapshotCachePath: true
            )
            let expectedSnapshotPath = cache.snapshotsDirectory(repo: "user/model", kind: .model)
                .appendingPathComponent(commit)
            #expect(result == expectedSnapshotPath)
            #expect(FileManager.default.fileExists(atPath: result.appendingPathComponent("config.json").path))
            #expect(
                FileManager.default.fileExists(atPath: destination.appendingPathComponent("config.json").path) == false
            )
        }

        @Test("downloadSnapshot localFilesOnly uses cache without network", .mockURLSession)
        func testDownloadSnapshotLocalFilesOnlyUsesCache() async throws {
            let commit = "1234567890123456789012345678901234567890"
            let listResponse = """
                [
                    {"path": "config.json", "type": "file", "oid": "abc", "size": 12}
                ]
                """
            let fileBody = Data("{\"ok\":true}".utf8)
            await MockURLProtocol.setHandler { request in
                let path = request.url?.path ?? ""
                if path.contains("/api/models/user/model/tree/main") {
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 200,
                        httpVersion: "HTTP/1.1",
                        headerFields: ["Content-Type": "application/json"]
                    )!
                    return (response, Data(listResponse.utf8))
                }
                if path == "/user/model/resolve/main/config.json" {
                    let headers: [String: String] =
                        if request.httpMethod == "HEAD" {
                            ["ETag": "\"etag-123\"", "X-Repo-Commit": commit]
                        } else {
                            ["Content-Type": "application/json"]
                        }
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 200,
                        httpVersion: "HTTP/1.1",
                        headerFields: headers
                    )!
                    return (response, request.httpMethod == "HEAD" ? Data() : fileBody)
                }
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 404,
                    httpVersion: "HTTP/1.1",
                    headerFields: [:]
                )!
                return (response, Data())
            }

            let (client, cacheDirectory) = createMockClientWithCache()
            defer { try? FileManager.default.removeItem(at: cacheDirectory) }

            _ = try await client.downloadSnapshot(
                of: "user/model",
                kind: .model,
                revision: "main"
            )

            await MockURLProtocol.setHandler { _ in
                throw NSError(domain: NSURLErrorDomain, code: NSURLErrorNotConnectedToInternet)
            }
            let result = try await client.downloadSnapshot(
                of: "user/model",
                kind: .model,
                revision: "main",
                localFilesOnly: true
            )
            #expect(FileManager.default.fileExists(atPath: result.appendingPathComponent("config.json").path))
        }

        @Test("downloadSnapshot falls back to cache when tree request fails", .mockURLSession)
        func testDownloadSnapshotFallsBackToCacheOnTreeFailure() async throws {
            let commit = "1234567890123456789012345678901234567890"
            let listResponse = """
                [
                    {"path": "config.json", "type": "file", "oid": "abc", "size": 12}
                ]
                """
            let fileBody = Data("{\"ok\":true}".utf8)
            await MockURLProtocol.setHandler { request in
                let path = request.url?.path ?? ""
                if path.contains("/api/models/user/model/tree/main") {
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 200,
                        httpVersion: "HTTP/1.1",
                        headerFields: ["Content-Type": "application/json"]
                    )!
                    return (response, Data(listResponse.utf8))
                }
                if path == "/user/model/resolve/main/config.json" {
                    let headers: [String: String] =
                        if request.httpMethod == "HEAD" {
                            ["ETag": "\"etag-123\"", "X-Repo-Commit": commit]
                        } else {
                            ["Content-Type": "application/json"]
                        }
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 200,
                        httpVersion: "HTTP/1.1",
                        headerFields: headers
                    )!
                    return (response, request.httpMethod == "HEAD" ? Data() : fileBody)
                }
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 404,
                    httpVersion: "HTTP/1.1",
                    headerFields: [:]
                )!
                return (response, Data())
            }

            let (client, cacheDirectory) = createMockClientWithCache()
            defer { try? FileManager.default.removeItem(at: cacheDirectory) }
            _ = try await client.downloadSnapshot(
                of: "user/model",
                kind: .model,
                revision: "main"
            )

            await MockURLProtocol.setHandler { request in
                let path = request.url?.path ?? ""
                if path.contains("/api/models/user/model/tree/main") {
                    throw NSError(domain: NSURLErrorDomain, code: NSURLErrorNotConnectedToInternet)
                }
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 404,
                    httpVersion: "HTTP/1.1",
                    headerFields: [:]
                )!
                return (response, Data())
            }
            let result = try await client.downloadSnapshot(
                of: "user/model",
                kind: .model,
                revision: "main"
            )
            #expect(FileManager.default.fileExists(atPath: result.appendingPathComponent("config.json").path))
        }

        @Test("downloadSnapshot commit fast path skips API when metadata complete", .mockURLSession)
        func testDownloadSnapshotCommitFastPathSkipsAPI() async throws {
            let commit = "1234567890123456789012345678901234567890"
            let listResponse = """
                [
                    {"path": "config.json", "type": "file", "oid": "abc", "size": 12}
                ]
                """
            let fileBody = Data("{\"ok\":true}".utf8)
            let treeCalls = ProgressCallCounter()

            await MockURLProtocol.setHandler { request in
                let path = request.url?.path ?? ""
                if path.contains("/api/models/user/model/tree/") {
                    treeCalls.increment()
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 200,
                        httpVersion: "HTTP/1.1",
                        headerFields: ["Content-Type": "application/json"]
                    )!
                    return (response, Data(listResponse.utf8))
                }
                if path == "/user/model/resolve/\(commit)/config.json" {
                    let headers: [String: String] =
                        if request.httpMethod == "HEAD" {
                            ["ETag": "\"etag-123\"", "X-Repo-Commit": commit]
                        } else {
                            ["Content-Type": "application/json"]
                        }
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 200,
                        httpVersion: "HTTP/1.1",
                        headerFields: headers
                    )!
                    return (response, request.httpMethod == "HEAD" ? Data() : fileBody)
                }
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 404,
                    httpVersion: "HTTP/1.1",
                    headerFields: [:]
                )!
                return (response, Data())
            }

            let (client, cacheDirectory) = createMockClientWithCache()
            defer { try? FileManager.default.removeItem(at: cacheDirectory) }

            _ = try await client.downloadSnapshot(
                of: "user/model",
                kind: .model,
                revision: commit,
                matching: ["config.json"]
            )
            #expect(treeCalls.count == 1)

            await MockURLProtocol.setHandler { request in
                let path = request.url?.path ?? ""
                if path.contains("/api/models/user/model/tree/") {
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 500,
                        httpVersion: "HTTP/1.1",
                        headerFields: [:]
                    )!
                    return (response, Data("unexpected tree call".utf8))
                }
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 404,
                    httpVersion: "HTTP/1.1",
                    headerFields: [:]
                )!
                return (response, Data())
            }

            let second = try await client.downloadSnapshot(
                of: "user/model",
                kind: .model,
                revision: commit,
                matching: ["config.json"]
            )
            #expect(FileManager.default.fileExists(atPath: second.appendingPathComponent("config.json").path))
        }

        @Test("downloadSnapshot commit fast path falls through when metadata missing", .mockURLSession)
        func testDownloadSnapshotCommitFastPathFallsThroughWithoutMetadata() async throws {
            let commit = "1234567890123456789012345678901234567890"
            let listResponse = """
                [
                    {"path": "config.json", "type": "file", "oid": "abc", "size": 12}
                ]
                """
            let fileBody = Data("{\"ok\":true}".utf8)
            let treeCalls = ProgressCallCounter()

            await MockURLProtocol.setHandler { request in
                let path = request.url?.path ?? ""
                if path.contains("/api/models/user/model/tree/") {
                    treeCalls.increment()
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 200,
                        httpVersion: "HTTP/1.1",
                        headerFields: ["Content-Type": "application/json"]
                    )!
                    return (response, Data(listResponse.utf8))
                }
                if path == "/user/model/resolve/\(commit)/config.json" {
                    let headers: [String: String] =
                        if request.httpMethod == "HEAD" {
                            ["ETag": "\"etag-123\"", "X-Repo-Commit": commit]
                        } else {
                            ["Content-Type": "application/json"]
                        }
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 200,
                        httpVersion: "HTTP/1.1",
                        headerFields: headers
                    )!
                    return (response, request.httpMethod == "HEAD" ? Data() : fileBody)
                }
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 404,
                    httpVersion: "HTTP/1.1",
                    headerFields: [:]
                )!
                return (response, Data())
            }

            let (client, cacheDirectory) = createMockClientWithCache()
            defer { try? FileManager.default.removeItem(at: cacheDirectory) }

            _ = try await client.downloadSnapshot(
                of: "user/model",
                kind: .model,
                revision: commit,
                matching: ["config.json"]
            )
            #expect(treeCalls.count == 1)

            let metadataFile = HubCache(cacheDirectory: cacheDirectory)
                .metadataDirectory(repo: "user/model", kind: .model)
                .appendingPathComponent("\(commit).json")
            try? FileManager.default.removeItem(at: metadataFile)

            _ = try await client.downloadSnapshot(
                of: "user/model",
                kind: .model,
                revision: commit,
                matching: ["config.json"]
            )
            #expect(treeCalls.count == 2)
        }

        // MARK: - Delete Tests

        @Test("Delete single file", .mockURLSession)
        func testDeleteFile() async throws {
            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/models/user/model/commit/main")
                #expect(request.httpMethod == "POST")
                #expect(
                    request.value(forHTTPHeaderField: "Content-Type") == "application/json"
                )

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: [:]
                )!

                return (response, Data("true".utf8))
            }

            let client = createMockClient()
            let repoID: Repo.ID = "user/model"
            try await client.deleteFile(at: "test.txt", from: repoID, message: "Delete test file")
        }

        @Test("Delete multiple files", .mockURLSession)
        func testDeleteBatch() async throws {
            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/datasets/org/dataset/commit/dev")

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: [:]
                )!

                return (response, Data("true".utf8))
            }

            let client = createMockClient()
            let repoID: Repo.ID = "org/dataset"
            try await client.deleteFiles(
                at: ["file1.txt", "file2.txt", "dir/file3.txt"],
                from: repoID,
                kind: .dataset,
                branch: "dev",
                message: "Delete old files"
            )
        }

        // MARK: - FileBatch Tests

        @Test("FileBatch dictionary literal initialization")
        func testFileBatchDictionaryLiteral() {
            let batch: FileBatch = [
                "README.md": .path("/tmp/readme.md"),
                "config.json": .path("/tmp/config.json"),
            ]

            let items = Array(batch)
            #expect(items.count == 2)
            #expect(items.contains { $0.key == "README.md" && $0.value.url.path == "/tmp/readme.md" })
            #expect(items.contains { $0.key == "config.json" && $0.value.url.path == "/tmp/config.json" })
        }

        @Test("FileBatch add and remove")
        func testFileBatchMutations() {
            var batch = FileBatch()
            #expect(batch.count == 0)

            batch["file1.txt"] = .path("/tmp/file1.txt")
            #expect(batch.count == 1)

            batch["file2.txt"] = .path("/tmp/file2.txt")
            #expect(batch.count == 2)

            batch["file1.txt"] = nil
            #expect(batch.count == 1)
            #expect(batch["file2.txt"]?.url.path == "/tmp/file2.txt")
        }

        // MARK: - Error Handling Tests

        @Test("Handle network error", .mockURLSession)
        func testNetworkError() async throws {
            await MockURLProtocol.setHandler { request in
                throw NSError(
                    domain: NSURLErrorDomain,
                    code: NSURLErrorNotConnectedToInternet
                )
            }

            let client = createMockClient()
            let repoID: Repo.ID = "user/model"

            await #expect(throws: Error.self) {
                _ = try await client.downloadContentsOfFile(at: "test.txt", from: repoID)
            }
        }

        @Test("Handle unauthorized access", .mockURLSession)
        func testUnauthorized() async throws {
            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 401,
                    httpVersion: "HTTP/1.1",
                    headerFields: [:]
                )!

                return (response, Data("{\"error\": \"Unauthorized\"}".utf8))
            }

            let client = createMockClient(bearerToken: nil)
            let repoID: Repo.ID = "user/private-model"

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.downloadContentsOfFile(at: "test.txt", from: repoID)
            }
        }
    }

#endif  // swift(>=6.1)
