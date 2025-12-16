import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Testing

@testable import HuggingFace

#if swift(>=6.1)
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
                #expect(request.httpMethod == "GET")

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
                #expect(request.httpMethod == "GET")

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
            _ = try await client.downloadContentsOfFile(at: "test.txt", from: repoID, useRaw: true)
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
