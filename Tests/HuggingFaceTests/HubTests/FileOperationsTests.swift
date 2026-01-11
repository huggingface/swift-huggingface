import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Replay
import Testing

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("File Operations Tests", .serialized)
    struct FileOperationsTests {
        private func createClient(bearerToken: String? = "test_token") -> HubClient {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [PlaybackURLProtocol.self]
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

        @Test(
            "List files in repository",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/models/facebook/bart-large/tree/main?recursive=true",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
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
                    }
                ]
            )
        )
        func testListFiles() async throws {
            let client = createClient()
            let repoID: Repo.ID = "facebook/bart-large"
            let files = try await client.listFiles(in: repoID, kind: .model, revision: "main")

            #expect(files.count == 3)
            #expect(files[0].path == "README.md")
            #expect(files[0].type == .file)
            #expect(files[1].path == "config.json")
            #expect(files[2].path == "model")
            #expect(files[2].type == .directory)
        }

        @Test(
            "List files without recursive",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/models/user/repo/tree/main",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        [
                            {
                                "path": "README.md",
                                "type": "file",
                                "oid": "abc123",
                                "size": 1234
                            }
                        ]
                        """
                    }
                ]
            )
        )
        func testListFilesNonRecursive() async throws {
            let client = createClient()
            let repoID: Repo.ID = "user/repo"
            let files = try await client.listFiles(in: repoID, kind: .model, recursive: false)

            #expect(files.count == 1)
        }

        // MARK: - File Info Tests

        @Test(
            "Get file info - file exists",
            .replay(
                stubs: [
                    .head(
                        "https://huggingface.co/facebook/bart-large/resolve/main/README.md",
                        206,
                        [
                            "Content-Length": "12345",
                            "ETag": "\"abc123def\"",
                            "X-Repo-Commit": "commit-sha-123",
                        ]
                    )
                ]
            )
        )
        func testFileInfoExists() async throws {
            let client = createClient()
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

        @Test(
            "Get file info - LFS file",
            .replay(
                stubs: [
                    .head(
                        "https://huggingface.co/user/model/resolve/main/pytorch_model.bin",
                        200,
                        [
                            "Content-Length": "100000000",
                            "X-Linked-Size": "100000000",
                        ]
                    )
                ]
            )
        )
        func testFileInfoLFS() async throws {
            let client = createClient()
            let repoID: Repo.ID = "user/model"
            let info = try await client.getFile(at: "pytorch_model.bin", in: repoID)

            #expect(info.exists == true)
            #expect(info.isLFS == true)
        }

        @Test(
            "Get file info - file does not exist",
            .replay(
                stubs: [
                    .head(
                        "https://huggingface.co/user/model/resolve/main/nonexistent.txt",
                        404,
                        [:]
                    )
                ]
            )
        )
        func testFileInfoNotExists() async throws {
            let client = createClient()
            let repoID: Repo.ID = "user/model"
            let info = try await client.getFile(at: "nonexistent.txt", in: repoID)

            #expect(info.exists == false)
        }

        // MARK: - File Exists Tests

        @Test(
            "Check if file exists - true",
            .replay(
                stubs: [
                    .head(
                        "https://huggingface.co/user/model/resolve/main/README.md",
                        200,
                        [:]
                    )
                ]
            )
        )
        func testFileExists() async {
            let client = createClient()
            let repoID: Repo.ID = "user/model"
            let exists = await client.fileExists(at: "README.md", in: repoID)

            #expect(exists == true)
        }

        @Test(
            "Check if file exists - false",
            .replay(
                stubs: [
                    .head(
                        "https://huggingface.co/user/model/resolve/main/nonexistent.txt",
                        404,
                        [:]
                    )
                ]
            )
        )
        func testFileNotExists() async {
            let client = createClient()
            let repoID: Repo.ID = "user/model"
            let exists = await client.fileExists(at: "nonexistent.txt", in: repoID)

            #expect(exists == false)
        }

        // MARK: - Download Tests

        @Test(
            "Download file data",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/user/model/resolve/main/test.txt",
                        200,
                        ["Content-Type": "text/plain"]
                    ) {
                        "Hello, World!"
                    }
                ]
            )
        )
        func testDownloadData() async throws {
            let expectedData = "Hello, World!".data(using: .utf8)!

            let client = createClient()
            let repoID: Repo.ID = "user/model"
            let data = try await client.downloadContentsOfFile(at: "test.txt", from: repoID)

            #expect(data == expectedData)
        }

        @Test(
            "Download with raw endpoint",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/user/model/raw/main/test.txt",
                        200,
                        [:]
                    ) { "" }
                ]
            )
        )
        func testDownloadRaw() async throws {
            let client = createClient()
            let repoID: Repo.ID = "user/model"
            _ = try await client.downloadContentsOfFile(at: "test.txt", from: repoID, useRaw: true)
        }

        // MARK: - Delete Tests

        @Test(
            "Delete single file",
            .replay(
                stubs: [
                    .post(
                        "https://huggingface.co/api/models/user/model/commit/main",
                        200,
                        [:]
                    ) { "true" }
                ]
            )
        )
        func testDeleteFile() async throws {
            let client = createClient()
            let repoID: Repo.ID = "user/model"
            try await client.deleteFile(at: "test.txt", from: repoID, message: "Delete test file")
        }

        @Test(
            "Delete multiple files",
            .replay(
                stubs: [
                    .post(
                        "https://huggingface.co/api/datasets/org/dataset/commit/dev",
                        200,
                        [:]
                    ) { "true" }
                ]
            )
        )
        func testDeleteBatch() async throws {
            let client = createClient()
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

        @Test(
            "Handle network error",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/user/model/resolve/main/test.txt",
                        500,
                        ["Content-Type": "application/json"]
                    ) { "{\"error\": \"Internal Server Error\"}" }
                ]
            )
        )
        func testNetworkError() async throws {
            let client = createClient()
            let repoID: Repo.ID = "user/model"

            await #expect(throws: Error.self) {
                _ = try await client.downloadContentsOfFile(at: "test.txt", from: repoID)
            }
        }

        @Test(
            "Handle unauthorized access",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/user/private-model/resolve/main/test.txt",
                        401,
                        [:]
                    ) { "{\"error\": \"Unauthorized\"}" }
                ]
            )
        )
        func testUnauthorized() async throws {
            let client = createClient(bearerToken: nil)
            let repoID: Repo.ID = "user/private-model"

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.downloadContentsOfFile(at: "test.txt", from: repoID)
            }
        }
    }

#endif  // swift(>=6.1)
