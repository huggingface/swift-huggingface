import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Testing

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("Git Operations Tests", .serialized)
    struct GitOperationsTests {
        private func createMockClient(bearerToken: String? = "test_token") -> HubClient {
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

        @Test("modelTree builds tree path and decodes entries", .mockURLSession)
        func testModelTree() async throws {
            let responseBody = """
                [
                  {
                    "path": "README.md",
                    "type": "file",
                    "oid": "abc123",
                    "size": 12,
                    "lastCommit": null
                  }
                ]
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/models/user/repo/tree/main/src")
                #expect(request.httpMethod == "GET")
                #expect(request.value(forHTTPHeaderField: "Authorization") == "Bearer test_token")

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!
                return (response, Data(responseBody.utf8))
            }

            let client = createMockClient()
            let id: Repo.ID = "user/repo"
            let entries = try await client.modelTree(id, path: "src")
            #expect(entries.count == 1)
            #expect(entries[0].path == "README.md")
            #expect(entries[0].type == .file)
        }

        @Test("datasetRefs decodes branches and optional tags", .mockURLSession)
        func testDatasetRefs() async throws {
            let responseBody = """
                {
                  "branches": [
                    {
                      "name": "main",
                      "ref": "refs/heads/main",
                      "targetOid": "sha-main"
                    }
                  ],
                  "tags": null
                }
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/datasets/user/repo/refs")
                #expect(request.httpMethod == "GET")

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!
                return (response, Data(responseBody.utf8))
            }

            let client = createMockClient()
            let id: Repo.ID = "user/repo"
            let refs = try await client.datasetRefs(id)
            #expect(refs.branches.count == 1)
            #expect(refs.branches[0].name == "main")
            #expect(refs.tags == nil)
        }

        @Test("spaceCommits builds commits path and decodes payload", .mockURLSession)
        func testSpaceCommits() async throws {
            let responseBody = """
                [
                  {
                    "id": "commit123",
                    "title": "Initial commit",
                    "message": null,
                    "date": "2023-06-15T14:30:00.000Z",
                    "authors": [],
                    "parents": []
                  }
                ]
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/spaces/user/repo/commits/dev")
                #expect(request.httpMethod == "GET")

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!
                return (response, Data(responseBody.utf8))
            }

            let client = createMockClient()
            let id: Repo.ID = "user/repo"
            let commits = try await client.spaceCommits(id, revision: "dev")
            #expect(commits.count == 1)
            #expect(commits[0].id == "commit123")
        }

        @Test("resolve file uses correct prefixes for model dataset and space", .mockURLSession)
        func testResolveFilePrefixes() async throws {
            await MockURLProtocol.setHandler { request in
                let path = request.url?.path ?? ""
                #expect(request.httpMethod == "GET")

                let payload: Data
                switch path {
                case "/user/repo/resolve/main/file.txt":
                    payload = Data("model".utf8)
                case "/datasets/user/repo/resolve/main/file.txt":
                    payload = Data("dataset".utf8)
                case "/spaces/user/repo/resolve/main/file.txt":
                    payload = Data("space".utf8)
                default:
                    Issue.record("Unexpected resolve path: \(path)")
                    payload = Data()
                }

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/octet-stream"]
                )!
                return (response, payload)
            }

            let client = createMockClient()
            let id: Repo.ID = "user/repo"
            let model = try await client.resolveModelFile(id, path: "file.txt")
            let dataset = try await client.resolveDatasetFile(id, path: "file.txt")
            let space = try await client.resolveSpaceFile(id, path: "file.txt")

            #expect(String(decoding: model, as: UTF8.self) == "model")
            #expect(String(decoding: dataset, as: UTF8.self) == "dataset")
            #expect(String(decoding: space, as: UTF8.self) == "space")
        }

        @Test("treeSize handles root and nested paths", .mockURLSession)
        func testTreeSize() async throws {
            await MockURLProtocol.setHandler { request in
                let path = request.url?.path ?? ""
                let responseBody: String
                if path == "/api/models/user/repo/treesize/main" {
                    responseBody = #"{"path":"","size":42}"#
                } else if path == "/api/datasets/user/repo/treesize/rev/data" {
                    responseBody = #"{"path":"data","size":84}"#
                } else {
                    Issue.record("Unexpected treesize path: \(path)")
                    responseBody = #"{"path":"","size":0}"#
                }

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!
                return (response, Data(responseBody.utf8))
            }

            let client = createMockClient()
            let id: Repo.ID = "user/repo"
            let root = try await client.modelTreeSize(id)
            let nested = try await client.datasetTreeSize(id, revision: "rev", path: "data")

            #expect(root.path.isEmpty)
            #expect(root.size == 42)
            #expect(nested.path == "data")
            #expect(nested.size == 84)
        }

        @Test("createModelBranch sends branch params in request body", .mockURLSession)
        func testCreateModelBranch() async throws {
            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/models/user/repo/branch/feature")
                #expect(request.httpMethod == "POST")
                #expect(request.value(forHTTPHeaderField: "Authorization") == "Bearer test_token")

                if let body = request.httpBody,
                    let json = try? JSONSerialization.jsonObject(with: body) as? [String: Any]
                {
                    #expect(json["startingPoint"] as? String == "main")
                    #expect(json["emptyBranch"] as? Bool == true)
                    #expect(json["overwrite"] as? Bool == false)
                }

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!
                return (response, Data())
            }

            let client = createMockClient()
            let id: Repo.ID = "user/repo"
            let created = try await client.createModelBranch(
                id,
                branchName: "feature",
                startingPoint: "main",
                emptyBranch: true,
                overwrite: false
            )
            #expect(created == true)
        }

        @Test("deleteSpaceBranch sends delete request", .mockURLSession)
        func testDeleteSpaceBranch() async throws {
            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/spaces/user/repo/branch/stale")
                #expect(request.httpMethod == "DELETE")

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!
                return (response, Data())
            }

            let client = createMockClient()
            let id: Repo.ID = "user/repo"
            let deleted = try await client.deleteSpaceBranch(id, branchName: "stale")
            #expect(deleted == true)
        }

        @Test("compareDatasetRevisions sends raw query parameter", .mockURLSession)
        func testCompareDatasetRevisionsWithRawQuery() async throws {
            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/datasets/user/repo/compare/main...feature")
                #expect(request.url?.query?.contains("raw=true") == true)
                #expect(request.httpMethod == "GET")

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!
                return (response, Data(#""diff output""#.utf8))
            }

            let client = createMockClient()
            let id: Repo.ID = "user/repo"
            let diff = try await client.compareDatasetRevisions(
                id,
                compare: "main...feature",
                raw: true
            )
            #expect(diff == "diff output")
        }

        @Test("git operations propagate HTTP errors", .mockURLSession)
        func testGitOperationsErrorPropagation() async throws {
            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/models/user/repo/tree/main")

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 404,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!
                return (response, Data(#"{"error":"Not found"}"#.utf8))
            }

            let client = createMockClient()
            let id: Repo.ID = "user/repo"
            await #expect(throws: HTTPClientError.self) {
                _ = try await client.modelTree(id)
            }
        }
    }
#endif
