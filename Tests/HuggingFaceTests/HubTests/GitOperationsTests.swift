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
            // Captured on 2026-11-17 from
            // https://huggingface.co/api/models/openai-community/gpt2/tree/main/onnx?limit=2
            let responseBody =
                #"[{"type":"file","oid":"00bc2ad81dcdd043bf8490fdbd7cd82629fab389","size":879,"path":"onnx/config.json"},{"type":"file","oid":"d1e0f6797a98e4710ae4c6928bd19440cf39bfbc","size":653665842,"lfs":{"oid":"e3fc9615868ff8f5e0429b892a0f6ca692784ba6c4ca31c4e9ee8218e7cce34f","size":653665842,"pointerSize":134},"xetHash":"8e0a346a29483c54443f10b62662d77f82084cd3d8cde68b60013383754615be","path":"onnx/decoder_model.onnx"}]"#

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
            #expect(entries.map(\.path) == ["onnx/config.json", "onnx/decoder_model.onnx"])
            #expect(entries.map(\.type) == [.file, .file])
            #expect(entries[1].effectiveSize == 653_665_842)
        }

        @Test("datasetRefs decodes branches, tags, and target commits", .mockURLSession)
        func testDatasetRefs() async throws {
            // Captured on 2026-11-16 from
            // https://huggingface.co/api/datasets/nyu-mll/glue/refs
            let responseBody =
                #"{"tags":[],"branches":[{"name":"script","ref":"refs/heads/script","targetCommit":"06983ca6c77a72be60a85e65634e385b5cbe995c"},{"name":"main","ref":"refs/heads/main","targetCommit":"bcdcba79d07bc864c1c254ccfcedcce55bcc9a8c"}],"converts":[{"name":"parquet","ref":"refs/convert/parquet","targetCommit":"931904215b58b95ca536c5fe6103d54d03ff7842"}]}"#

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
            #expect(refs.branches.map(\.name) == ["script", "main"])
            let main = try #require(refs.branches.first { $0.name == "main" })
            #expect(main.ref == "refs/heads/main")
            #expect(main.targetCommit == "bcdcba79d07bc864c1c254ccfcedcce55bcc9a8c")
            #expect(refs.tags?.isEmpty == true)
        }

        @Test("spaceCommits builds commits path and decodes payload", .mockURLSession)
        func testSpaceCommits() async throws {
            // Captured on 2026-11-17 from
            // https://huggingface.co/api/spaces/gradio/hello_world/commits/main?limit=1
            let responseBody =
                #"[{"id":"9b2f9bdde5b35f2555eab5c23c3bbf664d3c513c","title":"Upload folder using huggingface_hub","message":"","authors":[{"user":"gradio-pr-bot","avatar":"https://cdn-avatars.huggingface.co/v1/production/uploads/653920e4b5a5431cee25051a/yUXDlNUCfU372lUYlVfuv.png"}],"date":"2026-11-12T20:42:02.000Z"}]"#

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
            #expect(commits[0].id == "9b2f9bdde5b35f2555eab5c23c3bbf664d3c513c")
            #expect(commits[0].title == "Upload folder using huggingface_hub")
            #expect(commits[0].message == "")
            #expect(commits[0].authors.map(\.user) == ["gradio-pr-bot"])
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
