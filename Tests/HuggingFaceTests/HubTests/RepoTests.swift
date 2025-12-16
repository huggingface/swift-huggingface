import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Testing

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("Repo Tests", .serialized)
    struct RepoTests {
        /// Helper to create a URL session with mock protocol handlers
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

        @Test("Create a new repository", .mockURLSession)
        func testCreateRepo() async throws {
            let mockResponse = """
                {
                    "url": "https://huggingface.co/user/my-new-model",
                    "repoID": "12345"
                }
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/repos/create")
                #expect(request.httpMethod == "POST")
                #expect(request.value(forHTTPHeaderField: "Authorization") == "Bearer test_token")

                // Verify request body
                if let body = request.httpBody,
                    let json = try? JSONSerialization.jsonObject(with: body) as? [String: Any]
                {
                    #expect(json["type"] as? String == "model")
                    #expect(json["name"] as? String == "my-new-model")
                    #expect(json["private"] as? Bool == false)
                }

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
            let result = try await client.createRepo(kind: .model, name: "my-new-model")

            #expect(result.url == "https://huggingface.co/user/my-new-model")
            #expect(result.repoId == "12345")
        }

        @Test("Create a private repository", .mockURLSession)
        func testCreatePrivateRepo() async throws {
            let mockResponse = """
                {
                    "url": "https://huggingface.co/user/my-private-model",
                    "repoId": "67890"
                }
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/repos/create")
                #expect(request.httpMethod == "POST")

                // Verify request body
                if let body = request.httpBody,
                    let json = try? JSONSerialization.jsonObject(with: body) as? [String: Any]
                {
                    #expect(json["private"] as? Bool == true)
                }

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
            let result = try await client.createRepo(
                kind: .model,
                name: "my-private-model",
                visibility: .private
            )

            #expect(result.url == "https://huggingface.co/user/my-private-model")
        }

        @Test("Create repository under organization", .mockURLSession)
        func testCreateRepoUnderOrganization() async throws {
            let mockResponse = """
                {
                    "url": "https://huggingface.co/myorg/org-model",
                    "repoId": "11111"
                }
                """

            await MockURLProtocol.setHandler { request in
                // Verify request body
                if let body = request.httpBody,
                    let json = try? JSONSerialization.jsonObject(with: body) as? [String: Any]
                {
                    #expect(json["organization"] as? String == "myorg")
                }

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
            let result = try await client.createRepo(
                kind: .model,
                name: "org-model",
                organization: "myorg"
            )

            #expect(result.url == "https://huggingface.co/myorg/org-model")
        }

        @Test("Update repository settings", .mockURLSession)
        func testUpdateRepoSettings() async throws {
            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/models/user/my-model/settings")
                #expect(request.httpMethod == "PUT")
                #expect(request.value(forHTTPHeaderField: "Authorization") == "Bearer test_token")

                // Verify request body
                if let body = request.httpBody,
                    let json = try? JSONSerialization.jsonObject(with: body) as? [String: Any]
                {
                    #expect(json["private"] as? Bool == true)
                    #expect(json["discussionsDisabled"] as? Bool == true)
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
            let repoID: Repo.ID = "user/my-model"
            let settings = Repo.Settings(visibility: .private, discussionsDisabled: true)
            let success = try await client.updateRepoSettings(
                kind: .model,
                repoID,
                settings: settings
            )

            #expect(success == true)
        }

        @Test("Move repository", .mockURLSession)
        func testMoveRepo() async throws {
            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/repos/move")
                #expect(request.httpMethod == "POST")
                #expect(request.value(forHTTPHeaderField: "Authorization") == "Bearer test_token")

                // Verify request body
                if let body = request.httpBody,
                    let json = try? JSONSerialization.jsonObject(with: body) as? [String: Any]
                {
                    #expect(json["fromRepo"] as? String == "user/old-name")
                    #expect(json["toRepo"] as? String == "user/new-name")
                    #expect(json["type"] as? String == "model")
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
            let fromID: Repo.ID = "user/old-name"
            let toID: Repo.ID = "user/new-name"
            let success = try await client.moveRepo(kind: .model, from: fromID, to: toID)

            #expect(success == true)
        }

        @Test("Move repository across namespaces", .mockURLSession)
        func testMoveRepoAcrossNamespaces() async throws {
            await MockURLProtocol.setHandler { request in
                // Verify request body
                if let body = request.httpBody,
                    let json = try? JSONSerialization.jsonObject(with: body) as? [String: Any]
                {
                    #expect(json["fromRepo"] as? String == "user/model")
                    #expect(json["toRepo"] as? String == "org/model")
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
            let fromID: Repo.ID = "user/model"
            let toID: Repo.ID = "org/model"
            let success = try await client.moveRepo(kind: .model, from: fromID, to: toID)

            #expect(success == true)
        }

        @Test("Create repo requires authentication", .mockURLSession)
        func testCreateRepoRequiresAuth() async throws {
            let errorResponse = """
                {
                    "error": "Unauthorized"
                }
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.value(forHTTPHeaderField: "Authorization") == nil)

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 401,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(errorResponse.utf8))
            }

            let client = createMockClient(bearerToken: nil)

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.createRepo(kind: .model, name: "test-model")
            }
        }

        @Test("Handle repo name conflict", .mockURLSession)
        func testCreateRepoNameConflict() async throws {
            let errorResponse = """
                {
                    "error": "Repository already exists"
                }
                """

            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 409,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(errorResponse.utf8))
            }

            let client = createMockClient()

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.createRepo(kind: .model, name: "existing-model")
            }
        }
    }

#endif  // swift(>=6.1)
