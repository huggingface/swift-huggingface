import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Testing

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("Space Tests", .serialized)
    struct SpaceTests {
        /// Helper to create a URL session with mock protocol handlers
        func createMockClient() -> HubClient {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [MockURLProtocol.self]
            let session = URLSession(configuration: configuration)
            return HubClient(
                session: session,
                host: URL(string: "https://huggingface.co")!,
                userAgent: "TestClient/1.0"
            )
        }

        @Test("List spaces with no parameters", .mockURLSession)
        func testListSpaces() async throws {
            let url = URL(string: "https://huggingface.co/api/spaces")!

            let mockResponse = """
                [
                    {
                        "id": "user/demo-space",
                        "author": "user",
                        "likes": 100,
                        "sdk": "gradio"
                    },
                    {
                        "id": "org/another-space",
                        "author": "org",
                        "likes": 50,
                        "sdk": "streamlit"
                    }
                ]
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/spaces")
                #expect(request.httpMethod == "GET")

                let response = HTTPURLResponse(
                    url: url,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
            let result = try await client.listSpaces()

            #expect(result.items.count == 2)
            #expect(result.items[0].id == "user/demo-space")
            #expect(result.items[0].author == "user")
            #expect(result.items[1].id == "org/another-space")
        }

        @Test("List spaces with search parameter", .mockURLSession)
        func testListSpacesWithSearch() async throws {
            let mockResponse = """
                [
                    {
                        "id": "user/demo-space",
                        "author": "user",
                        "likes": 100,
                        "sdk": "gradio"
                    }
                ]
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/spaces")
                #expect(request.url?.query?.contains("search=demo") == true)

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
            let result = try await client.listSpaces(search: "demo")

            #expect(result.items.count == 1)
            #expect(result.items[0].id == "user/demo-space")
        }

        @Test("List spaces with additional query parameters", .mockURLSession)
        func testListSpacesWithAdditionalParameters() async throws {
            let mockResponse = """
                [
                    {
                        "id": "user/demo-space"
                    }
                ]
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/spaces")

                let queryItems = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?.queryItems
                let query = Dictionary(uniqueKeysWithValues: (queryItems ?? []).map { ($0.name, $0.value ?? "") })

                #expect(query["datasets"]?.contains("datasets/squad") == true)
                #expect(query["models"]?.contains("google/bert-base-uncased") == true)
                #expect(query["linked"] == "true")

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
            let result = try await client.listSpaces(
                datasets: ["datasets/squad"],
                models: ["google/bert-base-uncased"],
                linked: true
            )

            #expect(result.items.count == 1)
        }

        @Test("Get specific space", .mockURLSession)
        func testGetSpace() async throws {
            let mockResponse = """
                {
                    "id": "user/demo-space",
                    "author": "user",
                    "likes": 100,
                    "sdk": "gradio",
                    "runtime": {
                        "stage": "RUNNING"
                    }
                }
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/spaces/user/demo-space")
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
            let repoID: Repo.ID = "user/demo-space"
            let space = try await client.getSpace(repoID)

            #expect(space.id == "user/demo-space")
            #expect(space.author == "user")
            #expect(space.sdk == "gradio")
        }

        @Test("Get space runtime", .mockURLSession)
        func testGetSpaceRuntime() async throws {
            let mockResponse = """
                {
                    "stage": "RUNNING",
                    "hardware": "cpu-basic",
                    "requestedHardware": "cpu-basic"
                }
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/spaces/user/demo-space/runtime")
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
            let repoID: Repo.ID = "user/demo-space"
            let runtime = try await client.spaceRuntime(repoID)

            #expect(runtime.stage == "RUNNING")
            #expect(runtime.hardware == "cpu-basic")
        }

        @Test("Sleep space", .mockURLSession)
        func testSleepSpace() async throws {
            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/spaces/user/demo-space/sleeptime")
                #expect(request.httpMethod == "POST")

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data())
            }

            let client = createMockClient()
            let repoID: Repo.ID = "user/demo-space"
            let success = try await client.sleepSpace(repoID)

            #expect(success == true)
        }

        @Test("Restart space", .mockURLSession)
        func testRestartSpace() async throws {
            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/spaces/user/demo-space/restart")
                #expect(request.httpMethod == "POST")

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data())
            }

            let client = createMockClient()
            let repoID: Repo.ID = "user/demo-space"
            let success = try await client.restartSpace(repoID)

            #expect(success == true)
        }

        @Test("Restart space with factory option", .mockURLSession)
        func testRestartSpaceFactory() async throws {
            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/spaces/user/demo-space/restart")
                #expect(request.httpMethod == "POST")

                // Verify factory parameter is in the request body
                if let body = request.httpBody,
                    let json = try? JSONSerialization.jsonObject(with: body) as? [String: Any]
                {
                    #expect(json["factory"] as? Bool == true)
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
            let repoID: Repo.ID = "user/demo-space"
            let success = try await client.restartSpace(repoID, factory: true)

            #expect(success == true)
        }

        @Test("Handle 404 error for space", .mockURLSession)
        func testGetSpaceNotFound() async throws {
            let errorResponse = """
                {
                    "error": "Space not found"
                }
                """

            await MockURLProtocol.setHandler { request in
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 404,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(errorResponse.utf8))
            }

            let client = createMockClient()
            let repoID: Repo.ID = "nonexistent/space"

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.getSpace(repoID)
            }
        }
    }

#endif  // swift(>=6.1)
