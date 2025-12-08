import Foundation
import Testing

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("Organization Tests", .serialized)
    struct OrganizationTests {
        /// Helper to create a URL session with mock protocol handlers
        func createMockClient(bearerToken: String? = nil) -> HubClient {
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

        @Test("List organizations with no parameters", .mockURLSession)
        func testListOrganizations() async throws {
            let url = URL(string: "https://huggingface.co/api/organizations")!

            let mockResponse = """
                [
                    {
                        "name": "huggingface",
                        "fullname": "Hugging Face",
                        "avatarUrl": "https://avatars.example.com/huggingface",
                        "isEnterprise": true,
                        "createdAt": "2016-01-01T00:00:00.000Z",
                        "numMembers": 100,
                        "numModels": 5000,
                        "numDatasets": 1000,
                        "numSpaces": 500
                    },
                    {
                        "name": "testorg",
                        "fullname": "Test Organization",
                        "avatarUrl": "https://avatars.example.com/testorg",
                        "isEnterprise": false,
                        "createdAt": "2020-01-01T00:00:00.000Z",
                        "numMembers": 10
                    }
                ]
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/organizations")
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
            let result = try await client.listOrganizations()

            #expect(result.items.count == 2)
            #expect(result.items[0].name == "huggingface")
            #expect(result.items[0].fullName == "Hugging Face")
            #expect(result.items[0].isEnterprise == true)
            #expect(result.items[1].name == "testorg")
        }

        @Test("List organizations with search parameter", .mockURLSession)
        func testListOrganizationsWithSearch() async throws {
            let mockResponse = """
                [
                    {
                        "name": "huggingface",
                        "fullname": "Hugging Face",
                        "avatarUrl": "https://avatars.example.com/huggingface",
                        "isEnterprise": true,
                        "createdAt": "2016-01-01T00:00:00.000Z"
                    }
                ]
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/organizations")
                #expect(request.url?.query?.contains("search=huggingface") == true)

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
            let result = try await client.listOrganizations(search: "huggingface")

            #expect(result.items.count == 1)
            #expect(result.items[0].name == "huggingface")
        }

        @Test("Get specific organization", .mockURLSession)
        func testGetOrganization() async throws {
            let mockResponse = """
                {
                    "name": "huggingface",
                    "fullname": "Hugging Face",
                    "avatarUrl": "https://avatars.example.com/huggingface",
                    "isEnterprise": true,
                    "createdAt": "2016-01-01T00:00:00.000Z",
                    "numMembers": 100,
                    "numModels": 5000,
                    "numDatasets": 1000,
                    "numSpaces": 500,
                    "description": "The AI community building the future",
                    "website": "https://huggingface.co"
                }
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/organizations/huggingface")
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
            let org = try await client.getOrganization("huggingface")

            #expect(org.name == "huggingface")
            #expect(org.fullName == "Hugging Face")
            #expect(org.isEnterprise == true)
            #expect(org.numberOfMembers == 100)
            #expect(org.website == "https://huggingface.co")
        }

        @Test("List organization members", .mockURLSession)
        func testListOrganizationMembers() async throws {
            let mockResponse = """
                [
                    {
                        "name": "johndoe",
                        "fullname": "John Doe",
                        "avatarUrl": "https://avatars.example.com/johndoe",
                        "role": "admin"
                    },
                    {
                        "name": "janedoe",
                        "fullname": "Jane Doe",
                        "avatarUrl": "https://avatars.example.com/janedoe",
                        "role": "member"
                    }
                ]
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/organizations/testorg/members")
                #expect(request.httpMethod == "GET")
                #expect(request.value(forHTTPHeaderField: "Authorization") == "Bearer test_token")

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient(bearerToken: "test_token")
            let members = try await client.listOrganizationMembers("testorg")

            #expect(members.count == 2)
            #expect(members[0].name == "johndoe")
            #expect(members[0].role == "admin")
            #expect(members[1].name == "janedoe")
            #expect(members[1].role == "member")
        }

        @Test("List organization members requires authentication", .mockURLSession)
        func testListOrganizationMembersRequiresAuth() async throws {
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

            let client = createMockClient()  // No bearer token

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.listOrganizationMembers("testorg")
            }
        }

        @Test("Handle 404 error for organization", .mockURLSession)
        func testGetOrganizationNotFound() async throws {
            let errorResponse = """
                {
                    "error": "Organization not found"
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

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.getOrganization("nonexistent")
            }
        }
    }

#endif  // swift(>=6.1)
