import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Replay
import Testing

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("Organization Tests", .serialized)
    struct OrganizationTests {
        private func createClient(bearerToken: String? = nil) -> HubClient {
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

        @Test(
            "List organizations with no parameters",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/organizations",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
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
                    }
                ]
            )
        )
        func testListOrganizations() async throws {
            let client = createClient()
            let result = try await client.listOrganizations()

            #expect(result.items.count == 2)
            #expect(result.items[0].name == "huggingface")
            #expect(result.items[0].fullName == "Hugging Face")
            #expect(result.items[0].isEnterprise == true)
            #expect(result.items[1].name == "testorg")
        }

        @Test(
            "List organizations with search parameter",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/organizations?search=huggingface",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
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
                    }
                ]
            )
        )
        func testListOrganizationsWithSearch() async throws {
            let client = createClient()
            let result = try await client.listOrganizations(search: "huggingface")

            #expect(result.items.count == 1)
            #expect(result.items[0].name == "huggingface")
        }

        @Test(
            "Get specific organization",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/organizations/huggingface",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
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
                    }
                ]
            )
        )
        func testGetOrganization() async throws {
            let client = createClient()
            let org = try await client.getOrganization("huggingface")

            #expect(org.name == "huggingface")
            #expect(org.fullName == "Hugging Face")
            #expect(org.isEnterprise == true)
            #expect(org.numberOfMembers == 100)
            #expect(org.website == "https://huggingface.co")
        }

        @Test(
            "List organization members",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/organizations/testorg/members",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
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
                    }
                ]
            )
        )
        func testListOrganizationMembers() async throws {
            let client = createClient(bearerToken: "test_token")
            let members = try await client.listOrganizationMembers("testorg")

            #expect(members.count == 2)
            #expect(members[0].name == "johndoe")
            #expect(members[0].role == "admin")
            #expect(members[1].name == "janedoe")
            #expect(members[1].role == "member")
        }

        @Test(
            "List organization members requires authentication",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/organizations/testorg/members",
                        401,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "error": "Unauthorized"
                        }
                        """
                    }
                ]
            )
        )
        func testListOrganizationMembersRequiresAuth() async throws {
            let client = createClient()

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.listOrganizationMembers("testorg")
            }
        }

        @Test(
            "Handle 404 error for organization",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/organizations/nonexistent",
                        404,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "error": "Organization not found"
                        }
                        """
                    }
                ]
            )
        )
        func testGetOrganizationNotFound() async throws {
            let client = createClient()

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.getOrganization("nonexistent")
            }
        }
    }

#endif  // swift(>=6.1)
