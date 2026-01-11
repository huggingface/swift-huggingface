import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Replay
import Testing

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("Repo Tests", .serialized)
    struct RepoTests {
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

        @Test(
            "Create a new repository",
            .replay(
                stubs: [
                    .post(
                        "https://huggingface.co/api/repos/create",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "url": "https://huggingface.co/user/my-new-model",
                            "repoID": "12345"
                        }
                        """
                    }
                ]
            )
        )
        func testCreateRepo() async throws {
            let client = createClient()
            let result = try await client.createRepo(kind: .model, name: "my-new-model")

            #expect(result.url == "https://huggingface.co/user/my-new-model")
            #expect(result.repoId == "12345")
        }

        @Test(
            "Create a private repository",
            .replay(
                stubs: [
                    .post(
                        "https://huggingface.co/api/repos/create",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "url": "https://huggingface.co/user/my-private-model",
                            "repoId": "67890"
                        }
                        """
                    }
                ]
            )
        )
        func testCreatePrivateRepo() async throws {
            let client = createClient()
            let result = try await client.createRepo(
                kind: .model,
                name: "my-private-model",
                visibility: .private
            )

            #expect(result.url == "https://huggingface.co/user/my-private-model")
        }

        @Test(
            "Create repository under organization",
            .replay(
                stubs: [
                    .post(
                        "https://huggingface.co/api/repos/create",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "url": "https://huggingface.co/myorg/org-model",
                            "repoId": "11111"
                        }
                        """
                    }
                ]
            )
        )
        func testCreateRepoUnderOrganization() async throws {
            let client = createClient()
            let result = try await client.createRepo(
                kind: .model,
                name: "org-model",
                organization: "myorg"
            )

            #expect(result.url == "https://huggingface.co/myorg/org-model")
        }

        @Test(
            "Update repository settings",
            .replay(
                stubs: [
                    .put(
                        "https://huggingface.co/api/models/user/my-model/settings",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        ""
                    }
                ]
            )
        )
        func testUpdateRepoSettings() async throws {
            let client = createClient()
            let repoID: Repo.ID = "user/my-model"
            let settings = Repo.Settings(visibility: .private, discussionsDisabled: true)
            let success = try await client.updateRepoSettings(
                kind: .model,
                repoID,
                settings: settings
            )

            #expect(success == true)
        }

        @Test(
            "Move repository",
            .replay(
                stubs: [
                    .post(
                        "https://huggingface.co/api/repos/move",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        ""
                    }
                ]
            )
        )
        func testMoveRepo() async throws {
            let client = createClient()
            let fromID: Repo.ID = "user/old-name"
            let toID: Repo.ID = "user/new-name"
            let success = try await client.moveRepo(kind: .model, from: fromID, to: toID)

            #expect(success == true)
        }

        @Test(
            "Move repository across namespaces",
            .replay(
                stubs: [
                    .post(
                        "https://huggingface.co/api/repos/move",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        ""
                    }
                ]
            )
        )
        func testMoveRepoAcrossNamespaces() async throws {
            let client = createClient()
            let fromID: Repo.ID = "user/model"
            let toID: Repo.ID = "org/model"
            let success = try await client.moveRepo(kind: .model, from: fromID, to: toID)

            #expect(success == true)
        }

        @Test(
            "Create repo requires authentication",
            .replay(
                stubs: [
                    .post(
                        "https://huggingface.co/api/repos/create",
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
        func testCreateRepoRequiresAuth() async throws {
            let client = createClient(bearerToken: nil)

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.createRepo(kind: .model, name: "test-model")
            }
        }

        @Test(
            "Handle repo name conflict",
            .replay(
                stubs: [
                    .post(
                        "https://huggingface.co/api/repos/create",
                        409,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "error": "Repository already exists"
                        }
                        """
                    }
                ]
            )
        )
        func testCreateRepoNameConflict() async throws {
            let client = createClient()

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.createRepo(kind: .model, name: "existing-model")
            }
        }
    }

#endif  // swift(>=6.1)
