import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Replay
import Testing

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("Space Tests", .serialized)
    struct SpaceTests {
        private func createClient() -> HubClient {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [PlaybackURLProtocol.self]
            let session = URLSession(configuration: configuration)
            return HubClient(
                session: session,
                host: URL(string: "https://huggingface.co")!,
                userAgent: "TestClient/1.0"
            )
        }

        @Test(
            "List spaces with no parameters",
            .replay(
                stubs: [
                    .get("https://huggingface.co/api/spaces", 200, ["Content-Type": "application/json"]) {
                        """
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
                    }
                ]
            )
        )
        func testListSpaces() async throws {
            let client = createClient()
            let result = try await client.listSpaces()

            #expect(result.items.count == 2)
            #expect(result.items[0].id == "user/demo-space")
            #expect(result.items[0].author == "user")
            #expect(result.items[1].id == "org/another-space")
        }

        @Test(
            "List spaces with search parameter",
            .replay(
                stubs: [
                    .get("https://huggingface.co/api/spaces?search=demo", 200, ["Content-Type": "application/json"]) {
                        """
                        [
                            {
                                "id": "user/demo-space",
                                "author": "user",
                                "likes": 100,
                                "sdk": "gradio"
                            }
                        ]
                        """
                    }
                ]
            )
        )
        func testListSpacesWithSearch() async throws {
            let client = createClient()
            let result = try await client.listSpaces(search: "demo")

            #expect(result.items.count == 1)
            #expect(result.items[0].id == "user/demo-space")
        }

        @Test(
            "Get specific space",
            .replay(
                stubs: [
                    .get("https://huggingface.co/api/spaces/user/demo-space", 200, ["Content-Type": "application/json"])
                    {
                        """
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
                    }
                ]
            )
        )
        func testGetSpace() async throws {
            let client = createClient()
            let repoID: Repo.ID = "user/demo-space"
            let space = try await client.getSpace(repoID)

            #expect(space.id == "user/demo-space")
            #expect(space.author == "user")
            #expect(space.sdk == "gradio")
        }

        @Test(
            "Get space runtime",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/spaces/user/demo-space/runtime",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "stage": "RUNNING",
                            "hardware": "cpu-basic",
                            "requestedHardware": "cpu-basic"
                        }
                        """
                    }
                ]
            )
        )
        func testGetSpaceRuntime() async throws {
            let client = createClient()
            let repoID: Repo.ID = "user/demo-space"
            let runtime = try await client.spaceRuntime(repoID)

            #expect(runtime.stage == "RUNNING")
            #expect(runtime.hardware == "cpu-basic")
        }

        @Test(
            "Sleep space",
            .replay(
                stubs: [
                    .post(
                        "https://huggingface.co/api/spaces/user/demo-space/sleeptime",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        ""
                    }
                ]
            )
        )
        func testSleepSpace() async throws {
            let client = createClient()
            let repoID: Repo.ID = "user/demo-space"
            let success = try await client.sleepSpace(repoID)

            #expect(success == true)
        }

        @Test(
            "Restart space",
            .replay(
                stubs: [
                    .post(
                        "https://huggingface.co/api/spaces/user/demo-space/restart",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        ""
                    }
                ]
            )
        )
        func testRestartSpace() async throws {
            let client = createClient()
            let repoID: Repo.ID = "user/demo-space"
            let success = try await client.restartSpace(repoID)

            #expect(success == true)
        }

        @Test(
            "Restart space with factory option",
            .replay(
                stubs: [
                    .post(
                        "https://huggingface.co/api/spaces/user/demo-space/restart",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        ""
                    }
                ]
            )
        )
        func testRestartSpaceFactory() async throws {
            let client = createClient()
            let repoID: Repo.ID = "user/demo-space"
            let success = try await client.restartSpace(repoID, factory: true)

            #expect(success == true)
        }

        @Test(
            "Handle 404 error for space",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/spaces/nonexistent/space",
                        404,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "error": "Space not found"
                        }
                        """
                    }
                ]
            )
        )
        func testGetSpaceNotFound() async throws {
            let client = createClient()
            let repoID: Repo.ID = "nonexistent/space"

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.getSpace(repoID)
            }
        }
    }

#endif  // swift(>=6.1)
