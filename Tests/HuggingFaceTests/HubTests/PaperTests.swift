import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Replay
import Testing

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("Paper Tests", .serialized)
    struct PaperTests {
        private func createClient() -> HubClient {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [PlaybackURLProtocol.self]
            let session = URLSession(configuration: configuration)

            return HubClient(
                session: session,
                host: URL(string: "https://huggingface.co")!,
                userAgent: "TestClient/1.0",
                cache: nil
            )
        }

        @Test(
            "List papers with no parameters",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/papers",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        [
                            {
                                "id": "2103.00020",
                                "title": "Learning Transferable Visual Models From Natural Language Supervision",
                                "summary": "State-of-the-art computer vision systems...",
                                "publishedAt": "2021-03-01T00:00:00.000Z"
                            },
                            {
                                "id": "2005.14165",
                                "title": "Language Models are Few-Shot Learners",
                                "summary": "Recent work has demonstrated substantial gains...",
                                "publishedAt": "2020-05-28T00:00:00.000Z"
                            }
                        ]
                        """
                    }
                ]
            )
        )
        func testListPapers() async throws {
            let client = createClient()
            let result = try await client.listPapers()

            #expect(result.items.count == 2)
            #expect(result.items[0].id == "2103.00020")
            #expect(
                result.items[0].title
                    == "Learning Transferable Visual Models From Natural Language Supervision"
            )
            #expect(result.items[1].id == "2005.14165")
        }

        @Test(
            "List papers with search parameter",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/papers?search=CLIP",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        [
                            {
                                "id": "2103.00020",
                                "title": "Learning Transferable Visual Models From Natural Language Supervision",
                                "summary": "State-of-the-art computer vision systems...",
                                "publishedAt": "2021-03-01T00:00:00.000Z"
                            }
                        ]
                        """
                    }
                ]
            )
        )
        func testListPapersWithSearch() async throws {
            let client = createClient()
            let result = try await client.listPapers(search: "CLIP")

            #expect(result.items.count == 1)
            #expect(result.items[0].id == "2103.00020")
        }

        @Test(
            "List papers with sort parameter",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/papers?sort=trending",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        [
                            {
                                "id": "2005.14165",
                                "title": "Language Models are Few-Shot Learners",
                                "publishedAt": "2020-05-28T00:00:00.000Z"
                            }
                        ]
                        """
                    }
                ]
            )
        )
        func testListPapersWithSort() async throws {
            let client = createClient()
            let result = try await client.listPapers(sort: "trending")

            #expect(result.items.count == 1)
        }

        @Test(
            "List papers with limit parameter",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/papers?limit=1",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        [
                            {
                                "id": "2103.00020",
                                "title": "Learning Transferable Visual Models From Natural Language Supervision",
                                "publishedAt": "2021-03-01T00:00:00.000Z"
                            }
                        ]
                        """
                    }
                ]
            )
        )
        func testListPapersWithLimit() async throws {
            let client = createClient()
            let result = try await client.listPapers(limit: 1)

            #expect(result.items.count == 1)
        }

        @Test(
            "Get specific paper",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/papers/2103.00020",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "id": "2103.00020",
                            "title": "Learning Transferable Visual Models From Natural Language Supervision",
                            "summary": "State-of-the-art computer vision systems are trained to predict...",
                            "published": "2021-03-01T00:00:00.000Z",
                            "authors": ["Alec Radford", "Jong Wook Kim"],
                            "upvotes": 150
                        }
                        """
                    }
                ]
            )
        )
        func testGetPaper() async throws {
            let client = createClient()
            let paper = try await client.getPaper("2103.00020")

            #expect(paper.id == "2103.00020")
            #expect(
                paper.title
                    == "Learning Transferable Visual Models From Natural Language Supervision"
            )
            #expect(paper.upvotes == 150)
        }

        @Test(
            "Handle 404 error for paper",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/papers/9999.99999",
                        404,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "error": "Paper not found"
                        }
                        """
                    }
                ]
            )
        )
        func testGetPaperNotFound() async throws {
            let client = createClient()

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.getPaper("9999.99999")
            }
        }
    }

#endif  // swift(>=6.1)
