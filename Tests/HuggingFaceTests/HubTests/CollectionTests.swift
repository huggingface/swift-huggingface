import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Replay
import Testing

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("Collection Tests", .serialized)
    struct CollectionTests {
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
            "List collections with no parameters",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/collections",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        [
                            {
                                "id": "123",
                                "slug": "user/my-collection",
                                "title": "My Collection",
                                "description": "A test collection",
                                "owner": "user",
                                "position": 1,
                                "private": false,
                                "theme": "default",
                                "upvotes": 10,
                                "items": []
                            },
                            {
                                "id": "456",
                                "slug": "org/another-collection",
                                "title": "Another Collection",
                                "owner": "org",
                                "position": 2,
                                "private": false,
                                "theme": "default",
                                "upvotes": 5,
                                "items": []
                            }
                        ]
                        """
                    }
                ]
            )
        )
        func testListCollections() async throws {
            let client = createClient()
            let result = try await client.listCollections()

            #expect(result.items.count == 2)
            #expect(result.items[0].slug == "user/my-collection")
            #expect(result.items[0].title == "My Collection")
            #expect(result.items[1].slug == "org/another-collection")
        }

        @Test(
            "List collections with owner filter",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/collections?owner=user",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        [
                            {
                                "id": "123",
                                "slug": "user/my-collection",
                                "title": "My Collection",
                                "owner": "user",
                                "position": 1,
                                "private": false,
                                "theme": "default",
                                "upvotes": 10,
                                "items": []
                            }
                        ]
                        """
                    }
                ]
            )
        )
        func testListCollectionsWithOwner() async throws {
            let client = createClient()
            let result = try await client.listCollections(owner: "user")

            #expect(result.items.count == 1)
            #expect(result.items[0].owner == "user")
        }

        @Test(
            "Get specific collection",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/collections/user/my-collection",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "id": "123",
                            "slug": "user/my-collection",
                            "title": "My Collection",
                            "description": "A test collection",
                            "owner": "user",
                            "position": 1,
                            "private": false,
                            "theme": "default",
                            "upvotes": 10,
                            "items": [
                                {
                                    "item_type": "model",
                                    "item_id": "facebook/bart-large",
                                    "position": 0,
                                    "note": "A great model"
                                }
                            ]
                        }
                        """
                    }
                ]
            )
        )
        func testGetCollection() async throws {
            let client = createClient()
            let collection = try await client.getCollection("user/my-collection")

            #expect(collection.slug == "user/my-collection")
            #expect(collection.title == "My Collection")
            #expect(collection.items?.count == 1)
            #expect(collection.items?[0].itemType == "model")
            #expect(collection.items?[0].itemID == "facebook/bart-large")
        }

        @Test(
            "Handle 404 error for collection",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/collections/nonexistent/collection",
                        404,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "error": "Collection not found"
                        }
                        """
                    }
                ]
            )
        )
        func testGetCollectionNotFound() async throws {
            let client = createClient()

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.getCollection("nonexistent/collection")
            }
        }
    }

#endif  // swift(>=6.1)
