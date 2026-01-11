import Foundation

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif
import Replay
import Testing

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("Discussion Tests", .serialized)
    struct DiscussionTests {
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
            "List discussions for a model",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/models/facebook/bart-large/discussions",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "discussions": [
                                {
                                    "number": 1,
                                    "title": "Bug in inference",
                                    "status": "open",
                                    "author": {
                                        "name": "user1"
                                    },
                                    "repo": "facebook/bart-large",
                                    "createdAt": "2023-01-01T00:00:00.000Z",
                                    "isPullRequest": false,
                                    "numberOfComments": 3,
                                    "numberOfReactionUsers": 2,
                                    "pinned": false,
                                    "topReactions": [],
                                    "repoOwner": {
                                        "name": "facebook",
                                        "type": "organization",
                                        "isParticipating": false,
                                        "isDiscussionAuthor": false
                                    }
                                },
                                {
                                    "number": 2,
                                    "title": "Feature request",
                                    "status": "open",
                                    "author": {
                                        "name": "user2"
                                    },
                                    "repo": "facebook/bart-large",
                                    "createdAt": "2023-01-02T00:00:00.000Z",
                                    "isPullRequest": false,
                                    "numberOfComments": 1,
                                    "numberOfReactionUsers": 0,
                                    "pinned": false,
                                    "topReactions": [],
                                    "repoOwner": {
                                        "name": "facebook",
                                        "type": "organization",
                                        "isParticipating": false,
                                        "isDiscussionAuthor": false
                                    }
                                }
                            ],
                            "count": 2,
                            "start": 0
                        }
                        """
                    }
                ]
            )
        )
        func testListDiscussions() async throws {
            let client = createClient()
            let repoID: Repo.ID = "facebook/bart-large"
            let (discussions, _, _, _) = try await client.listDiscussions(
                kind: .model,
                repoID
            )

            #expect(discussions.count == 2)
            #expect(discussions[0].number == 1)
            #expect(discussions[0].title == "Bug in inference")
            #expect(discussions[1].number == 2)
        }

        @Test(
            "List discussions with status filter",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/models/facebook/bart-large/discussions?status=closed",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "discussions": [
                                {
                                    "number": 3,
                                    "title": "Closed issue",
                                    "status": "closed",
                                    "author": {
                                        "name": "user3"
                                    },
                                    "repo": "facebook/bart-large",
                                    "createdAt": "2023-01-03T00:00:00.000Z",
                                    "isPullRequest": false,
                                    "numberOfComments": 0,
                                    "numberOfReactionUsers": 0,
                                    "pinned": false,
                                    "topReactions": [],
                                    "repoOwner": {
                                        "name": "facebook",
                                        "type": "organization",
                                        "isParticipating": false,
                                        "isDiscussionAuthor": false
                                    }
                                }
                            ],
                            "count": 1,
                            "start": 0
                        }
                        """
                    }
                ]
            )
        )
        func testListDiscussionsWithStatus() async throws {
            let client = createClient()
            let repoID: Repo.ID = "facebook/bart-large"
            let (discussions, _, _, _) = try await client.listDiscussions(
                kind: .model,
                repoID,
                status: "closed"
            )

            #expect(discussions.count == 1)
            #expect(discussions[0].status == .closed)
        }

        @Test(
            "Get specific discussion",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/models/facebook/bart-large/discussions/1",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "num": 1,
                            "title": "Bug in inference",
                            "status": "open",
                            "author": {
                                "name": "user1",
                                "avatarURL": "https://avatars.example.com/user1"
                            },
                            "createdAt": "2023-01-01T00:00:00.000Z",
                            "isPullRequest": false,
                            "comments": [
                                {
                                    "id": "comment-1",
                                    "author": {
                                        "name": "user1"
                                    },
                                    "createdAt": "2023-01-01T00:00:00.000Z",
                                    "content": "I found a bug"
                                }
                            ]
                        }
                        """
                    }
                ]
            )
        )
        func testGetDiscussion() async throws {
            let client = createClient()
            let repoID: Repo.ID = "facebook/bart-large"
            let discussion = try await client.getDiscussion(
                kind: .model,
                repoID,
                number: 1
            )

            #expect(discussion.number == 1)
            #expect(discussion.title == "Bug in inference")
            #expect(discussion.comments?.count == 1)
        }

        @Test(
            "Add comment to discussion",
            .replay(
                stubs: [
                    .post(
                        "https://huggingface.co/api/models/facebook/bart-large/discussions/1/comment",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        ""
                    }
                ]
            )
        )
        func testAddComment() async throws {
            let client = createClient()
            let repoID: Repo.ID = "facebook/bart-large"
            let success = try await client.addCommentToDiscussion(
                kind: .model,
                repoID,
                number: 1,
                comment: "Thanks for reporting!"
            )

            #expect(success == true)
        }

        @Test(
            "Merge pull request discussion",
            .replay(
                stubs: [
                    .post(
                        "https://huggingface.co/api/models/user/my-model/discussions/5/merge",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        ""
                    }
                ]
            )
        )
        func testMergeDiscussion() async throws {
            let client = createClient()
            let repoID: Repo.ID = "user/my-model"
            let success = try await client.mergeDiscussion(
                kind: .model,
                repoID,
                number: 5
            )

            #expect(success == true)
        }

        @Test(
            "Pin discussion",
            .replay(
                stubs: [
                    .post(
                        "https://huggingface.co/api/models/user/my-model/discussions/1/pin",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        ""
                    }
                ]
            )
        )
        func testPinDiscussion() async throws {
            let client = createClient()
            let repoID: Repo.ID = "user/my-model"
            let success = try await client.pinDiscussion(
                kind: .model,
                repoID,
                number: 1
            )

            #expect(success == true)
        }

        @Test(
            "Update discussion status",
            .replay(
                stubs: [
                    .patch(
                        "https://huggingface.co/api/models/user/my-model/discussions/1/status",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        ""
                    }
                ]
            )
        )
        func testUpdateDiscussionStatus() async throws {
            let client = createClient()
            let repoID: Repo.ID = "user/my-model"
            let success = try await client.updateDiscussionStatus(
                kind: .model,
                repoID,
                number: 1,
                status: .closed
            )

            #expect(success == true)
        }

        @Test(
            "Update discussion title",
            .replay(
                stubs: [
                    .patch(
                        "https://huggingface.co/api/models/user/my-model/discussions/1/title",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        ""
                    }
                ]
            )
        )
        func testUpdateDiscussionTitle() async throws {
            let client = createClient()
            let repoID: Repo.ID = "user/my-model"
            let success = try await client.updateDiscussionTitle(
                kind: .model,
                repoID,
                number: 1,
                title: "Updated title"
            )

            #expect(success == true)
        }

        @Test(
            "Mark discussions as read",
            .replay(
                stubs: [
                    .post(
                        "https://huggingface.co/api/discussions/mark-as-read",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        ""
                    }
                ]
            )
        )
        func testMarkDiscussionsAsRead() async throws {
            let client = createClient()
            let success = try await client.markDiscussionsAsRead([1, 2, 3])

            #expect(success == true)
        }

        @Test(
            "List discussions for dataset",
            .replay(
                stubs: [
                    .get(
                        "https://huggingface.co/api/datasets/_/squad/discussions",
                        200,
                        ["Content-Type": "application/json"]
                    ) {
                        """
                        {
                            "discussions": [
                                {
                                    "number": 1,
                                    "title": "Data quality issue",
                                    "status": "open",
                                    "author": {
                                        "name": "user1"
                                    },
                                    "repo": "_/squad",
                                    "createdAt": "2023-01-01T00:00:00.000Z",
                                    "isPullRequest": false,
                                    "numberOfComments": 2,
                                    "numberOfReactionUsers": 1,
                                    "pinned": false,
                                    "topReactions": [],
                                    "repoOwner": {
                                        "name": "_",
                                        "type": "user",
                                        "isParticipating": false,
                                        "isDiscussionAuthor": false
                                    }
                                }
                            ],
                            "count": 1,
                            "start": 0
                        }
                        """
                    }
                ]
            )
        )
        func testListDiscussionsForDataset() async throws {
            let client = createClient()
            let repoID: Repo.ID = "_/squad"
            let (discussions, _, _, _) = try await client.listDiscussions(
                kind: .dataset,
                repoID
            )

            #expect(discussions.count == 1)
            #expect(discussions[0].title == "Data quality issue")
        }
    }

#endif  // swift(>=6.1)
