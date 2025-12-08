import Foundation
import Testing

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("Discussion Tests", .serialized)
    struct DiscussionTests {
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

        @Test("List discussions for a model", .mockURLSession)
        func testListDiscussions() async throws {
            let mockResponse = """
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

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/models/facebook/bart-large/discussions")
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

        @Test("List discussions with status filter", .mockURLSession)
        func testListDiscussionsWithStatus() async throws {
            let mockResponse = """
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

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.query?.contains("status=closed") == true)

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
            let repoID: Repo.ID = "facebook/bart-large"
            let (discussions, _, _, _) = try await client.listDiscussions(
                kind: .model,
                repoID,
                status: "closed"
            )

            #expect(discussions.count == 1)
            #expect(discussions[0].status == .closed)
        }

        @Test("Get specific discussion", .mockURLSession)
        func testGetDiscussion() async throws {
            let mockResponse = """
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

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/models/facebook/bart-large/discussions/1")
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

        @Test("Add comment to discussion", .mockURLSession)
        func testAddComment() async throws {
            await MockURLProtocol.setHandler { request in
                #expect(
                    request.url?.path == "/api/models/facebook/bart-large/discussions/1/comment"
                )
                #expect(request.httpMethod == "POST")

                // Verify request body
                if let body = request.httpBody,
                    let json = try? JSONSerialization.jsonObject(with: body) as? [String: Any]
                {
                    #expect(json["comment"] as? String == "Thanks for reporting!")
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
            let repoID: Repo.ID = "facebook/bart-large"
            let success = try await client.addCommentToDiscussion(
                kind: .model,
                repoID,
                number: 1,
                comment: "Thanks for reporting!"
            )

            #expect(success == true)
        }

        @Test("Merge pull request discussion", .mockURLSession)
        func testMergeDiscussion() async throws {
            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/models/user/my-model/discussions/5/merge")
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
            let repoID: Repo.ID = "user/my-model"
            let success = try await client.mergeDiscussion(
                kind: .model,
                repoID,
                number: 5
            )

            #expect(success == true)
        }

        @Test("Pin discussion", .mockURLSession)
        func testPinDiscussion() async throws {
            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/models/user/my-model/discussions/1/pin")
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
            let repoID: Repo.ID = "user/my-model"
            let success = try await client.pinDiscussion(
                kind: .model,
                repoID,
                number: 1
            )

            #expect(success == true)
        }

        @Test("Update discussion status", .mockURLSession)
        func testUpdateDiscussionStatus() async throws {
            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/models/user/my-model/discussions/1/status")
                #expect(request.httpMethod == "PATCH")

                // Verify request body
                if let body = request.httpBody,
                    let json = try? JSONSerialization.jsonObject(with: body) as? [String: Any]
                {
                    #expect(json["status"] as? String == "closed")
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
            let success = try await client.updateDiscussionStatus(
                kind: .model,
                repoID,
                number: 1,
                status: .closed
            )

            #expect(success == true)
        }

        @Test("Update discussion title", .mockURLSession)
        func testUpdateDiscussionTitle() async throws {
            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/models/user/my-model/discussions/1/title")
                #expect(request.httpMethod == "PATCH")

                // Verify request body
                if let body = request.httpBody,
                    let json = try? JSONSerialization.jsonObject(with: body) as? [String: Any]
                {
                    #expect(json["title"] as? String == "Updated title")
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
            let success = try await client.updateDiscussionTitle(
                kind: .model,
                repoID,
                number: 1,
                title: "Updated title"
            )

            #expect(success == true)
        }

        @Test("Mark discussions as read", .mockURLSession)
        func testMarkDiscussionsAsRead() async throws {
            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/discussions/mark-as-read")
                #expect(request.httpMethod == "POST")

                // Verify request body
                if let body = request.httpBody,
                    let json = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
                    let nums = json["discussionNums"] as? [Int]
                {
                    #expect(nums.count == 3)
                    #expect(nums.contains(1))
                    #expect(nums.contains(2))
                    #expect(nums.contains(3))
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
            let success = try await client.markDiscussionsAsRead([1, 2, 3])

            #expect(success == true)
        }

        @Test("List discussions for dataset", .mockURLSession)
        func testListDiscussionsForDataset() async throws {
            let mockResponse = """
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

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/datasets/_/squad/discussions")
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
