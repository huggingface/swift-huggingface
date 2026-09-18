import Foundation
import Testing

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("Tree Pagination Tests", .serialized)
    struct TreePaginationTests {
        private final class Requests: @unchecked Sendable {
            private let lock = NSLock()
            private var storage: [URL] = []

            func record(_ request: URLRequest) -> Int {
                lock.lock()
                defer { lock.unlock() }
                storage.append(request.url!)
                return storage.count
            }

            var urls: [URL] {
                lock.lock()
                defer { lock.unlock() }
                return storage
            }
        }

        private func client(host: String = "https://huggingface.co/hf") -> HubClient {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [MockURLProtocol.self]
            return HubClient(
                session: URLSession(configuration: configuration),
                host: URL(string: host)!,
                bearerToken: "test_token"
            )
        }

        private func response(
            _ request: URLRequest,
            body: String = "[]",
            next: String? = nil,
            status: Int = 200
        ) -> (HTTPURLResponse, Data) {
            var headers = ["Content-Type": "application/json"]
            if let next { headers["Link"] = "<\(next)>; rel=\"next\"" }
            return (
                HTTPURLResponse(
                    url: request.url!,
                    statusCode: status,
                    httpVersion: "HTTP/1.1",
                    headerFields: headers
                )!,
                Data(body.utf8)
            )
        }

        @Test(
            "Recursive subtree pages preserve the pinned revision, prefix, and query",
            .mockURLSession,
            arguments: ["https://huggingface.co/hf", "https://huggingface.co/hf/"]
        )
        func testRecursiveSubtree(host: String) async throws {
            let requests = Requests()
            await MockURLProtocol.setHandler { request in
                let count = requests.record(request)
                #expect(request.url?.path == "/hf/api/models/org/model/tree/commit123/macos")
                #expect(request.value(forHTTPHeaderField: "Authorization") == "Bearer test_token")
                let query = URLComponents(url: request.url!, resolvingAgainstBaseURL: true)?.queryItems ?? []
                #expect(query.contains(URLQueryItem(name: "recursive", value: "true")))
                if count == 1 {
                    return response(
                        request,
                        body: #"[{"path":"macos/config.json","type":"file","size":10}]"#,
                        next: "?cursor=two"
                    )
                }
                #expect(query.contains(URLQueryItem(name: "cursor", value: "two")))
                return response(
                    request,
                    body: #"[{"path":"macos/nested/weights.bin","type":"file","size":20}]"#
                )
            }

            let pages = try await client(host: host).listAllTree(
                in: "org/model",
                revision: "commit123",
                path: "macos",
                recursive: true
            )
            #expect(requests.urls.count == 1)
            var paths: [String] = []
            for try await page in pages {
                paths += page.items.map(\.path)
            }
            #expect(paths == ["macos/config.json", "macos/nested/weights.bin"])
            #expect(paths.allSatisfy { $0.hasPrefix("macos/") })
            #expect(requests.urls.count == 2)
        }

        @Test(
            "Tree links cannot change the requested scope",
            .mockURLSession,
            arguments: [
                "/api/models/org/model/tree/commit123/macos?cursor=two",
                "https://huggingface.co/hf/api/models/org/model/tree/main?cursor=two&recursive=false",
                "//huggingface.co/hf/api/models/org/model/tree/commit123/macos?cursor=two",
            ]
        )
        func testLinkScope(next: String) async throws {
            let requests = Requests()
            await MockURLProtocol.setHandler { request in
                let count = requests.record(request)
                #expect(request.url?.path == "/hf/api/models/org/model/tree/commit123/macos")
                #expect(request.url?.query?.contains("recursive=true") == true)
                return response(request, next: count == 1 ? next : nil)
            }
            let hub = client()
            let first = try await hub.listTree(
                in: "org/model",
                revision: "commit123",
                path: "macos",
                recursive: true
            )
            let second = try await hub.nextPage(after: first)
            #expect(second != nil)
            #expect(requests.urls.count == 2)
        }

        @Test(
            "Tree pages support all repository kinds and encode revisions as one component",
            .mockURLSession,
            arguments: [Repo.Kind.model, .dataset, .space]
        )
        func testRepositoryKinds(kind: Repo.Kind) async throws {
            await MockURLProtocol.setHandler { request in
                let components = URLComponents(url: request.url!, resolvingAgainstBaseURL: true)!
                #expect(
                    components.percentEncodedPath
                        == "/hf/api/\(kind.pluralized)/org/model/tree/refs%2Fpr%2F1/dir%20name/nested"
                )
                #expect(components.queryItems == [URLQueryItem(name: "recursive", value: "false")])
                return response(request)
            }
            _ = try await client().listTree(
                in: "org/model",
                kind: kind,
                revision: "refs/pr/1",
                path: "dir name/nested"
            )
        }

        @Test("A failed second page can be retried without fetching the first", .mockURLSession)
        func testRetryFailedPage() async throws {
            let requests = Requests()
            await MockURLProtocol.setHandler { request in
                switch requests.record(request) {
                case 1:
                    return response(request, next: "?cursor=two")
                case 2:
                    return response(request, body: #"{"error":"Try again"}"#, status: 503)
                default:
                    return response(request, body: #"[{"path":"macos/file","type":"file"}]"#)
                }
            }
            let hub = client()
            let first = try await hub.listTree(in: "org/model", revision: "commit123", path: "macos")
            await #expect(throws: HTTPClientError.self) {
                _ = try await hub.nextPage(after: first)
            }
            let second = try await hub.nextPage(after: first)
            #expect(second?.items.first?.path == "macos/file")
            #expect(requests.urls.count == 3)
            #expect(requests.urls[1] == requests.urls[2])
            #expect(requests.urls[0] != requests.urls[1])
        }

        @Test(
            "Pagination rejects links to another origin before sending credentials",
            .mockURLSession,
            arguments: [
                "https://other.example/api/models?cursor=two",
                "//other.example/api/models?cursor=two",
                "http://huggingface.co/hf/api/models?cursor=two",
                "https://huggingface.co:444/hf/api/models?cursor=two",
                "https://user:password@huggingface.co/hf/api/models?cursor=two",
            ]
        )
        func testUnsafeLinks(next: String) async throws {
            let requests = Requests()
            await MockURLProtocol.setHandler { request in
                requests.record(request)
                return response(request, next: next)
            }
            let hub = client()
            let first = try await hub.listTree(in: "org/model")
            await #expect(throws: HTTPClientError.self) {
                _ = try await hub.nextPage(after: first)
            }
            #expect(requests.urls.count == 1)
        }

        @Test(
            "Repeated links and cycles stop before another request",
            .mockURLSession,
            arguments: [false, true]
        )
        func testRepeatedLinks(cycle: Bool) async throws {
            let requests = Requests()
            await MockURLProtocol.setHandler { request in
                let count = requests.record(request)
                let next = cycle && count == 2 ? "?cursor=three" : "?recursive=false&cursor=two#fragment"
                return response(request, next: next)
            }
            let pages = try await client().listAllTree(in: "org/model")
            var iterator = pages.makeAsyncIterator()
            _ = try await iterator.next()
            _ = try await iterator.next()
            if cycle { _ = try await iterator.next() }
            await #expect(throws: HTTPClientError.self) {
                _ = try await iterator.next()
            }
            #expect(requests.urls.count == (cycle ? 3 : 2))
        }

        @Test("Cancellation stops page iteration and explicit next-page requests", .mockURLSession)
        func testCancellation() async throws {
            let requests = Requests()
            await MockURLProtocol.setHandler { request in
                requests.record(request)
                return response(request, next: "?cursor=two")
            }
            let hub = client()
            let first = try await hub.listTree(in: "org/model")
            let task = Task {
                var iterator = Pages(firstPage: first) { page in
                    try await hub.nextPage(after: page)
                }.makeAsyncIterator()
                _ = try await iterator.next()
                withUnsafeCurrentTask { $0?.cancel() }
                await #expect(throws: CancellationError.self) {
                    _ = try await iterator.next()
                }
                await #expect(throws: CancellationError.self) {
                    _ = try await hub.nextPage(after: first)
                }
                await #expect(throws: CancellationError.self) {
                    _ = try await hub.listTree(in: "org/model")
                }
            }
            try await task.value
            #expect(requests.urls.count == 1)
        }

        @Test("Existing array APIs return only the first page", .mockURLSession)
        func testArrayCompatibility() async throws {
            let requests = Requests()
            await MockURLProtocol.setHandler { request in
                requests.record(request)
                #expect(request.url?.path.hasPrefix("/hf/api/") == true)
                return response(request, body: #"[{"path":"file","type":"file"}]"#, next: "?cursor=two")
            }
            let hub = client()
            #expect(try await hub.modelTree("org/model").count == 1)
            #expect(try await hub.datasetTree("org/model").count == 1)
            #expect(try await hub.spaceTree("org/model").count == 1)
            #expect(try await hub.listFiles(in: "org/model").count == 1)
            #expect(requests.urls.count == 4)
        }
    }
#endif
