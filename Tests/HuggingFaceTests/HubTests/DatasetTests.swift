import Foundation
import Testing

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

@testable import HuggingFace

#if swift(>=6.1)
    @Suite("Dataset Tests", .serialized)
    struct DatasetTests {
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

        @Test("List datasets with no parameters", .mockURLSession)
        func testListDatasets() async throws {
            let url = URL(string: "https://huggingface.co/api/datasets")!

            let mockResponse = """
                [
                    {
                        "id": "datasets/squad",
                        "author": "datasets",
                        "downloads": 500000,
                        "likes": 250
                    },
                    {
                        "id": "stanfordnlp/imdb",
                        "author": "stanfordnlp",
                        "downloads": 300000,
                        "likes": 150
                    }
                ]
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/datasets")
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
            let result = try await client.listDatasets()

            #expect(result.items.count == 2)
            #expect(result.items[0].id == "datasets/squad")
            #expect(result.items[0].author == "datasets")
            #expect(result.items[1].id == "stanfordnlp/imdb")
        }

        @Test("List datasets with search parameter", .mockURLSession)
        func testListDatasetsWithSearch() async throws {
            let mockResponse = """
                [
                    {
                        "id": "datasets/squad",
                        "author": "datasets",
                        "downloads": 500000,
                        "likes": 250
                    }
                ]
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/datasets")
                #expect(request.url?.query?.contains("search=squad") == true)

                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                )!

                return (response, Data(mockResponse.utf8))
            }

            let client = createMockClient()
            let result = try await client.listDatasets(search: "squad")

            #expect(result.items.count == 1)
            #expect(result.items[0].id == "datasets/squad")
        }

        @Test("Get specific dataset", .mockURLSession)
        func testGetDataset() async throws {
            let mockResponse = """
                {
                    "id": "_/squad",
                    "author": "datasets",
                    "downloads": 500000,
                    "likes": 250,
                    "tags": ["question-answering"]
                }
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/datasets/_/squad")
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
            let dataset = try await client.getDataset(repoID)

            #expect(dataset.id == "_/squad")
            #expect(dataset.author == "datasets")
            #expect(dataset.downloads == 500000)
        }

        @Test("Get dataset with namespace", .mockURLSession)
        func testGetDatasetWithNamespace() async throws {
            let mockResponse = """
                {
                    "id": "huggingface/squad",
                    "author": "huggingface",
                    "downloads": 500000,
                    "likes": 250
                }
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/datasets/huggingface/squad")
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
            let repoID: Repo.ID = "huggingface/squad"
            let dataset = try await client.getDataset(repoID)

            #expect(dataset.id == "huggingface/squad")
            #expect(dataset.author == "huggingface")
        }

        @Test("Get dataset tags", .mockURLSession)
        func testGetDatasetTags() async throws {
            let mockResponse = """
                {
                    "tags": {
                        "task_categories": [
                            {"id": "question-answering", "label": "Question Answering"},
                            {"id": "text-classification", "label": "Text Classification"}
                        ],
                        "languages": [
                            {"id": "en", "label": "English"},
                            {"id": "fr", "label": "French"}
                        ]
                    }
                }
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/datasets-tags-by-type")
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
            let tags = try await client.getDatasetTags()

            #expect(tags["task_categories"]?.count == 2)
            #expect(tags["languages"]?.count == 2)
        }

        @Test("List parquet files", .mockURLSession)
        func testListParquetFiles() async throws {
            let mockResponse = """
                [
                    {
                        "dataset": "squad",
                        "config": "default",
                        "split": "train",
                        "url": "https://huggingface.co/datasets/squad/resolve/main/data/train.parquet",
                        "filename": "train.parquet",
                        "size": 1024000
                    },
                    {
                        "dataset": "squad",
                        "config": "default",
                        "split": "validation",
                        "url": "https://huggingface.co/datasets/squad/resolve/main/data/validation.parquet",
                        "filename": "validation.parquet",
                        "size": 204800
                    }
                ]
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/datasets/_/squad/parquet")
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
            let files = try await client.listParquetFiles(repoID)

            #expect(files.count == 2)
            #expect(files[0].split == "train")
            #expect(files[1].split == "validation")
        }

        @Test("List parquet files with subset", .mockURLSession)
        func testListParquetFilesWithSubset() async throws {
            let mockResponse = """
                [
                    {
                        "dataset": "squad",
                        "config": "plain_text",
                        "split": "train",
                        "url": "https://huggingface.co/datasets/squad/resolve/main/data/plain_text/train.parquet",
                        "filename": "train.parquet",
                        "size": 1024000
                    }
                ]
                """

            await MockURLProtocol.setHandler { request in
                #expect(request.url?.path == "/api/datasets/_/squad/parquet/plain_text")
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
            let files = try await client.listParquetFiles(repoID, subset: "plain_text")

            #expect(files.count == 1)
            #expect(files[0].config == "plain_text")
        }

        @Test("List parquet files with subset and split", .mockURLSession)
        func testListParquetFilesWithSubsetAndSplit() async throws {
            let mockResponse = """
                [
                    {
                        "dataset": "squad",
                        "config": "plain_text",
                        "split": "train",
                        "url": "https://huggingface.co/datasets/squad/resolve/main/data/plain_text/train.parquet",
                        "filename": "train.parquet",
                        "size": 1024000
                    }
                ]
                """

            await MockURLProtocol.setHandler { request in
                #expect(
                    request.url?.path == "/api/datasets/_/squad/parquet/plain_text/train"
                )
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
            let files = try await client.listParquetFiles(
                repoID,
                subset: "plain_text",
                split: "train"
            )

            #expect(files.count == 1)
            #expect(files[0].split == "train")
        }

        @Test("Handle 404 error for dataset", .mockURLSession)
        func testGetDatasetNotFound() async throws {
            let errorResponse = """
                {
                    "error": "Dataset not found"
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
            let repoID: Repo.ID = "nonexistent/dataset"

            await #expect(throws: HTTPClientError.self) {
                _ = try await client.getDataset(repoID)
            }
        }
    }

#endif  // swift(>=6.1)
